use std::cmp;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::{bail, Context};
use tokio::sync::Semaphore;

use super::Tenant;
use crate::tenant::Timeline;
use utils::id::TimelineId;
use utils::lsn::Lsn;

use tracing::*;

use tenant_size_model::{Segment, StorageModel};

/// Inputs to the actual tenant sizing model
///
/// Implements [`serde::Serialize`] but is not meant to be part of the public API, instead meant to
/// be a transferrable format between execution environments and developer.
#[serde_with::serde_as]
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct ModelInputs {
    pub segments: Vec<SegmentMeta>,
    pub timeline_inputs: Vec<TimelineInputs>,
}

/// A [`Segment`], with some extra information for display purposes
#[serde_with::serde_as]
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct SegmentMeta {
    pub segment: Segment,
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub timeline_id: TimelineId,
    pub kind: LsnKind,
}

#[derive(
    Debug, Clone, Copy, Eq, Ord, PartialEq, PartialOrd, serde::Serialize, serde::Deserialize,
)]
pub enum LsnKind {
    BranchStart, // this timeline starts here
    BranchPoint, // a child timeline branches off from here
    GcCutOff,    // GC cutoff point
    BranchEnd,   // last record LSN
}

/// Collect all relevant LSNs to the inputs. These will only be helpful in the serialized form as
/// part of [`ModelInputs`] from the HTTP api, explaining the inputs.
#[serde_with::serde_as]
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct TimelineInputs {
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub timeline_id: TimelineId,

    #[serde_as(as = "serde_with::DisplayFromStr")]
    last_record: Lsn,
    #[serde_as(as = "serde_with::DisplayFromStr")]
    latest_gc_cutoff: Lsn,
    #[serde_as(as = "serde_with::DisplayFromStr")]
    horizon_cutoff: Lsn,
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pitr_cutoff: Lsn,

    /// Cutoff point based on GC settings
    #[serde_as(as = "serde_with::DisplayFromStr")]
    next_gc_cutoff: Lsn,

    /// Cutoff point calculated from the user-supplied 'max_retention_period'
    #[serde_as(as = "Option<serde_with::DisplayFromStr>")]
    retention_param_cutoff: Option<Lsn>,
}

/// Gathers the inputs for the tenant sizing model.
///
/// Tenant size does not consider the latest state (???), but only the state until next_gc_cutoff, which
/// is updated on-demand, during the start of this calculation and separate from the
/// [`Timeline::latest_gc_cutoff`].
///
/// For timelines in general:
///
/// ```ignore
/// 0-----|---------|----|------------| · · · · · |·> lsn
///   initdb_lsn  branchpoints*  next_gc_cutoff  latest
/// ```
///
/// Until gc_horizon_cutoff > `Timeline::last_record_lsn` for any of the tenant's timelines, the
/// tenant size will be zero.
pub(super) async fn gather_inputs(
    tenant: &Tenant,
    limit: &Arc<Semaphore>,
    max_retention_period: Option<u64>,
    logical_size_cache: &mut HashMap<(TimelineId, Lsn), u64>,
) -> anyhow::Result<ModelInputs> {
    // Collect information about all the timelines
    let timelines = tenant
        .refresh_gc_info()
        .context("Failed to refresh gc_info before gathering inputs")?;
    if timelines.is_empty() {
        // All timelines are below tenant's gc_horizon; alternative would be to use
        // Tenant::list_timelines but then those gc_info's would not be updated yet, possibly
        // missing GcInfo::retain_lsns or having obsolete values for cutoff's.
        return Ok(ModelInputs {
            segments: vec![],
            timeline_inputs: Vec::new(),
        });
    }

    // Build a map of branch points.
    let mut branchpoints: HashMap<TimelineId, HashSet<Lsn>> = HashMap::new();
    for timeline in timelines.iter() {
        if let Some(ancestor_id) = timeline.get_ancestor_timeline_id() {
            branchpoints
                .entry(ancestor_id)
                .or_default()
                .insert(timeline.get_ancestor_lsn());
        }
    }

    // These become the final result.
    let mut timeline_inputs = Vec::with_capacity(timelines.len());
    let mut segments: Vec<SegmentMeta> = Vec::new();

    //
    // Build Segments representing each timeline. As we do that, also remember
    // the branchpoints and branch startpoints in 'branchpoint_segments' and
    // 'branchstart_segments'
    //

    // BranchPoint segments of each timeline
    let mut branchpoint_segments: HashMap<(TimelineId, Lsn), usize> = HashMap::new();

    // timeline, Branchpoint seg id, (ancestor, ancestor LSN)
    let mut branchstart_segments: Vec<(TimelineId, usize, Option<(TimelineId, Lsn)>)> = Vec::new();

    for timeline in timelines.iter() {
        let timeline_id = timeline.timeline_id;
        let last_record_lsn = timeline.get_last_record_lsn();

        // there's a race between the update (holding tenant.gc_lock) and this read but it
        // might not be an issue, because it's not for Timeline::gc
        let gc_info = timeline.gc_info.read().unwrap();

        // similar to gc, but Timeline::get_latest_gc_cutoff_lsn() will not be updated before a
        // new gc run, which we have no control over. however differently from `Timeline::gc`
        // we don't consider the `Timeline::disk_consistent_lsn` at all, because we are not
        // actually removing files.
        let mut next_gc_cutoff = cmp::min(gc_info.horizon_cutoff, gc_info.pitr_cutoff);

        // If the caller provided a shorter retention period, use that instead of the GC cutoff.
        let retention_param_cutoff = if let Some(max_retention_period) = max_retention_period {
            let param_cutoff = Lsn(last_record_lsn.0.saturating_sub(max_retention_period));
            if next_gc_cutoff < param_cutoff {
                next_gc_cutoff = param_cutoff;
            }
            Some(param_cutoff)
        } else {
            None
        };

        // the minimum where we should find the next_gc_cutoff for our calculations.
        //
        // next_gc_cutoff in parent branch are not of interest (right now at least), nor do we
        // want to query any logical size before initdb_lsn.
        let cutoff_minimum = cmp::max(timeline.get_ancestor_lsn(), timeline.initdb_lsn);

        // Build "interesting LSNs" on this timeline
        let mut lsns: Vec<(Lsn, LsnKind)> = gc_info
            .retain_lsns
            .iter()
            .inspect(|&&lsn| {
                trace!(
                    timeline_id=%timeline.timeline_id,
                    "retained lsn: {lsn:?}, is_before_ancestor_lsn={}",
                    lsn < timeline.get_ancestor_lsn()
                )
            })
            .filter(|&&lsn| lsn > timeline.get_ancestor_lsn())
            .copied()
            // this assumes there are no other retain_lsns than the branchpoints
            .map(|lsn| (lsn, LsnKind::BranchPoint))
            .collect::<Vec<_>>();

        // Add branch points we collected earlier, just in case there were any that were
        // not present in retain_lsns. Shouldn't happen, but better safe than sorry.
        // We will remove any duplicates below later.
        if let Some(this_branchpoints) = branchpoints.get(&timeline_id) {
            lsns.extend(
                this_branchpoints
                    .iter()
                    .map(|lsn| (*lsn, LsnKind::BranchPoint)),
            )
        }

        // Add a point for the GC cutoff
        if next_gc_cutoff > cutoff_minimum {
            lsns.push((next_gc_cutoff, LsnKind::GcCutOff));
        } else {
            // keep all of this timeline
        }

        lsns.sort_unstable();
        lsns.dedup();

        //
        // Create Segments for the interesting points.
        //

        // Timeline start point
        let ancestor = if let Some(ancestor_id) = timeline.get_ancestor_timeline_id() {
            Some((ancestor_id, timeline.get_ancestor_lsn()))
        } else {
            None
        };
        branchstart_segments.push((timeline_id, segments.len(), ancestor));
        let start_lsn = cmp::max(timeline.get_ancestor_lsn(), timeline.initdb_lsn);
        segments.push(SegmentMeta {
            segment: Segment {
                parent: None, // filled in later
                lsn: start_lsn.0,
                size: None, // filled in later
                needed: start_lsn > next_gc_cutoff,
            },
            timeline_id: timeline.timeline_id,
            kind: LsnKind::BranchStart,
        });

        // GC cutoff point, and any branch points, i.e. points where
        // other timelines branch off from this timeline.
        let mut parent = segments.len() - 1;
        for (lsn, kind) in lsns {
            if kind == LsnKind::BranchPoint {
                branchpoint_segments.insert((timeline_id, lsn), segments.len());
            }
            segments.push(SegmentMeta {
                segment: Segment {
                    parent: Some(parent),
                    lsn: lsn.0,
                    size: None,
                    needed: lsn > next_gc_cutoff,
                },
                timeline_id: timeline.timeline_id,
                kind,
            });
            parent += 1;
        }

        // Current end of the timeline
        segments.push(SegmentMeta {
            segment: Segment {
                parent: Some(parent),
                lsn: last_record_lsn.0,
                size: None, // Filled in later, if necessary
                needed: true,
            },
            timeline_id: timeline.timeline_id,
            kind: LsnKind::BranchEnd,
        });

        timeline_inputs.push(TimelineInputs {
            timeline_id: timeline.timeline_id,
            last_record: last_record_lsn,
            // this is not used above, because it might not have updated recently enough
            latest_gc_cutoff: *timeline.get_latest_gc_cutoff_lsn(),
            horizon_cutoff: gc_info.horizon_cutoff,
            pitr_cutoff: gc_info.pitr_cutoff,
            next_gc_cutoff,
            retention_param_cutoff,
        });
    }

    // We now have all segments from the timelines in 'segments'. The timelines
    // haven't been linked to each other yet, though. Do that.
    for (_timeline_id, seg_id, ancestor) in branchstart_segments {
        // Look up the branch point
        if let Some(ancestor) = ancestor {
            let parent_id = *branchpoint_segments.get(&ancestor).unwrap();
            segments[seg_id].segment.parent = Some(parent_id);
        }
    }

    // We left the 'size' field empty in all of the Segments so far.
    // Now find logical sizes for all of the points that might need or benefit from them.
    fill_logical_sizes(&timelines, &mut segments, limit, logical_size_cache).await?;

    Ok(ModelInputs {
        segments,
        timeline_inputs,
    })
}

/// Augment 'segments' with logical sizes
///
/// this will probably conflict with on-demand downloaded layers, or at least force them all
/// to be downloaded
async fn fill_logical_sizes(
    timelines: &[Arc<Timeline>],
    segments: &mut Vec<SegmentMeta>,
    limit: &Arc<Semaphore>,
    logical_size_cache: &mut HashMap<(TimelineId, Lsn), u64>,
) -> anyhow::Result<()> {
    let timeline_hash: HashMap<TimelineId, Arc<Timeline>> = HashMap::from_iter(
        timelines
            .iter()
            .map(|timeline| (timeline.timeline_id, Arc::clone(timeline))),
    );

    // record the used/inserted cache keys here, to remove extras not to start leaking
    // after initial run the cache should be quite stable, but live timelines will eventually
    // require new lsns to be inspected.
    let mut sizes_needed = HashMap::<(TimelineId, Lsn), Option<u64>>::new();

    // with joinset, on drop, all of the tasks will just be de-scheduled, which we can use to
    // our advantage with `?` error handling.
    let mut joinset = tokio::task::JoinSet::new();

    // For each point that would benefit from having a logical size available,
    // spawn a Task to fetch it, unless we have it cached already.
    for seg in segments.iter() {
        let calc_size = match seg.kind {
            LsnKind::BranchStart => false, // not needed
            LsnKind::BranchPoint => true,
            LsnKind::GcCutOff => true,
            LsnKind::BranchEnd => false,
        };
        if !calc_size {
            continue;
        }

        let timeline_id = seg.timeline_id;
        let lsn = Lsn(seg.segment.lsn);

        if let Entry::Vacant(e) = sizes_needed.entry((timeline_id, lsn)) {
            let cached_size = logical_size_cache.get(&(timeline_id, lsn)).cloned();
            if cached_size.is_none() {
                let timeline = Arc::clone(timeline_hash.get(&timeline_id).unwrap());
                let parallel_size_calcs = Arc::clone(limit);
                joinset.spawn(calculate_logical_size(parallel_size_calcs, timeline, lsn));
            }
            e.insert(cached_size);
        }
    }

    // Perform the size lookups
    let mut have_any_error = false;
    while let Some(res) = joinset.join_next().await {
        // each of these come with Result<Result<_, JoinError>, JoinError>
        // because of spawn + spawn_blocking
        let res = res.and_then(|inner| inner);
        match res {
            Ok(TimelineAtLsnSizeResult(timeline, lsn, Ok(size))) => {
                debug!(timeline_id=%timeline.timeline_id, %lsn, size, "size calculated");

                logical_size_cache.insert((timeline.timeline_id, lsn), size);
                sizes_needed.insert((timeline.timeline_id, lsn), Some(size));
            }
            Ok(TimelineAtLsnSizeResult(timeline, lsn, Err(error))) => {
                warn!(
                    timeline_id=%timeline.timeline_id,
                    "failed to calculate logical size at {lsn}: {error:#}"
                );
                have_any_error = true;
            }
            Err(join_error) if join_error.is_cancelled() => {
                unreachable!("we are not cancelling any of the futures, nor should be");
            }
            Err(join_error) => {
                // cannot really do anything, as this panic is likely a bug
                error!("logical size query panicked: {join_error:#}");
                have_any_error = true;
            }
        }
    }

    // prune any keys not needed anymore; we record every used key and added key.
    logical_size_cache.retain(|key, _| sizes_needed.contains_key(key));

    if have_any_error {
        // we cannot complete this round, because we are missing data.
        // we have however cached all we were able to request calculation on.
        anyhow::bail!("failed to calculate some logical_sizes");
    }

    // Insert the looked up sizes to the Segments
    for seg in segments.iter_mut() {
        let calc_size = match seg.kind {
            LsnKind::BranchStart => false, // not needed
            LsnKind::BranchPoint => true,
            LsnKind::GcCutOff => true,
            LsnKind::BranchEnd => false,
        };
        if !calc_size {
            continue;
        }

        let timeline_id = seg.timeline_id;
        let lsn = Lsn(seg.segment.lsn);

        if let Some(Some(size)) = sizes_needed.get(&(timeline_id, lsn)) {
            seg.segment.size = Some(*size);
        } else {
            bail!("could not find size at {} in timeline {}", lsn, timeline_id);
        }
    }
    Ok(())
}

impl ModelInputs {
    pub fn calculate_model(&self) -> anyhow::Result<tenant_size_model::StorageModel> {
        // Option<TimelineId> is used for "naming" the branches because it is assumed to be
        // impossible to always determine the a one main branch.

        // Convert SegmentMetas into plain Segments
        let storage = StorageModel {
            segments: self
                .segments
                .iter()
                .map(|seg| seg.segment.clone())
                .collect(),
        };

        Ok(storage)
    }

    // calculate total project size
    pub fn calculate(&self) -> anyhow::Result<u64> {
        let storage = self.calculate_model()?;

        let sizes = storage.calculate();

        // find the roots
        let mut total_size = 0;
        for (seg_id, seg) in storage.segments.iter().enumerate() {
            if seg.parent.is_none() {
                total_size += sizes[seg_id].accum_size;
            }
        }

        Ok(total_size)
    }
}

/// Newtype around the tuple that carries the timeline at lsn logical size calculation.
struct TimelineAtLsnSizeResult(
    Arc<crate::tenant::Timeline>,
    utils::lsn::Lsn,
    anyhow::Result<u64>,
);

#[instrument(skip_all, fields(timeline_id=%timeline.timeline_id, lsn=%lsn))]
async fn calculate_logical_size(
    limit: Arc<tokio::sync::Semaphore>,
    timeline: Arc<crate::tenant::Timeline>,
    lsn: utils::lsn::Lsn,
) -> Result<TimelineAtLsnSizeResult, tokio::task::JoinError> {
    let permit = tokio::sync::Semaphore::acquire_owned(limit)
        .await
        .expect("global semaphore should not had been closed");

    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let size_res = timeline.calculate_logical_size(lsn);
        TimelineAtLsnSizeResult(timeline, lsn, size_res)
    })
    .await
}

#[test]
fn verify_size_for_multiple_branches() {
    // this is generated from integration test test_tenant_size_with_multiple_branches, but this way
    // it has the stable lsn's
    let doc = r#"{"updates":[{"lsn":"0/0","command":{"branch_from":null},"timeline_id":"cd9d9409c216e64bf580904facedb01b"},{"lsn":"0/176FA40","command":{"update":25763840},"timeline_id":"cd9d9409c216e64bf580904facedb01b"},{"lsn":"0/176FA40","command":{"branch_from":"cd9d9409c216e64bf580904facedb01b"},"timeline_id":"10b532a550540bc15385eac4edde416a"},{"lsn":"0/1819818","command":{"update":26075136},"timeline_id":"10b532a550540bc15385eac4edde416a"},{"lsn":"0/18B5E40","command":{"update":26427392},"timeline_id":"cd9d9409c216e64bf580904facedb01b"},{"lsn":"0/18D3DF0","command":{"update":26492928},"timeline_id":"cd9d9409c216e64bf580904facedb01b"},{"lsn":"0/18D3DF0","command":{"branch_from":"cd9d9409c216e64bf580904facedb01b"},"timeline_id":"230fc9d756f7363574c0d66533564dcc"},{"lsn":"0/220F438","command":{"update":25239552},"timeline_id":"230fc9d756f7363574c0d66533564dcc"}],"retention_period":131072,"timeline_inputs":{"cd9d9409c216e64bf580904facedb01b":{"last_record":"0/18D5E40","latest_gc_cutoff":"0/169ACF0","horizon_cutoff":"0/18B5E40","pitr_cutoff":"0/18B5E40","next_gc_cutoff":"0/18B5E40"},"10b532a550540bc15385eac4edde416a":{"last_record":"0/1839818","latest_gc_cutoff":"0/169ACF0","horizon_cutoff":"0/1819818","pitr_cutoff":"0/1819818","next_gc_cutoff":"0/1819818"},"230fc9d756f7363574c0d66533564dcc":{"last_record":"0/222F438","latest_gc_cutoff":"0/169ACF0","horizon_cutoff":"0/220F438","pitr_cutoff":"0/220F438","next_gc_cutoff":"0/220F438"}}}"#;

    let inputs: ModelInputs = serde_json::from_str(doc).unwrap();

    assert_eq!(inputs.calculate().unwrap(), 36_409_872);
}
