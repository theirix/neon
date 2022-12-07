//!
//! Common traits and structs for layers
//!

use crate::repository::{Key, Value};
use crate::walrecord::NeonWalRecord;
use anyhow::Result;
use bytes::Bytes;
use std::ops::Range;
use std::sync::Arc;

use utils::{
    id::{TenantId, TimelineId},
    lsn::Lsn,
};

use super::filename::LayerFileName;
use super::layer_map::LayerMapLayer;
pub fn range_overlaps<T>(a: &Range<T>, b: &Range<T>) -> bool
where
    T: PartialOrd<T>,
{
    if a.start < b.start {
        a.end > b.start
    } else {
        b.end > a.start
    }
}

pub fn range_eq<T>(a: &Range<T>, b: &Range<T>) -> bool
where
    T: PartialEq<T>,
{
    a.start == b.start && a.end == b.end
}

/// Struct used to communicate across calls to 'get_value_reconstruct_data'.
///
/// Before first call, you can fill in 'page_img' if you have an older cached
/// version of the page available. That can save work in
/// 'get_value_reconstruct_data', as it can stop searching for page versions
/// when all the WAL records going back to the cached image have been collected.
///
/// When get_value_reconstruct_data returns Complete, 'img' is set to an image
/// of the page, or the oldest WAL record in 'records' is a will_init-type
/// record that initializes the page without requiring a previous image.
///
/// If 'get_page_reconstruct_data' returns Continue, some 'records' may have
/// been collected, but there are more records outside the current layer. Pass
/// the same ValueReconstructState struct in the next 'get_value_reconstruct_data'
/// call, to collect more records.
///
#[derive(Debug)]
pub struct ValueReconstructState {
    pub records: Vec<(Lsn, NeonWalRecord)>,
    pub img: Option<(Lsn, Bytes)>,
}

/// Return value from Layer::get_page_reconstruct_data
#[derive(Clone, Copy, Debug)]
pub enum ValueReconstructResult {
    /// Got all the data needed to reconstruct the requested page
    Complete,
    /// This layer didn't contain all the required data, the caller should look up
    /// the predecessor layer at the returned LSN and collect more data from there.
    Continue,

    /// This layer didn't contain data needed to reconstruct the page version at
    /// the returned LSN. This is usually considered an error, but might be OK
    /// in some circumstances.
    Missing,
}

/// A Layer contains all data in a "rectangle" consisting of a range of keys and
/// range of LSNs.
///
/// There are two kinds of layers, in-memory and on-disk layers. In-memory
/// layers are used to ingest incoming WAL, and provide fast access to the
/// recent page versions. On-disk layers are stored as files on disk, and are
/// immutable. This trait presents the common functionality of in-memory and
/// on-disk layers.
///
/// Furthermore, there are two kinds of on-disk layers: delta and image layers.
/// A delta layer contains all modifications within a range of LSNs and keys.
/// An image layer is a snapshot of all the data in a key-range, at a single
/// LSN
///
pub trait Layer: Send + Sync + LayerMapLayer {
    fn get_tenant_id(&self) -> TenantId;

    /// Identify the timeline this layer belongs to
    fn get_timeline_id(&self) -> TimelineId;

    /// Filename used to store this layer on disk. (Even in-memory layers
    /// implement this, to print a handy unique identifier for the layer for
    /// log messages, even though they're never not on disk.)
    fn filename(&self) -> LayerFileName;

    /// If a layer has a corresponding file on a local filesystem, return its absolute path.
    fn local_path(&self) -> Option<LayerFileName>;

    /// Returns true for layers that are represented in memory.
    fn is_in_memory(&self) -> bool;

    /// Iterate through all keys and values stored in the layer
    fn iter(&self) -> Box<dyn Iterator<Item = Result<(Key, Lsn, Value)>> + '_>;

    /// Iterate through all keys stored in the layer. Returns key, lsn and value size
    /// It is used only for compaction and so is currently implemented only for DeltaLayer
    fn key_iter(&self) -> Box<dyn Iterator<Item = (Key, Lsn, u64)> + '_> {
        panic!("Not implemented")
    }

    /// Permanently remove this layer from disk.
    fn delete(&self) -> Result<()>;

    // https://articles.bchlr.de/traits-dynamic-dispatch-upcasting
    fn as_arc_dyn_layer_map_layer<'a>(self: Arc<Self>) -> Arc<dyn LayerMapLayer>
    where
        Self: 'a;
}
