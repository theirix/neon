//!
//! Generate a tarball with files needed to bootstrap ComputeNode.
//!
//! TODO: this module has nothing to do with PostgreSQL pg_basebackup.
//! It could use a better name.
//!
//! Stateless Postgres compute node is launched by sending a tarball
//! which contains non-relational data (multixacts, clog, filenodemaps, twophase files),
//! generated pg_control and dummy segment of WAL.
//! This module is responsible for creation of such tarball
//! from data stored in object storage.
//!
use anyhow::{anyhow, bail, ensure, Context};
use bytes::{BufMut, BytesMut};
use fail::fail_point;
use std::fmt::Write as FmtWrite;
use std::time::SystemTime;
use tokio::io;
use tokio::io::AsyncWrite;
use tracing::*;

/// NB: This relies on a modified version of tokio_tar that does *not* write the
/// end-of-archive marker (1024 zero bytes), when the Builder struct is dropped
/// without explicitly calling 'finish' or 'into_inner'!
///
/// See https://github.com/neondatabase/tokio-tar/pull/1
///
use tokio_tar::{Builder, EntryType, Header};

use crate::context::RequestContext;
use crate::tenant::Timeline;
use pageserver_api::reltag::{RelTag, SlruKind};

use postgres_ffi::pg_constants::{DEFAULTTABLESPACE_OID, GLOBALTABLESPACE_OID};
use postgres_ffi::pg_constants::{PGDATA_SPECIAL_FILES, PGDATA_SUBDIRS, PG_HBA};
use postgres_ffi::TransactionId;
use postgres_ffi::XLogFileName;
use postgres_ffi::PG_TLI;
use postgres_ffi::{BLCKSZ, RELSEG_SIZE, WAL_SEGMENT_SIZE};
use utils::lsn::Lsn;

/// Create basebackup with non-rel data in it.
/// Only include relational data if 'full_backup' is true.
///
/// Currently we use empty 'req_lsn' in two cases:
///  * During the basebackup right after timeline creation
///  * When working without safekeepers. In this situation it is important to match the lsn
///    we are taking basebackup on with the lsn that is used in pageserver's walreceiver
///    to start the replication.
pub async fn send_basebackup_tarball<'a, W>(
    write: &'a mut W,
    timeline: &'a Timeline,
    req_lsn: Option<Lsn>,
    prev_lsn: Option<Lsn>,
    full_backup: bool,
    ctx: &'a RequestContext,
) -> anyhow::Result<()>
where
    W: AsyncWrite + Send + Sync + Unpin,
{
    // Compute postgres doesn't have any previous WAL files, but the first
    // record that it's going to write needs to include the LSN of the
    // previous record (xl_prev). We include prev_record_lsn in the
    // "zenith.signal" file, so that postgres can read it during startup.
    //
    // We don't keep full history of record boundaries in the page server,
    // however, only the predecessor of the latest record on each
    // timeline. So we can only provide prev_record_lsn when you take a
    // base backup at the end of the timeline, i.e. at last_record_lsn.
    // Even at the end of the timeline, we sometimes don't have a valid
    // prev_lsn value; that happens if the timeline was just branched from
    // an old LSN and it doesn't have any WAL of its own yet. We will set
    // prev_lsn to Lsn(0) if we cannot provide the correct value.
    let (backup_prev, backup_lsn) = if let Some(req_lsn) = req_lsn {
        // Backup was requested at a particular LSN. The caller should've
        // already checked that it's a valid LSN.

        // If the requested point is the end of the timeline, we can
        // provide prev_lsn. (get_last_record_rlsn() might return it as
        // zero, though, if no WAL has been generated on this timeline
        // yet.)
        let end_of_timeline = timeline.get_last_record_rlsn();
        if req_lsn == end_of_timeline.last {
            (end_of_timeline.prev, req_lsn)
        } else {
            (Lsn(0), req_lsn)
        }
    } else {
        // Backup was requested at end of the timeline.
        let end_of_timeline = timeline.get_last_record_rlsn();
        (end_of_timeline.prev, end_of_timeline.last)
    };

    // Consolidate the derived and the provided prev_lsn values
    let prev_lsn = if let Some(provided_prev_lsn) = prev_lsn {
        if backup_prev != Lsn(0) {
            ensure!(backup_prev == provided_prev_lsn);
        }
        provided_prev_lsn
    } else {
        backup_prev
    };

    info!(
        "taking basebackup lsn={}, prev_lsn={} (full_backup={})",
        backup_lsn, prev_lsn, full_backup
    );

    let basebackup = Basebackup {
        ar: Builder::new_non_terminated(write),
        timeline,
        lsn: backup_lsn,
        prev_record_lsn: prev_lsn,
        full_backup,
        ctx,
    };
    basebackup
        .send_tarball()
        .instrument(info_span!("send_tarball", backup_lsn=%backup_lsn))
        .await
}

/// This is short-living object only for the time of tarball creation,
/// created mostly to avoid passing a lot of parameters between various functions
/// used for constructing tarball.
struct Basebackup<'a, W>
where
    W: AsyncWrite + Send + Sync + Unpin,
{
    ar: Builder<&'a mut W>,
    timeline: &'a Timeline,
    lsn: Lsn,
    prev_record_lsn: Lsn,
    full_backup: bool,
    ctx: &'a RequestContext,
}

impl<'a, W> Basebackup<'a, W>
where
    W: AsyncWrite + Send + Sync + Unpin,
{
    async fn send_tarball(mut self) -> anyhow::Result<()> {
        // TODO include checksum

        // Create pgdata subdirs structure
        for dir in PGDATA_SUBDIRS.iter() {
            let header = new_tar_header_dir(dir)?;
            self.ar
                .append(&header, &mut io::empty())
                .await
                .context("could not add directory to basebackup tarball")?;
        }

        // Send config files.
        for filepath in PGDATA_SPECIAL_FILES.iter() {
            if *filepath == "pg_hba.conf" {
                let data = PG_HBA.as_bytes();
                let header = new_tar_header(filepath, data.len() as u64)?;
                self.ar
                    .append(&header, data)
                    .await
                    .context("could not add config file to basebackup tarball")?;
            } else {
                let header = new_tar_header(filepath, 0)?;
                self.ar
                    .append(&header, &mut io::empty())
                    .await
                    .context("could not add config file to basebackup tarball")?;
            }
        }

        // Gather non-relational files from object storage pages.
        for kind in [
            SlruKind::Clog,
            SlruKind::MultiXactOffsets,
            SlruKind::MultiXactMembers,
        ] {
            for segno in self
                .timeline
                .list_slru_segments(kind, self.lsn, self.ctx)
                .await?
            {
                self.add_slru_segment(kind, segno).await?;
            }
        }

        // Create tablespace directories
        for ((spcnode, dbnode), has_relmap_file) in
            self.timeline.list_dbdirs(self.lsn, self.ctx).await?
        {
            self.add_dbdir(spcnode, dbnode, has_relmap_file).await?;

            // Gather and send relational files in each database if full backup is requested.
            if self.full_backup {
                for rel in self
                    .timeline
                    .list_rels(spcnode, dbnode, self.lsn, self.ctx)
                    .await?
                {
                    self.add_rel(rel).await?;
                }
            }
        }
        for xid in self
            .timeline
            .list_twophase_files(self.lsn, self.ctx)
            .await?
        {
            self.add_twophase_file(xid).await?;
        }

        fail_point!("basebackup-before-control-file", |_| {
            bail!("failpoint basebackup-before-control-file")
        });

        // Generate pg_control and bootstrap WAL segment.
        self.add_pgcontrol_file().await?;
        self.ar.finish().await?;
        debug!("all tarred up!");
        Ok(())
    }

    async fn add_rel(&mut self, tag: RelTag) -> anyhow::Result<()> {
        let nblocks = self
            .timeline
            .get_rel_size(tag, self.lsn, false, self.ctx)
            .await?;

        // If the relation is empty, create an empty file
        if nblocks == 0 {
            let file_name = tag.to_segfile_name(0);
            let header = new_tar_header(&file_name, 0)?;
            self.ar.append(&header, &mut io::empty()).await?;
            return Ok(());
        }

        // Add a file for each chunk of blocks (aka segment)
        let mut startblk = 0;
        let mut seg = 0;
        while startblk < nblocks {
            let endblk = std::cmp::min(startblk + RELSEG_SIZE, nblocks);

            let mut segment_data: Vec<u8> = vec![];
            for blknum in startblk..endblk {
                let img = self
                    .timeline
                    .get_rel_page_at_lsn(tag, blknum, self.lsn, false, self.ctx)
                    .await?;
                segment_data.extend_from_slice(&img[..]);
            }

            let file_name = tag.to_segfile_name(seg as u32);
            let header = new_tar_header(&file_name, segment_data.len() as u64)?;
            self.ar.append(&header, segment_data.as_slice()).await?;

            seg += 1;
            startblk = endblk;
        }

        Ok(())
    }

    //
    // Generate SLRU segment files from repository.
    //
    async fn add_slru_segment(&mut self, slru: SlruKind, segno: u32) -> anyhow::Result<()> {
        let nblocks = self
            .timeline
            .get_slru_segment_size(slru, segno, self.lsn, self.ctx)
            .await?;

        let mut slru_buf: Vec<u8> = Vec::with_capacity(nblocks as usize * BLCKSZ as usize);
        for blknum in 0..nblocks {
            let img = self
                .timeline
                .get_slru_page_at_lsn(slru, segno, blknum, self.lsn, self.ctx)
                .await?;

            if slru == SlruKind::Clog {
                ensure!(img.len() == BLCKSZ as usize || img.len() == BLCKSZ as usize + 8);
            } else {
                ensure!(img.len() == BLCKSZ as usize);
            }

            slru_buf.extend_from_slice(&img[..BLCKSZ as usize]);
        }

        let segname = format!("{}/{:>04X}", slru.to_str(), segno);
        let header = new_tar_header(&segname, slru_buf.len() as u64)?;
        self.ar.append(&header, slru_buf.as_slice()).await?;

        trace!("Added to basebackup slru {} relsize {}", segname, nblocks);
        Ok(())
    }

    //
    // Include database/tablespace directories.
    //
    // Each directory contains a PG_VERSION file, and the default database
    // directories also contain pg_filenode.map files.
    //
    async fn add_dbdir(
        &mut self,
        spcnode: u32,
        dbnode: u32,
        has_relmap_file: bool,
    ) -> anyhow::Result<()> {
        let relmap_img = if has_relmap_file {
            let img = self
                .timeline
                .get_relmap_file(spcnode, dbnode, self.lsn, self.ctx)
                .await?;
            ensure!(img.len() == 512);
            Some(img)
        } else {
            None
        };

        if spcnode == GLOBALTABLESPACE_OID {
            let pg_version_str = self.timeline.pg_version.to_string();
            let header = new_tar_header("PG_VERSION", pg_version_str.len() as u64)?;
            self.ar.append(&header, pg_version_str.as_bytes()).await?;

            info!("timeline.pg_version {}", self.timeline.pg_version);

            if let Some(img) = relmap_img {
                // filenode map for global tablespace
                let header = new_tar_header("global/pg_filenode.map", img.len() as u64)?;
                self.ar.append(&header, &img[..]).await?;
            } else {
                warn!("global/pg_filenode.map is missing");
            }
        } else {
            // User defined tablespaces are not supported. However, as
            // a special case, if a tablespace/db directory is
            // completely empty, we can leave it out altogether. This
            // makes taking a base backup after the 'tablespace'
            // regression test pass, because the test drops the
            // created tablespaces after the tests.
            //
            // FIXME: this wouldn't be necessary, if we handled
            // XLOG_TBLSPC_DROP records. But we probably should just
            // throw an error on CREATE TABLESPACE in the first place.
            if !has_relmap_file
                && self
                    .timeline
                    .list_rels(spcnode, dbnode, self.lsn, self.ctx)
                    .await?
                    .is_empty()
            {
                return Ok(());
            }
            // User defined tablespaces are not supported
            ensure!(spcnode == DEFAULTTABLESPACE_OID);

            // Append dir path for each database
            let path = format!("base/{}", dbnode);
            let header = new_tar_header_dir(&path)?;
            self.ar.append(&header, &mut io::empty()).await?;

            if let Some(img) = relmap_img {
                let dst_path = format!("base/{}/PG_VERSION", dbnode);

                let pg_version_str = self.timeline.pg_version.to_string();
                let header = new_tar_header(&dst_path, pg_version_str.len() as u64)?;
                self.ar.append(&header, pg_version_str.as_bytes()).await?;

                let relmap_path = format!("base/{}/pg_filenode.map", dbnode);
                let header = new_tar_header(&relmap_path, img.len() as u64)?;
                self.ar.append(&header, &img[..]).await?;
            }
        };
        Ok(())
    }

    //
    // Extract twophase state files
    //
    async fn add_twophase_file(&mut self, xid: TransactionId) -> anyhow::Result<()> {
        let img = self
            .timeline
            .get_twophase_file(xid, self.lsn, self.ctx)
            .await?;

        let mut buf = BytesMut::new();
        buf.extend_from_slice(&img[..]);
        let crc = crc32c::crc32c(&img[..]);
        buf.put_u32_le(crc);
        let path = format!("pg_twophase/{:>08X}", xid);
        let header = new_tar_header(&path, buf.len() as u64)?;
        self.ar.append(&header, &buf[..]).await?;

        Ok(())
    }

    //
    // Add generated pg_control file and bootstrap WAL segment.
    // Also send zenith.signal file with extra bootstrap data.
    //
    async fn add_pgcontrol_file(&mut self) -> anyhow::Result<()> {
        // add zenith.signal file
        let mut zenith_signal = String::new();
        if self.prev_record_lsn == Lsn(0) {
            if self.lsn == self.timeline.get_ancestor_lsn() {
                write!(zenith_signal, "PREV LSN: none")?;
            } else {
                write!(zenith_signal, "PREV LSN: invalid")?;
            }
        } else {
            write!(zenith_signal, "PREV LSN: {}", self.prev_record_lsn)?;
        }
        self.ar
            .append(
                &new_tar_header("zenith.signal", zenith_signal.len() as u64)?,
                zenith_signal.as_bytes(),
            )
            .await?;

        let checkpoint_bytes = self
            .timeline
            .get_checkpoint(self.lsn, self.ctx)
            .await
            .context("failed to get checkpoint bytes")?;
        let pg_control_bytes = self
            .timeline
            .get_control_file(self.lsn, self.ctx)
            .await
            .context("failed get control bytes")?;

        let (pg_control_bytes, system_identifier) = postgres_ffi::generate_pg_control(
            &pg_control_bytes,
            &checkpoint_bytes,
            self.lsn,
            self.timeline.pg_version,
        )?;

        //send pg_control
        let header = new_tar_header("global/pg_control", pg_control_bytes.len() as u64)?;
        self.ar.append(&header, &pg_control_bytes[..]).await?;

        //send wal segment
        let segno = self.lsn.segment_number(WAL_SEGMENT_SIZE);
        let wal_file_name = XLogFileName(PG_TLI, segno, WAL_SEGMENT_SIZE);
        let wal_file_path = format!("pg_wal/{}", wal_file_name);
        let header = new_tar_header(&wal_file_path, WAL_SEGMENT_SIZE as u64)?;

        let wal_seg =
            postgres_ffi::generate_wal_segment(segno, system_identifier, self.timeline.pg_version)
                .map_err(|e| anyhow!(e).context("Failed generating wal segment"))?;
        ensure!(wal_seg.len() == WAL_SEGMENT_SIZE);
        self.ar.append(&header, &wal_seg[..]).await?;
        Ok(())
    }
}

//
// Create new tarball entry header
//
fn new_tar_header(path: &str, size: u64) -> anyhow::Result<Header> {
    let mut header = Header::new_gnu();
    header.set_size(size);
    header.set_path(path)?;
    header.set_mode(0b110000000); // -rw-------
    header.set_mtime(
        // use currenttime as last modified time
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs(),
    );
    header.set_cksum();
    Ok(header)
}

fn new_tar_header_dir(path: &str) -> anyhow::Result<Header> {
    let mut header = Header::new_gnu();
    header.set_size(0);
    header.set_path(path)?;
    header.set_mode(0o755); // -rw-------
    header.set_entry_type(EntryType::dir());
    header.set_mtime(
        // use currenttime as last modified time
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs(),
    );
    header.set_cksum();
    Ok(header)
}
