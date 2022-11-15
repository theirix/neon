mod calculation;
pub mod svg;

/// Pricing model or history size builder.
///
pub struct StorageModel {
    pub segments: Vec<Segment>,
}

///
/// Segment represents one point in the tree of branches, *and* the edge that leads
/// to it. We don't need separate structs for points and edges, because each
/// point can have only one parent.
///
/// When 'needed' is true, it means that we need to be able to reconstruct
/// any version between 'parent.lsn' and 'lsn'. If you want to represent that only
/// a single point is needed, create two Segments with the same lsn, and mark only
/// the child as needed.
///
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Segment {
    /// Previous segment index into ['Storage::segments`], if any.
    pub parent: Option<usize>,

    /// LSN at this point
    pub lsn: u64,

    /// Logical size at this node, if known.
    pub size: Option<u64>,

    /// If true, the segment from parent to this node is needed by `retention_period`
    pub needed: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SizeResult {
    pub method: SegmentMethod,
    // calculated size of this subtree, using this method
    pub accum_size: u64,
}

/// Different methods to retain history from a particular state
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum SegmentMethod {
    SnapshotHere, // A logical snapshot is needed after this segment
    Wal,          // Keep WAL leading up to this node
    Skipped,
}
