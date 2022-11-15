//! Tenant size model testing ground.
//!
//! Has a number of scenarios and a `main` for invoking these by number, calculating the history
//! size, outputs graphviz graph. Makefile in directory shows how to use graphviz to turn scenarios
//! into pngs.

use tenant_size_model::{Segment, SizeResult, StorageModel};

use std::collections::HashMap;

struct ScenarioBuilder {
    segments: Vec<Segment>,

    /// Mapping from the branch name to the index of a segment describing its latest state.
    branches: HashMap<String, usize>,
}

impl ScenarioBuilder {
    /// Creates a new storage with the given default branch name.
    pub fn new(initial_branch: &str) -> ScenarioBuilder {
        let init_segment = Segment {
            parent: None,
            lsn: 0,
            size: Some(0),
            needed: false, // determined later
        };

        ScenarioBuilder {
            segments: vec![init_segment],
            branches: HashMap::from([(initial_branch.into(), 0)]),
        }
    }

    /// Advances the branch with the named operation, by the relative LSN and logical size bytes.
    pub fn modify_branch(&mut self, branch: &str, lsn_bytes: u64, size_bytes: i64) {
        let lastseg_id = *self.branches.get(branch).unwrap();
        let newseg_id = self.segments.len();
        let lastseg = &mut self.segments[lastseg_id];

        let newseg = Segment {
            parent: Some(lastseg_id),
            lsn: lastseg.lsn + lsn_bytes,
            size: Some((lastseg.size.unwrap() as i64 + size_bytes) as u64),
            needed: false,
        };

        self.segments.push(newseg);
        *self.branches.get_mut(branch).expect("read already") = newseg_id;
    }

    pub fn insert(&mut self, branch: &str, bytes: u64) {
        self.modify_branch(branch, bytes, bytes as i64);
    }

    pub fn update(&mut self, branch: &str, bytes: u64) {
        self.modify_branch(branch, bytes, 0i64);
    }

    pub fn _delete(&mut self, branch: &str, bytes: u64) {
        self.modify_branch(branch, bytes, -(bytes as i64));
    }

    /// Panics if the parent branch cannot be found.
    pub fn branch(&mut self, parent: &str, name: &str) {
        // Find the right segment
        let branchseg_id = *self
            .branches
            .get(parent)
            .expect("should had found the parent by key");
        let _branchseg = &mut self.segments[branchseg_id];

        // Create branch name for it
        self.branches.insert(name.to_string(), branchseg_id);
    }

    pub fn calculate(&mut self, retention_period: u64) -> Vec<SizeResult> {
        // Phase 1: Mark all the segments that need to be retained
        for (_branch, &last_seg_id) in self.branches.iter() {
            let last_seg = &self.segments[last_seg_id];
            let cutoff_lsn = last_seg.lsn.saturating_sub(retention_period);
            let mut seg_id = last_seg_id;
            loop {
                let seg = &mut self.segments[seg_id];
                if seg.lsn < cutoff_lsn {
                    break;
                }
                seg.needed = true;
                if let Some(prev_seg_id) = seg.parent {
                    seg_id = prev_seg_id;
                } else {
                    break;
                }
            }
        }

        // Perform the calculation
        let storage_model = StorageModel {
            segments: self.segments.clone(),
        };
        storage_model.calculate()
    }

    pub fn into_segments(self) -> Vec<Segment> {
        self.segments
    }
}

// Main branch only. Some updates on it.
fn scenario_1() -> (Vec<Segment>, Vec<SizeResult>) {
    // Create main branch
    let mut scenario = ScenarioBuilder::new("main");

    // Bulk load 5 GB of data to it
    scenario.insert("main", 5_000);

    // Stream of updates
    for _ in 0..5 {
        scenario.update("main", 1_000);
    }

    let size = scenario.calculate(1000);

    (scenario.into_segments(), size)
}

// Main branch only. Some updates on it.
fn scenario_2() -> (Vec<Segment>, Vec<SizeResult>) {
    // Create main branch
    let mut scenario = ScenarioBuilder::new("main");

    // Bulk load 5 GB of data to it
    scenario.insert("main", 5_000);

    // Stream of updates
    for _ in 0..5 {
        scenario.update("main", 1_000);
    }

    // Branch
    scenario.branch("main", "child");
    scenario.update("child", 1_000);

    // More updates on parent
    scenario.update("main", 1_000);

    let size = scenario.calculate(1000);

    (scenario.into_segments(), size)
}

// Like 2, but more updates on main
fn scenario_3() -> (Vec<Segment>, Vec<SizeResult>) {
    // Create main branch
    let mut scenario = ScenarioBuilder::new("main");

    // Bulk load 5 GB of data to it
    scenario.insert("main", 5_000);

    // Stream of updates
    for _ in 0..5 {
        scenario.update("main", 1_000);
    }

    // Branch
    scenario.branch("main", "child");
    scenario.update("child", 1_000);

    // More updates on parent
    for _ in 0..5 {
        scenario.update("main", 1_000);
    }

    let size = scenario.calculate(1000);

    (scenario.into_segments(), size)
}

// Diverged branches
fn scenario_4() -> (Vec<Segment>, Vec<SizeResult>) {
    // Create main branch
    let mut scenario = ScenarioBuilder::new("main");

    // Bulk load 5 GB of data to it
    scenario.insert("main", 5_000);

    // Stream of updates
    for _ in 0..5 {
        scenario.update("main", 1_000);
    }

    // Branch
    scenario.branch("main", "child");
    scenario.update("child", 1_000);

    // More updates on parent
    for _ in 0..8 {
        scenario.update("main", 1_000);
    }

    let size = scenario.calculate(1000);

    (scenario.into_segments(), size)
}

fn scenario_5() -> (Vec<Segment>, Vec<SizeResult>) {
    let mut scenario = ScenarioBuilder::new("a");
    scenario.insert("a", 5000);
    scenario.branch("a", "b");
    scenario.update("b", 4000);
    scenario.update("a", 2000);
    scenario.branch("a", "c");
    scenario.insert("c", 4000);
    scenario.insert("a", 2000);

    let size = scenario.calculate(5000);

    (scenario.into_segments(), size)
}

fn scenario_6() -> (Vec<Segment>, Vec<SizeResult>) {
    let branches = [
        "7ff1edab8182025f15ae33482edb590a",
        "b1719e044db05401a05a2ed588a3ad3f",
        "0xb68d6691c895ad0a70809470020929ef",
    ];

    // compared to other scenarios, this one uses bytes instead of kB

    let mut scenario = ScenarioBuilder::new("");

    scenario.branch(&"", branches[0]); // at 0
    scenario.modify_branch(&branches[0], 108951064, 43696128); // at 108951064
    scenario.branch(&branches[0], branches[1]); // at 108951064
    scenario.modify_branch(&branches[1], 15560408, -1851392); // at 124511472
    scenario.modify_branch(&branches[0], 174464360, -1531904); // at 283415424
    scenario.branch(&branches[0], branches[2]); // at 283415424
    scenario.modify_branch(&branches[2], 15906192, 8192); // at 299321616
    scenario.modify_branch(&branches[0], 18909976, 32768); // at 302325400

    let size = scenario.calculate(100_000);

    (scenario.into_segments(), size)
}

fn main() {
    let args: Vec<String> = std::env::args().collect();

    let scenario = if args.len() < 2 { "1" } else { &args[1] };

    let (_segments, _size) = match scenario {
        "1" => scenario_1(),
        "2" => scenario_2(),
        "3" => scenario_3(),
        "4" => scenario_4(),
        "5" => scenario_5(),
        "6" => scenario_6(),
        other => {
            eprintln!("invalid scenario {}", other);
            std::process::exit(1);
        }
    };

    // FIXME: broken
    //graphviz_tree(&segments, &size);
}

// FIXME: this is broken. Not sure if it's worth fixing it, or focus 100% on the
// SVG version
/*
fn graphviz_recurse(segments: &[Segment], node: &SegmentSize) {
    use tenant_size_model::SegmentMethod::*;

    let seg_id = node.seg_id;
    let seg = segments.get(seg_id).unwrap();
    let lsn = seg.end_lsn;
    let size = seg.end_size.unwrap_or(0);
    let method = node.method;

    println!("  {{");
    println!("    node [width=0.1 height=0.1 shape=oval]");

    let tenant_size = node.total_children();

    let penwidth = if seg.needed { 6 } else { 3 };
    let x = match method {
        SnapshotAfter =>
            format!("label=\"lsn: {lsn}\\nsize: {size}\\ntenant_size: {tenant_size}\" style=filled penwidth={penwidth}"),
        Wal =>
            format!("label=\"lsn: {lsn}\\nsize: {size}\\ntenant_size: {tenant_size}\" color=\"black\" penwidth={penwidth}"),
        WalNeeded =>
            format!("label=\"lsn: {lsn}\\nsize: {size}\\ntenant_size: {tenant_size}\" color=\"black\" penwidth={penwidth}"),
        Skipped =>
            format!("label=\"lsn: {lsn}\\nsize: {size}\\ntenant_size: {tenant_size}\" color=\"gray\" penwidth={penwidth}"),
    };

    println!("    \"seg{seg_id}\" [{x}]");
    println!("  }}");

    // Recurse. Much of the data is actually on the edge
    for child in node.children.iter() {
        let child_id = child.seg_id;
        graphviz_recurse(segments, child);

        let edge_color = match child.method {
            SnapshotAfter => "gray",
            Wal => "black",
            WalNeeded => "black",
            Skipped => "gray",
        };

        println!("  {{");
        println!("    edge [] ");
        print!("    \"seg{seg_id}\" -> \"seg{child_id}\" [");
        print!("color={edge_color}");
        if child.method == WalNeeded {
            print!(" penwidth=6");
        }
        if child.method == Wal {
            print!(" penwidth=3");
        }

        let next = segments.get(child_id).unwrap();

        if next.op.is_empty() {
            print!(
                " label=\"{} / {}\"",
                next.end_lsn - seg.end_lsn,
                (next.end_size.unwrap_or(0) as i128 - seg.end_size.unwrap_or(0) as i128)
            );
        } else {
            print!(" label=\"{}: {}\"", next.op, next.end_lsn - seg.end_lsn);
        }
        println!("]");
        println!("  }}");
    }
}

fn graphviz_tree(segments: &[Segment], tree: &SegmentSize) {
    println!("digraph G {{");
    println!("  fontname=\"Helvetica,Arial,sans-serif\"");
    println!("  node [fontname=\"Helvetica,Arial,sans-serif\"]");
    println!("  edge [fontname=\"Helvetica,Arial,sans-serif\"]");
    println!("  graph [center=1 rankdir=LR]");
    println!("  edge [dir=none]");

    graphviz_recurse(segments, tree);

    println!("}}");
}
 */

#[test]
fn scenarios_return_same_size() {
    type ScenarioFn = fn() -> (Vec<Segment>, SegmentSize);
    let truths: &[(u32, ScenarioFn, _)] = &[
        (line!(), scenario_1, 8000),
        (line!(), scenario_2, 9000),
        (line!(), scenario_3, 13000),
        (line!(), scenario_4, 16000),
        (line!(), scenario_5, 17000),
        (line!(), scenario_6, 333_792_000),
    ];

    for (line, scenario, expected) in truths {
        let (_, size) = scenario();
        assert_eq!(*expected, size.total_children(), "scenario on line {line}");
    }
}
