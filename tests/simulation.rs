//! Randomized deterministic simulation. Set `SIM_SEEDS` to change the number of seeds
//! per scenario (default 1000); e.g. `SIM_SEEDS=5000 cargo test --release --test simulation`.

use raft_kv::node::ReadMode;
use raft_kv::sim::{self, SimConfig, SimFailure};

fn seeds() -> u64 {
    std::env::var("SIM_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1000)
}

/// Runs `count` seeds starting at `start` across all cores; returns the failures.
fn run_seeds(start: u64, count: u64, configure: impl Fn(&mut SimConfig) + Sync) -> Vec<SimFailure> {
    let threads = std::thread::available_parallelism().map_or(4, |n| n.get()) as u64;
    let failures = std::sync::Mutex::new(Vec::new());
    std::thread::scope(|s| {
        for t in 0..threads {
            let failures = &failures;
            let configure = &configure;
            s.spawn(move || {
                let mut seed = start + t;
                while seed < start + count {
                    let mut cfg = SimConfig::new(seed);
                    configure(&mut cfg);
                    if let Err(f) = sim::run(&cfg) {
                        failures.lock().unwrap().push(f);
                    }
                    seed += threads;
                }
            });
        }
    });
    let mut f = failures.into_inner().unwrap();
    f.sort_by_key(|f| f.seed);
    f
}

fn assert_all_pass(failures: Vec<SimFailure>) {
    if let Some(first) = failures.first() {
        panic!(
            "{} seed(s) failed; first: {first}\nreproduce with: cargo run --release --bin raft-sim -- --seed {}",
            failures.len(),
            first.seed
        );
    }
}

#[test]
fn five_nodes_with_crashes_partitions_and_message_loss() {
    assert_all_pass(run_seeds(0, seeds(), |_| {}));
}

#[test]
fn three_nodes_with_crashes_partitions_and_message_loss() {
    assert_all_pass(run_seeds(100_000, seeds(), |c| c.nodes = 3));
}

#[test]
fn heavy_faults_without_pre_vote() {
    assert_all_pass(run_seeds(200_000, seeds(), |c| {
        c.crash_rate = 4.0;
        c.partition_rate = 4.0;
        c.pre_vote = false;
    }));
}

#[test]
fn single_node_cluster() {
    assert_all_pass(run_seeds(300_000, seeds() / 3, |c| {
        c.nodes = 1;
        c.partition_rate = 0.0;
    }));
}

#[test]
fn same_seed_produces_identical_runs() {
    for seed in [7, 42, 1234] {
        let a = sim::run(&SimConfig::new(seed)).unwrap();
        let b = sim::run(&SimConfig::new(seed)).unwrap();
        assert_eq!(a.trace_hash, b.trace_hash, "seed {seed}");
        assert_eq!(a.stats, b.stats, "seed {seed}");
    }
    let a = sim::run(&SimConfig::new(1)).unwrap();
    let b = sim::run(&SimConfig::new(2)).unwrap();
    assert_ne!(a.trace_hash, b.trace_hash);
}

#[test]
fn runs_exercise_the_interesting_paths() {
    let mut totals = (0, 0, 0, 0, 0);
    for seed in 0..50 {
        let r = sim::run(&SimConfig::new(seed)).unwrap();
        totals.0 += r.stats.leaders_elected;
        totals.1 += r.stats.crashes;
        totals.2 += r.stats.partitions;
        totals.3 += r.stats.snapshots_installed;
        totals.4 += r.stats.ops_completed;
    }
    let (leaders, crashes, partitions, installs, ops) = totals;
    assert!(leaders > 100, "leaders elected: {leaders}");
    assert!(crashes > 100, "crashes: {crashes}");
    assert!(partitions > 100, "partitions: {partitions}");
    assert!(installs > 20, "snapshots installed: {installs}");
    assert!(ops > 10_000, "ops completed: {ops}");
}

/// Negative control: with a deliberately unsafe read path (a leader answers reads from
/// local state without confirming it is still leader), the linearizability checker
/// must find a stale read.
#[test]
fn checker_detects_stale_reads_from_unsafe_read_path() {
    let failures = run_seeds(0, 1000, |c| c.read_mode = ReadMode::UnsafeLeaderLocal);
    assert!(
        failures
            .iter()
            .any(|f| f.reason.contains("linearizability")),
        "expected the checker to catch a stale read in 1000 seeds"
    );
}
