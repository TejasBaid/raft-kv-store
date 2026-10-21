//! Runs the deterministic simulator over many seeds in parallel.
//!
//! ```text
//! cargo run --release --bin raft-sim -- --seeds 10000
//! cargo run --release --bin raft-sim -- --seed 1234 --verbose   # reproduce one run
//! ```

use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use clap::Parser;
use raft_kv::node::ReadMode;
use raft_kv::sim::{self, SimConfig, SimFailure, SimStats};

#[derive(Parser, Debug)]
#[command(about = "Deterministic fault-injection simulation of the Raft KV store")]
struct Args {
    /// Number of seeds to run.
    #[arg(long, default_value_t = 1000)]
    seeds: u64,
    /// First seed.
    #[arg(long, default_value_t = 0)]
    start: u64,
    /// Run exactly this one seed (overrides --seeds/--start).
    #[arg(long)]
    seed: Option<u64>,
    /// Cluster size.
    #[arg(long, default_value_t = 5)]
    nodes: usize,
    /// Concurrent clients.
    #[arg(long, default_value_t = 5)]
    clients: usize,
    /// Simulated duration of the faulty phase.
    #[arg(long, default_value_t = 10_000)]
    duration: u64,
    /// Expected crashes per 1000 time units.
    #[arg(long)]
    crash_rate: Option<f64>,
    /// Expected partitions per 1000 time units.
    #[arg(long)]
    partition_rate: Option<f64>,
    /// Per-message drop probability.
    #[arg(long)]
    drop: Option<f64>,
    /// Client request timeout before retrying elsewhere.
    #[arg(long)]
    client_timeout: Option<u64>,
    /// Disable pre-vote (exercises the plain Raft election path).
    #[arg(long)]
    no_pre_vote: bool,
    /// Worker threads (defaults to available parallelism).
    #[arg(long)]
    threads: Option<usize>,
    /// Serve reads from the leader's local state without ReadIndex (a deliberate bug,
    /// to demonstrate that the checker catches stale reads).
    #[arg(long)]
    unsafe_reads: bool,
    /// Print per-seed statistics.
    #[arg(long)]
    verbose: bool,
}

fn main() {
    let args = Args::parse();
    let (start, count) = match args.seed {
        Some(s) => (s, 1),
        None => (args.start, args.seeds),
    };
    let threads = args
        .threads
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(4, |n| n.get()));
    let next = AtomicU64::new(start);
    let end = start + count;
    let failures: Mutex<Vec<SimFailure>> = Mutex::new(Vec::new());
    let totals: Mutex<SimStats> = Mutex::new(SimStats::default());
    let done = AtomicU64::new(0);
    let began = Instant::now();

    std::thread::scope(|scope| {
        for _ in 0..threads {
            scope.spawn(|| {
                loop {
                    let seed = next.fetch_add(1, Ordering::Relaxed);
                    if seed >= end {
                        break;
                    }
                    let mut cfg = SimConfig::new(seed);
                    cfg.nodes = args.nodes;
                    cfg.clients = args.clients;
                    cfg.duration = args.duration;
                    if let Some(r) = args.crash_rate {
                        cfg.crash_rate = r;
                    }
                    if let Some(r) = args.partition_rate {
                        cfg.partition_rate = r;
                    }
                    if let Some(t) = args.client_timeout {
                        cfg.client_timeout = Some(t);
                    }
                    if let Some(p) = args.drop {
                        cfg.drop_prob = p;
                    }
                    cfg.pre_vote = !args.no_pre_vote;
                    if args.unsafe_reads {
                        cfg.read_mode = ReadMode::UnsafeLeaderLocal;
                    }
                    match sim::run(&cfg) {
                        Ok(report) => {
                            if args.verbose {
                                println!(
                                    "seed {seed}: ok  hash={:016x} {:?}",
                                    report.trace_hash, report.stats
                                );
                            }
                            accumulate(&mut totals.lock().unwrap(), &report.stats);
                        }
                        Err(f) => {
                            println!("FAIL {f}");
                            failures.lock().unwrap().push(f);
                        }
                    }
                    let d = done.fetch_add(1, Ordering::Relaxed) + 1;
                    if !args.verbose && d % 500 == 0 {
                        eprintln!("  ... {d}/{count} seeds");
                    }
                }
            });
        }
    });

    let failures = failures.into_inner().unwrap();
    let t = totals.into_inner().unwrap();
    let elapsed = began.elapsed();
    println!();
    println!(
        "{} seeds ({}..{}) on {} nodes in {:.1}s using {threads} threads",
        count,
        start,
        end,
        args.nodes,
        elapsed.as_secs_f64()
    );
    println!(
        "  client ops completed: {} ({} reads, {} writes issued)",
        t.ops_completed, t.reads, t.writes
    );
    println!(
        "  leaders elected: {}   crashes: {} ({} between send and fsync)   partitions: {}",
        t.leaders_elected, t.crashes, t.crashes_before_persist, t.partitions
    );
    println!(
        "  messages: {} sent, {} dropped   snapshots: {} taken, {} installed",
        t.messages_sent, t.messages_dropped, t.snapshots_taken, t.snapshots_installed
    );
    if failures.is_empty() {
        println!("  all seeds passed: election safety, log matching, leader completeness,");
        println!("  state machine safety, liveness, replica convergence and linearizability");
    } else {
        println!("  {} FAILED seeds:", failures.len());
        for f in failures.iter().take(20) {
            println!("    {f}");
        }
        std::process::exit(1);
    }
}

fn accumulate(t: &mut SimStats, s: &SimStats) {
    t.ops_completed += s.ops_completed;
    t.reads += s.reads;
    t.writes += s.writes;
    t.leaders_elected += s.leaders_elected;
    t.crashes += s.crashes;
    t.crashes_before_persist += s.crashes_before_persist;
    t.partitions += s.partitions;
    t.messages_sent += s.messages_sent;
    t.messages_dropped += s.messages_dropped;
    t.snapshots_taken += s.snapshots_taken;
    t.snapshots_installed += s.snapshots_installed;
}
