//! `raft-kv`: run servers, a local cluster, client commands and benchmarks.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use clap::{Parser, Subcommand, ValueEnum};
use raft_kv::bench::{self, BenchOptions, Workload};
use raft_kv::client::Client;
use raft_kv::cluster::{ClusterOptions, LocalCluster};
use raft_kv::raft::NodeId;
use raft_kv::server::{self, ServerConfig};
use tokio::net::TcpListener;

const DEFAULT_CLUSTER: &str = "127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003";

#[derive(Parser)]
#[command(
    name = "raft-kv",
    version,
    about = "A Raft-replicated, linearizable key-value store"
)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Run a single server node.
    Server {
        /// This node's id (must appear in --peers).
        #[arg(long)]
        id: NodeId,
        /// Every node in the cluster, e.g. `1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003`.
        #[arg(long)]
        peers: String,
        #[arg(long)]
        data_dir: PathBuf,
        /// Do not fsync the WAL (survives process crashes, not power loss).
        #[arg(long)]
        no_fsync: bool,
        #[arg(long, default_value_t = 10_000)]
        snapshot_threshold: u64,
    },
    /// Run an N-node cluster inside this process until Ctrl-C.
    Cluster {
        #[arg(long, default_value_t = 3)]
        nodes: usize,
        #[arg(long, default_value_t = 7001)]
        base_port: u16,
        #[arg(long, default_value = "./data")]
        data_dir: PathBuf,
        #[arg(long)]
        no_fsync: bool,
    },
    /// Linearizable read.
    Get {
        key: String,
        #[arg(long, default_value = DEFAULT_CLUSTER)]
        cluster: String,
    },
    Put {
        key: String,
        value: String,
        #[arg(long, default_value = DEFAULT_CLUSTER)]
        cluster: String,
    },
    Delete {
        key: String,
        #[arg(long, default_value = DEFAULT_CLUSTER)]
        cluster: String,
    },
    /// Compare-and-swap: set KEY to --new if its current value is --expected.
    /// Omit --expected to require that the key is absent; omit --new to delete.
    Cas {
        key: String,
        #[arg(long)]
        expected: Option<String>,
        #[arg(long)]
        new: Option<String>,
        #[arg(long, default_value = DEFAULT_CLUSTER)]
        cluster: String,
    },
    /// Show each node's role, term and log positions.
    Status {
        #[arg(long, default_value = DEFAULT_CLUSTER)]
        cluster: String,
    },
    /// Benchmark throughput and latency against a fresh local cluster.
    Bench {
        #[arg(long, default_value_t = 3)]
        nodes: usize,
        /// Comma-separated concurrency levels.
        #[arg(long, default_value = "1,8,32,128")]
        clients: String,
        /// Comma-separated workloads: write, read, mixed.
        #[arg(long, default_value = "write,read")]
        workloads: String,
        #[arg(long, value_enum, default_value_t = FsyncMode::Both)]
        fsync: FsyncMode,
        /// Measured seconds per data point.
        #[arg(long, default_value_t = 5)]
        duration: u64,
        #[arg(long, default_value_t = 1)]
        warmup: u64,
        #[arg(long, default_value_t = 128)]
        value_size: usize,
        #[arg(long, default_value_t = 10_000)]
        keys: usize,
    },
}

#[derive(Clone, Copy, PartialEq, Eq, ValueEnum)]
enum FsyncMode {
    On,
    Off,
    Both,
}

fn parse_addrs(s: &str) -> Vec<String> {
    s.split(',')
        .map(str::trim)
        .filter(|a| !a.is_empty())
        .map(String::from)
        .collect()
}

fn parse_peers(s: &str) -> Result<BTreeMap<NodeId, SocketAddr>, String> {
    s.split(',')
        .map(|p| {
            let (id, addr) = p
                .split_once('=')
                .ok_or_else(|| format!("bad peer {p:?}, expected ID=HOST:PORT"))?;
            let id: NodeId = id
                .trim()
                .parse()
                .map_err(|e| format!("bad id {id:?}: {e}"))?;
            let addr: SocketAddr = addr
                .trim()
                .parse()
                .map_err(|e| format!("bad address {addr:?}: {e}"))?;
            Ok((id, addr))
        })
        .collect()
}

fn show(v: &Option<Vec<u8>>) -> String {
    match v {
        Some(b) => format!("{:?}", String::from_utf8_lossy(b)),
        None => "(nil)".into(),
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "raft_kv=info".into()),
        )
        .with_writer(std::io::stderr)
        .init();
    match run(Cli::parse()).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}

async fn run(cli: Cli) -> Result<(), Box<dyn std::error::Error>> {
    match cli.cmd {
        Cmd::Server {
            id,
            peers,
            data_dir,
            no_fsync,
            snapshot_threshold,
        } => {
            let peers = parse_peers(&peers)?;
            let addr = *peers.get(&id).ok_or("--id must appear in --peers")?;
            let mut cfg = ServerConfig::new(id, peers, data_dir);
            cfg.fsync = !no_fsync;
            cfg.snapshot_threshold = snapshot_threshold;
            let listener = TcpListener::bind(addr).await?;
            let handle = server::start(cfg, listener)?;
            let mut poll = tokio::time::interval(Duration::from_millis(200));
            loop {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => break,
                    _ = poll.tick() => {
                        if handle.is_finished() {
                            return Err("node stopped unexpectedly (see log)".into());
                        }
                    }
                }
            }
            handle.shutdown().await;
        }
        Cmd::Cluster {
            nodes,
            base_port,
            data_dir,
            no_fsync,
        } => {
            let mut opts = ClusterOptions::new(nodes, data_dir);
            opts.base_port = Some(base_port);
            opts.fsync = !no_fsync;
            let cluster = LocalCluster::start(opts).await?;
            for (id, addr) in cluster.addrs() {
                println!("node {id} listening on {addr}");
            }
            let leader = cluster.wait_for_leader(Duration::from_secs(10)).await?;
            println!("leader: node {leader}. Press Ctrl-C to stop.");
            tokio::signal::ctrl_c().await?;
            cluster.shutdown().await;
        }
        Cmd::Get { key, cluster } => {
            let v = Client::new(parse_addrs(&cluster)).get(&key).await?;
            println!("{}", show(&v));
        }
        Cmd::Put {
            key,
            value,
            cluster,
        } => {
            Client::new(parse_addrs(&cluster)).put(&key, value).await?;
            println!("OK");
        }
        Cmd::Delete { key, cluster } => {
            let existed = Client::new(parse_addrs(&cluster)).delete(&key).await?;
            println!("{}", if existed { "deleted" } else { "not found" });
        }
        Cmd::Cas {
            key,
            expected,
            new,
            cluster,
        } => {
            let (swapped, previous) = Client::new(parse_addrs(&cluster))
                .cas(
                    &key,
                    expected.map(String::into_bytes),
                    new.map(String::into_bytes),
                )
                .await?;
            if swapped {
                println!("swapped (was {})", show(&previous));
            } else {
                println!("not swapped (current value {})", show(&previous));
            }
        }
        Cmd::Status { cluster } => {
            println!(
                "{:<22} {:>3} {:<13} {:>5} {:>6} {:>8} {:>8} {:>8} {:>8}",
                "address", "id", "role", "term", "leader", "commit", "applied", "last", "snapshot"
            );
            for addr in parse_addrs(&cluster) {
                match Client::status(&addr, Duration::from_secs(1)).await {
                    Ok(s) => println!(
                        "{:<22} {:>3} {:<13} {:>5} {:>6} {:>8} {:>8} {:>8} {:>8}",
                        addr,
                        s.id,
                        s.role.to_string(),
                        s.term,
                        s.leader.map_or("-".into(), |l| l.to_string()),
                        s.commit_index,
                        s.last_applied,
                        s.last_log_index,
                        s.snapshot_index
                    ),
                    Err(e) => println!("{addr:<22} unreachable ({e})"),
                }
            }
        }
        Cmd::Bench {
            nodes,
            clients,
            workloads,
            fsync,
            duration,
            warmup,
            value_size,
            keys,
        } => {
            let clients: Vec<usize> = clients
                .split(',')
                .map(|c| c.trim().parse())
                .collect::<Result<_, _>>()?;
            let workloads: Vec<Workload> = workloads
                .split(',')
                .map(|w| match w.trim() {
                    "write" => Ok(Workload::Write),
                    "read" => Ok(Workload::Read),
                    "mixed" => Ok(Workload::Mixed),
                    other => Err(format!("unknown workload {other:?}")),
                })
                .collect::<Result<_, _>>()?;
            let modes: &[bool] = match fsync {
                FsyncMode::On => &[true],
                FsyncMode::Off => &[false],
                FsyncMode::Both => &[false, true],
            };
            println!(
                "{nodes}-node cluster on 127.0.0.1, {value_size}-byte values, {keys} keys, \
                 {duration}s per point (+{warmup}s warmup), closed-loop clients\n"
            );
            println!(
                "| fsync | workload | clients | ops/s | p50 (ms) | p90 (ms) | p99 (ms) | p99.9 (ms) | errors |"
            );
            println!("|---|---|---:|---:|---:|---:|---:|---:|---:|");
            for &fs in modes {
                let dir = tempfile::tempdir()?;
                let mut opts = ClusterOptions::new(nodes, dir.path().to_path_buf());
                opts.fsync = fs;
                opts.snapshot_threshold = 50_000;
                let cluster = LocalCluster::start(opts).await?;
                cluster.wait_for_leader(Duration::from_secs(10)).await?;
                let addrs = cluster.client_addrs();
                bench::preload(&addrs, keys, value_size).await?;
                for &workload in &workloads {
                    for &c in &clients {
                        let r = bench::run(
                            &addrs,
                            &BenchOptions {
                                workload,
                                clients: c,
                                duration: Duration::from_secs(duration),
                                warmup: Duration::from_secs(warmup),
                                keys,
                                value_size,
                            },
                        )
                        .await;
                        let ms = |d: Duration| d.as_secs_f64() * 1000.0;
                        println!(
                            "| {} | {} | {} | {:.0} | {:.2} | {:.2} | {:.2} | {:.2} | {} |",
                            if fs { "on" } else { "off" },
                            r.workload,
                            r.clients,
                            r.throughput(),
                            ms(r.p50),
                            ms(r.p90),
                            ms(r.p99),
                            ms(r.p999),
                            r.errors
                        );
                    }
                }
                cluster.shutdown().await;
            }
        }
    }
    Ok(())
}
