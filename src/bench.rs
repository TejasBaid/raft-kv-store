//! Closed-loop load generator: N concurrent clients, each issuing one request at a time.

use std::time::{Duration, Instant};

use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

use crate::client::{Client, ClientError};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Workload {
    /// 100% puts.
    Write,
    /// 100% linearizable gets.
    Read,
    /// 50% puts, 50% gets.
    Mixed,
}

impl std::fmt::Display for Workload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Workload::Write => "write",
            Workload::Read => "read",
            Workload::Mixed => "mixed",
        })
    }
}

#[derive(Clone, Debug)]
pub struct BenchOptions {
    pub workload: Workload,
    pub clients: usize,
    pub duration: Duration,
    pub warmup: Duration,
    pub keys: usize,
    pub value_size: usize,
}

#[derive(Clone, Debug)]
pub struct BenchResult {
    pub workload: Workload,
    pub clients: usize,
    pub ops: u64,
    pub errors: u64,
    pub elapsed: Duration,
    pub p50: Duration,
    pub p90: Duration,
    pub p99: Duration,
    pub p999: Duration,
    pub max: Duration,
}

impl BenchResult {
    pub fn throughput(&self) -> f64 {
        self.ops as f64 / self.elapsed.as_secs_f64()
    }
}

/// Writes every key once so reads hit existing data.
pub async fn preload(addrs: &[String], keys: usize, value_size: usize) -> Result<(), ClientError> {
    let value = vec![b'x'; value_size];
    let workers = 16.min(keys.max(1));
    let mut tasks = Vec::new();
    for w in 0..workers {
        let addrs = addrs.to_vec();
        let value = value.clone();
        tasks.push(tokio::spawn(async move {
            let mut c = Client::new(addrs);
            for k in (w..keys).step_by(workers) {
                c.put(&format!("key-{k}"), value.clone()).await?;
            }
            Ok::<(), ClientError>(())
        }));
    }
    for t in tasks {
        t.await.expect("preload task panicked")?;
    }
    Ok(())
}

/// Runs one workload and reports throughput and latency percentiles.
pub async fn run(addrs: &[String], opts: &BenchOptions) -> BenchResult {
    let start = Instant::now();
    let measure_from = start + opts.warmup;
    let end = measure_from + opts.duration;
    let mut tasks = Vec::new();
    for c in 0..opts.clients {
        let addrs = addrs.to_vec();
        let opts = opts.clone();
        tasks.push(tokio::spawn(async move {
            let mut client = Client::new(addrs);
            let mut rng = ChaCha8Rng::seed_from_u64(c as u64);
            let value = vec![b'v'; opts.value_size];
            let mut latencies: Vec<u32> = Vec::with_capacity(1 << 16);
            let mut errors = 0u64;
            loop {
                let now = Instant::now();
                if now >= end {
                    break;
                }
                let key = format!("key-{}", rng.gen_range(0..opts.keys));
                let write = match opts.workload {
                    Workload::Write => true,
                    Workload::Read => false,
                    Workload::Mixed => rng.gen_bool(0.5),
                };
                let t0 = Instant::now();
                let ok = if write {
                    client.put(&key, value.clone()).await.is_ok()
                } else {
                    client.get(&key).await.is_ok()
                };
                let t1 = Instant::now();
                if t0 >= measure_from && t1 <= end {
                    if ok {
                        latencies.push((t1 - t0).as_micros().min(u32::MAX as u128) as u32);
                    } else {
                        errors += 1;
                    }
                }
            }
            (latencies, errors)
        }));
    }
    let mut all = Vec::new();
    let mut errors = 0;
    for t in tasks {
        let (l, e) = t.await.expect("bench task panicked");
        all.extend(l);
        errors += e;
    }
    all.sort_unstable();
    let pct = |p: f64| -> Duration {
        if all.is_empty() {
            return Duration::ZERO;
        }
        let idx = ((all.len() as f64 * p).ceil() as usize).clamp(1, all.len()) - 1;
        Duration::from_micros(u64::from(all[idx]))
    };
    BenchResult {
        workload: opts.workload,
        clients: opts.clients,
        ops: all.len() as u64,
        errors,
        elapsed: opts.duration,
        p50: pct(0.50),
        p90: pct(0.90),
        p99: pct(0.99),
        p999: pct(0.999),
        max: all
            .last()
            .map_or(Duration::ZERO, |&m| Duration::from_micros(u64::from(m))),
    }
}
