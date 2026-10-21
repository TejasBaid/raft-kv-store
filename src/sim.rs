//! Deterministic simulation testing.
//!
//! The simulator runs a whole cluster plus a set of clients inside one thread, in
//! discrete simulated time, with every source of nondeterminism drawn from one seeded
//! RNG. The very same [`KvNode`] code that runs in production is driven here; only the
//! network, the disk and the clock are simulated:
//!
//! * **Network**: every message (Raft and client) is independently dropped, delayed
//!   (occasionally by a lot, which reorders messages) or duplicated. Partitions split
//!   the nodes into groups that cannot exchange Raft messages.
//! * **Crashes**: a node is dropped on the floor, losing all volatile state, and later
//!   rebuilt from whatever its simulated disk ([`MemStorage`]) holds.
//! * **Clocks**: each node ticks at a slightly different rate.
//!
//! While running, the simulator continuously checks Raft's safety properties:
//!
//! * **Election safety**: at most one leader per term.
//! * **Log matching**: if two logs hold an entry with the same index and term, the logs
//!   are identical up to that index.
//! * **Leader completeness**: a newly elected leader holds every entry already applied
//!   anywhere.
//! * **State machine safety**: no two nodes apply different entries at the same index.
//!
//! When the run ends, faults stop and the simulator checks **liveness** (every client
//! operation completes and all replicas converge to the same state) and
//! **linearizability** of the complete client history.
//!
//! A failing seed reproduces exactly: `cargo run --release --bin raft-sim -- --seed N`.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap};
use std::fmt;

use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

use crate::kv::{Command, KvResult, WriteRequest};
use crate::linearizability::{self, KvCheck, KvInput, KvOperation, Operation};
use crate::node::{KvNode, NodeConfig, ReadMode, RequestId};
use crate::protocol::{ClientRequest, ClientResponse};
use crate::raft::{self, LogIndex, Message, NodeId, Term};
use crate::storage::{MemStorage, Storage};
use crate::util::{fnv1a, fnv1a_extend};

/// Parameters for one simulation run. Times are in abstract units (think milliseconds).
#[derive(Clone, Debug)]
pub struct SimConfig {
    pub seed: u64,
    pub nodes: usize,
    pub clients: usize,
    pub keys: usize,
    /// Faults are injected and clients start new operations only before this time.
    pub duration: u64,
    /// After `duration`, every outstanding operation must finish within this long.
    pub liveness_timeout: u64,
    /// Nominal time between Raft ticks (each node's clock is skewed by up to ±10%).
    pub tick_interval: u64,
    pub min_delay: u64,
    pub max_delay: u64,
    /// Probability a message is delayed by up to `long_delay_max` (reordering it).
    pub long_delay_prob: f64,
    pub long_delay_max: u64,
    pub drop_prob: f64,
    pub dup_prob: f64,
    /// Expected node crashes per 1000 time units.
    pub crash_rate: f64,
    pub restart_delay: (u64, u64),
    /// Expected partitions per 1000 time units.
    pub partition_rate: f64,
    pub partition_duration: (u64, u64),
    /// Probability that a node crashes after sending its pre-persist messages but
    /// before its disk write completes (per batch that has something to persist).
    pub crash_before_persist_prob: f64,
    pub snapshot_threshold: u64,
    /// Client retry timeout; `None` draws one per seed from {60, 100, 250}. Impatient
    /// clients fail over faster, which makes stale reads from a deposed leader likelier.
    pub client_timeout: Option<u64>,
    pub read_mode: ReadMode,
    pub pre_vote: bool,
    pub check_linearizability: bool,
}

impl SimConfig {
    pub fn new(seed: u64) -> Self {
        Self {
            seed,
            nodes: 5,
            clients: 5,
            keys: 3,
            duration: 10_000,
            liveness_timeout: 10_000,
            tick_interval: 10,
            min_delay: 1,
            max_delay: 8,
            long_delay_prob: 0.03,
            long_delay_max: 120,
            drop_prob: 0.05,
            dup_prob: 0.02,
            crash_rate: 1.0,
            restart_delay: (50, 1_500),
            partition_rate: 1.0,
            partition_duration: (100, 1_500),
            crash_before_persist_prob: 0.0005,
            snapshot_threshold: 50,
            client_timeout: None,
            read_mode: ReadMode::ReadIndex,
            pre_vote: true,
            check_linearizability: true,
        }
    }
}

/// Counters describing what happened during a run.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SimStats {
    pub ops_completed: u64,
    pub reads: u64,
    pub writes: u64,
    pub max_term: Term,
    pub leaders_elected: u64,
    pub crashes: u64,
    pub crashes_before_persist: u64,
    pub partitions: u64,
    pub messages_sent: u64,
    pub messages_dropped: u64,
    pub snapshots_taken: u64,
    pub snapshots_installed: u64,
    pub final_commit_index: LogIndex,
    pub end_time: u64,
}

#[derive(Clone, Debug)]
pub struct SimReport {
    pub seed: u64,
    pub stats: SimStats,
    /// Hash of the complete client history and final node states. Two runs with the
    /// same configuration must produce the same hash.
    pub trace_hash: u64,
}

#[derive(Clone, Debug)]
pub struct SimFailure {
    pub seed: u64,
    pub time: u64,
    pub reason: String,
}

impl fmt::Display for SimFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "seed {} failed at t={}: {}",
            self.seed, self.time, self.reason
        )
    }
}

impl std::error::Error for SimFailure {}

/// Runs one simulation.
pub fn run(cfg: &SimConfig) -> Result<SimReport, SimFailure> {
    Sim::new(cfg.clone()).run()
}

// ---------------------------------------------------------------------- internals

#[derive(Clone)]
enum Envelope {
    Raft(Message),
    Request {
        client: usize,
        node: NodeId,
        attempt: u64,
        req: ClientRequest,
    },
    Response {
        client: usize,
        node: NodeId,
        attempt: u64,
        resp: ClientResponse,
    },
}

struct InFlight {
    at: u64,
    seq: u64,
    env: Envelope,
}

impl PartialEq for InFlight {
    fn eq(&self, other: &Self) -> bool {
        (self.at, self.seq) == (other.at, other.seq)
    }
}
impl Eq for InFlight {}
impl PartialOrd for InFlight {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for InFlight {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.at, self.seq).cmp(&(other.at, other.seq))
    }
}

struct SimNode {
    id: NodeId,
    kv: Option<KvNode>,
    disk: MemStorage,
    next_tick: u64,
    tick_period: u64,
    restart_at: Option<u64>,
    incarnation: u64,
    /// Requests this incarnation has accepted: request id -> (client, attempt).
    inflight: BTreeMap<RequestId, (usize, u64)>,
    next_req: RequestId,
    last_term_seen_leading: Option<Term>,
}

struct ActiveOp {
    hist: usize,
    req: ClientRequest,
    attempt: u64,
    target: NodeId,
    deadline: u64,
    retry_at: Option<u64>,
}

struct SimClient {
    client_id: u64,
    seq: u64,
    /// Attempt ids are unique per client across operations, so a late response to an
    /// old operation can never be mistaken for a response to the current one.
    attempts: u64,
    leader_guess: NodeId,
    op: Option<ActiveOp>,
    next_op_at: u64,
    counter: u64,
    last_seen: BTreeMap<String, Option<Vec<u8>>>,
}

struct HistEntry {
    client: u64,
    key: String,
    input: KvInput,
    output: Option<KvResult>,
    call: u64,
    ret: Option<u64>,
}

struct Partition {
    group: Vec<u8>,
    heal_at: u64,
}

/// Raft tunables drawn once per seed, so the seed space also covers configurations.
#[derive(Clone, Copy, Debug)]
struct Tunables {
    max_entries_per_append: usize,
    max_inflight_entries: u64,
    snapshot_threshold: u64,
    client_timeout: u64,
}

struct Sim {
    cfg: SimConfig,
    tunables: Tunables,
    rng: ChaCha8Rng,
    now: u64,
    nodes: Vec<SimNode>,
    clients: Vec<SimClient>,
    queue: BinaryHeap<Reverse<InFlight>>,
    msg_seq: u64,
    event_seq: u64,
    partition: Option<Partition>,
    history: Vec<HistEntry>,
    leaders_by_term: BTreeMap<Term, NodeId>,
    /// Entries known to be committed (applied by at least one node): index -> (term, digest).
    committed: BTreeMap<LogIndex, (Term, u64)>,
    stats: SimStats,
}

type SimResult<T = ()> = Result<T, String>;

impl Sim {
    fn new(cfg: SimConfig) -> Self {
        assert!(cfg.nodes >= 1 && cfg.clients >= 1 && cfg.keys >= 1);
        let mut rng = ChaCha8Rng::seed_from_u64(cfg.seed);
        let random_timeout = [60, 100, 250][rng.gen_range(0..3)];
        let tunables = Tunables {
            max_entries_per_append: [1, 3, 16, 64][rng.gen_range(0..4)],
            max_inflight_entries: [1, 8, 64, 8192][rng.gen_range(0..4)],
            client_timeout: cfg.client_timeout.unwrap_or(random_timeout),
            snapshot_threshold: rng
                .gen_range(cfg.snapshot_threshold / 4..=cfg.snapshot_threshold * 2),
        };
        let nodes = (1..=cfg.nodes as NodeId)
            .map(|id| {
                let skew = cfg.tick_interval / 10;
                let tick_period =
                    rng.gen_range(cfg.tick_interval - skew..=cfg.tick_interval + skew);
                SimNode {
                    id,
                    kv: None,
                    disk: MemStorage::new(),
                    next_tick: rng.gen_range(1..=tick_period),
                    tick_period,
                    restart_at: None,
                    incarnation: 0,
                    inflight: BTreeMap::new(),
                    next_req: 0,
                    last_term_seen_leading: None,
                }
            })
            .collect();
        let clients = (0..cfg.clients)
            .map(|i| SimClient {
                client_id: i as u64 + 1,
                seq: 0,
                attempts: 0,
                leader_guess: rng.gen_range(1..=cfg.nodes as NodeId),
                op: None,
                next_op_at: rng.gen_range(0..100),
                counter: 0,
                last_seen: BTreeMap::new(),
            })
            .collect();
        let mut sim = Self {
            cfg,
            tunables,
            rng,
            now: 0,
            nodes,
            clients,
            queue: BinaryHeap::new(),
            msg_seq: 0,
            event_seq: 0,
            partition: None,
            history: Vec::new(),
            leaders_by_term: BTreeMap::new(),
            committed: BTreeMap::new(),
            stats: SimStats::default(),
        };
        for i in 0..sim.nodes.len() {
            sim.start_node(i);
        }
        sim
    }

    fn fail(&self, reason: String) -> SimFailure {
        SimFailure {
            seed: self.cfg.seed,
            time: self.now,
            reason,
        }
    }

    fn run(mut self) -> Result<SimReport, SimFailure> {
        // Phase 1: workload with faults.
        while self.now < self.cfg.duration {
            self.step(true).map_err(|e| self.fail(e))?;
        }
        // Phase 2: heal everything and wait for outstanding operations (liveness).
        self.partition = None;
        for i in 0..self.nodes.len() {
            if self.nodes[i].kv.is_none() {
                self.start_node(i);
            }
        }
        let deadline = self.cfg.duration + self.cfg.liveness_timeout;
        while self.clients.iter().any(|c| c.op.is_some()) {
            if self.now >= deadline {
                let pending = self.clients.iter().filter(|c| c.op.is_some()).count();
                return Err(self.fail(format!(
                    "liveness: {pending} client operations still pending {} units after faults stopped",
                    self.cfg.liveness_timeout
                )));
            }
            self.step(false).map_err(|e| self.fail(e))?;
        }
        // Phase 3: let replicas converge and compare state machines.
        self.converge().map_err(|e| self.fail(e))?;
        self.check_log_matching().map_err(|e| self.fail(e))?;
        if self.cfg.check_linearizability {
            self.check_linearizability().map_err(|e| self.fail(e))?;
        }
        self.stats.end_time = self.now;
        self.stats.final_commit_index = self
            .nodes
            .iter()
            .filter_map(|n| n.kv.as_ref())
            .map(|kv| kv.raft().commit_index())
            .max()
            .unwrap_or(0);
        for n in &self.nodes {
            if let Some(kv) = &n.kv {
                self.stats.snapshots_taken += kv.snapshots_taken();
                self.stats.snapshots_installed += kv.snapshots_installed();
            }
        }
        let trace_hash = self.trace_hash();
        Ok(SimReport {
            seed: self.cfg.seed,
            stats: self.stats,
            trace_hash,
        })
    }

    /// Advances simulated time by one unit.
    fn step(&mut self, faults: bool) -> SimResult {
        self.now += 1;
        if faults {
            self.inject_faults();
        }
        self.deliver_messages()?;
        for i in 0..self.nodes.len() {
            let node = &mut self.nodes[i];
            if let Some(kv) = node.kv.as_mut() {
                while node.next_tick <= self.now {
                    kv.tick();
                    node.next_tick += node.tick_period;
                }
            }
        }
        self.drive_clients(faults);
        for i in 0..self.nodes.len() {
            self.poll_node(i, faults)?;
        }
        if self.now % 50 == 0 {
            self.check_log_matching()?;
        }
        Ok(())
    }

    // ------------------------------------------------------------------ faults

    fn inject_faults(&mut self) {
        let now = self.now;
        for i in 0..self.nodes.len() {
            if self.nodes[i].restart_at.is_some_and(|t| t <= now) {
                self.start_node(i);
            }
        }
        if self.rng.gen_bool((self.cfg.crash_rate / 1000.0).min(1.0)) {
            let alive: Vec<usize> = (0..self.nodes.len())
                .filter(|&i| self.nodes[i].kv.is_some())
                .collect();
            if !alive.is_empty() {
                let victim = alive[self.rng.gen_range(0..alive.len())];
                self.crash(victim);
            }
        }
        match &self.partition {
            Some(p) if p.heal_at <= now => self.partition = None,
            Some(_) => {}
            None => {
                if self
                    .rng
                    .gen_bool((self.cfg.partition_rate / 1000.0).min(1.0))
                    && self.nodes.len() > 1
                {
                    let n = self.nodes.len();
                    let group = if self.rng.gen_bool(0.5) {
                        // Isolate one node (often the leader, to exercise failover).
                        let leader = self
                            .nodes
                            .iter()
                            .position(|nd| nd.kv.as_ref().is_some_and(|kv| kv.raft().is_leader()));
                        let victim = match leader {
                            Some(l) if self.rng.gen_bool(0.7) => l,
                            _ => self.rng.gen_range(0..n),
                        };
                        (0..n).map(|i| u8::from(i == victim)).collect()
                    } else {
                        // Random split into two non-empty groups.
                        loop {
                            let g: Vec<u8> = (0..n).map(|_| self.rng.gen_range(0..2)).collect();
                            if g.contains(&0) && g.contains(&1) {
                                break g;
                            }
                        }
                    };
                    let (lo, hi) = self.cfg.partition_duration;
                    self.partition = Some(Partition {
                        group,
                        heal_at: now + self.rng.gen_range(lo..=hi),
                    });
                    self.stats.partitions += 1;
                }
            }
        }
    }

    /// Drops a node's volatile state; it restarts later from its simulated disk.
    fn crash(&mut self, i: usize) {
        let (lo, hi) = self.cfg.restart_delay;
        let restart_at = self.now + self.rng.gen_range(lo..=hi);
        let node = &mut self.nodes[i];
        node.kv = None;
        node.inflight.clear();
        node.restart_at = Some(restart_at);
        self.stats.crashes += 1;
    }

    fn connected(&self, a: NodeId, b: NodeId) -> bool {
        match &self.partition {
            None => true,
            Some(p) => p.group[(a - 1) as usize] == p.group[(b - 1) as usize],
        }
    }

    fn start_node(&mut self, i: usize) {
        let ids: Vec<NodeId> = (1..=self.cfg.nodes as NodeId).collect();
        let node = &mut self.nodes[i];
        node.incarnation += 1;
        let mut raft_cfg = raft::Config::new(node.id, ids);
        raft_cfg.seed = self.rng.r#gen();
        raft_cfg.max_entries_per_append = self.tunables.max_entries_per_append;
        raft_cfg.max_inflight_entries = self.tunables.max_inflight_entries;
        raft_cfg.pre_vote = self.cfg.pre_vote;
        let mut cfg = NodeConfig::new(raft_cfg);
        cfg.snapshot_threshold = self.tunables.snapshot_threshold.max(1);
        cfg.read_mode = self.cfg.read_mode;
        let kv = KvNode::new(cfg, node.disk.state().clone()).expect("disk state decodes");
        node.kv = Some(kv);
        node.restart_at = None;
        node.inflight.clear();
        node.last_term_seen_leading = None;
    }

    // ------------------------------------------------------------------ network

    fn send(&mut self, env: Envelope) {
        self.stats.messages_sent += 1;
        if self.rng.gen_bool(self.cfg.drop_prob) {
            self.stats.messages_dropped += 1;
            return;
        }
        let copies = if self.rng.gen_bool(self.cfg.dup_prob) {
            2
        } else {
            1
        };
        let mut env = Some(env);
        for c in 0..copies {
            let delay = if self.rng.gen_bool(self.cfg.long_delay_prob) {
                self.rng
                    .gen_range(self.cfg.max_delay..=self.cfg.long_delay_max)
            } else {
                self.rng.gen_range(self.cfg.min_delay..=self.cfg.max_delay)
            };
            let e = if c + 1 == copies {
                env.take().expect("present")
            } else {
                env.as_ref().expect("present").clone()
            };
            self.msg_seq += 1;
            self.queue.push(Reverse(InFlight {
                at: self.now + delay,
                seq: self.msg_seq,
                env: e,
            }));
        }
    }

    fn deliver_messages(&mut self) -> SimResult {
        while self.queue.peek().is_some_and(|Reverse(m)| m.at <= self.now) {
            let Reverse(m) = self.queue.pop().expect("peeked");
            match m.env {
                Envelope::Raft(msg) => {
                    if !self.connected(msg.from, msg.to) {
                        self.stats.messages_dropped += 1;
                        continue;
                    }
                    if let Some(kv) = self.nodes[(msg.to - 1) as usize].kv.as_mut() {
                        kv.step(msg);
                    }
                }
                Envelope::Request {
                    client,
                    node,
                    attempt,
                    req,
                } => {
                    let n = &mut self.nodes[(node - 1) as usize];
                    if let Some(kv) = n.kv.as_mut() {
                        let id = n.next_req;
                        n.next_req += 1;
                        n.inflight.insert(id, (client, attempt));
                        kv.handle_client(id, req);
                    }
                }
                Envelope::Response {
                    client,
                    node,
                    attempt,
                    resp,
                } => self.on_response(client, node, attempt, resp)?,
            }
        }
        Ok(())
    }

    /// Drives one node until it has no more output, following the [`Output`] contract.
    fn poll_node(&mut self, i: usize, faults: bool) -> SimResult {
        let id = self.nodes[i].id;
        loop {
            let Some(kv) = self.nodes[i].kv.as_mut() else {
                return Ok(());
            };
            let out = kv.poll();
            if out.is_empty() {
                return Ok(());
            }
            for msg in out.pre_persist_messages {
                self.send(Envelope::Raft(msg));
            }
            // Crash after a leader has sent entries but before they reached its disk.
            if faults
                && out.persist.is_some()
                && self.rng.gen_bool(self.cfg.crash_before_persist_prob)
            {
                self.crash(i);
                self.stats.crashes_before_persist += 1;
                return Ok(());
            }
            if let Some(p) = &out.persist {
                self.nodes[i]
                    .disk
                    .persist(p)
                    .expect("memory storage cannot fail");
            }
            self.nodes[i].kv.as_mut().expect("alive").advance();
            // State machine safety.
            for a in &out.applied {
                match self.committed.get(&a.index) {
                    Some(&(term, digest)) if (term, digest) != (a.term, a.digest) => {
                        return Err(format!(
                            "state machine safety: node {id} applied index {} term {} but term {term} was applied elsewhere",
                            a.index, a.term
                        ));
                    }
                    Some(_) => {}
                    None => {
                        self.committed.insert(a.index, (a.term, a.digest));
                    }
                }
            }
            self.check_leader(i)?;
            for msg in out.messages {
                self.send(Envelope::Raft(msg));
            }
            for (req, resp) in out.responses {
                if let Some((client, attempt)) = self.nodes[i].inflight.remove(&req) {
                    self.send(Envelope::Response {
                        client,
                        node: id,
                        attempt,
                        resp,
                    });
                }
            }
        }
    }

    /// Election safety and leader completeness, checked whenever a node is leader.
    fn check_leader(&mut self, i: usize) -> SimResult {
        let node = &self.nodes[i];
        let Some(kv) = &node.kv else { return Ok(()) };
        let r = kv.raft();
        if !r.is_leader() {
            return Ok(());
        }
        let term = r.term();
        self.stats.max_term = self.stats.max_term.max(term);
        if node.last_term_seen_leading == Some(term) {
            return Ok(());
        }
        match self.leaders_by_term.get(&term) {
            Some(&other) if other != node.id => {
                return Err(format!(
                    "election safety: nodes {other} and {} are both leader in term {term}",
                    node.id
                ));
            }
            Some(_) => {}
            None => {
                self.leaders_by_term.insert(term, node.id);
                self.stats.leaders_elected += 1;
            }
        }
        let log = r.log();
        for (&idx, &(t, _)) in self.committed.range(log.first_index()..) {
            if log.term(idx) != Some(t) {
                return Err(format!(
                    "leader completeness: new leader {} (term {term}) lacks committed entry {idx} (term {t})",
                    node.id
                ));
            }
        }
        self.nodes[i].last_term_seen_leading = Some(term);
        Ok(())
    }

    fn check_log_matching(&self) -> SimResult {
        let alive: Vec<&SimNode> = self.nodes.iter().filter(|n| n.kv.is_some()).collect();
        for (a_idx, a) in alive.iter().enumerate() {
            for b in &alive[a_idx + 1..] {
                let la = a.kv.as_ref().expect("alive").raft().log();
                let lb = b.kv.as_ref().expect("alive").raft().log();
                let lo = la.first_index().max(lb.first_index());
                let hi = la.last_index().min(lb.last_index());
                let mut matched = false;
                let mut i = hi;
                while i >= lo && i > 0 {
                    let (ea, eb) = (
                        la.entry(i).expect("in range"),
                        lb.entry(i).expect("in range"),
                    );
                    if matched {
                        if ea != eb {
                            return Err(format!(
                                "log matching: nodes {} and {} share a later entry but differ at index {i}",
                                a.id, b.id
                            ));
                        }
                    } else if ea.term == eb.term {
                        matched = true;
                        if ea != eb {
                            return Err(format!(
                                "log matching: nodes {} and {} have different entries with term {} at index {i}",
                                a.id, b.id, ea.term
                            ));
                        }
                    }
                    i -= 1;
                }
            }
        }
        Ok(())
    }

    fn converge(&mut self) -> SimResult {
        let limit = self.now + 5_000;
        loop {
            let alive: Vec<&KvNode> = self.nodes.iter().filter_map(|n| n.kv.as_ref()).collect();
            let max_commit = alive
                .iter()
                .map(|kv| kv.raft().commit_index())
                .max()
                .unwrap_or(0);
            let converged = alive
                .iter()
                .all(|kv| kv.raft().last_applied() == max_commit)
                && alive.iter().any(|kv| kv.raft().is_leader());
            if converged {
                let digests: Vec<(NodeId, u64)> = alive
                    .iter()
                    .map(|kv| (kv.raft().id(), kv.state_machine().digest()))
                    .collect();
                if digests.windows(2).any(|w| w[0].1 != w[1].1) {
                    return Err(format!(
                        "replicas applied the same log but diverged: {digests:?}"
                    ));
                }
                return Ok(());
            }
            if self.now >= limit {
                return Err("replicas did not converge after faults stopped".into());
            }
            self.step(false)?;
        }
    }

    fn check_linearizability(&self) -> SimResult {
        let ops: Vec<KvOperation> = self
            .history
            .iter()
            .map(|h| KvOperation {
                key: h.key.clone(),
                op: Operation {
                    client: h.client,
                    input: h.input.clone(),
                    output: h.output.clone(),
                    call: h.call,
                    ret: h.ret,
                },
            })
            .collect();
        match linearizability::check_kv(&ops, 50_000_000) {
            KvCheck::Linearizable => Ok(()),
            KvCheck::Violation { key, history } => {
                let mut msg = format!(
                    "linearizability violation on key {key:?} ({} ops):",
                    history.len()
                );
                for op in history.iter().rev().take(12).rev() {
                    msg.push_str(&format!(
                        "\n    client {} call {} ret {:?}: {:?} -> {:?}",
                        op.op.client, op.op.call, op.op.ret, op.op.input, op.op.output
                    ));
                }
                Err(msg)
            }
            KvCheck::Unknown { key } => Err(format!(
                "linearizability check for key {key:?} exceeded its search budget"
            )),
        }
    }

    // ------------------------------------------------------------------ clients

    fn drive_clients(&mut self, accept_new: bool) {
        for c in 0..self.clients.len() {
            let now = self.now;
            let client = &self.clients[c];
            match &client.op {
                None if accept_new && now >= client.next_op_at => self.start_op(c),
                Some(op) if op.retry_at.is_some_and(|t| t <= now) || op.deadline <= now => {
                    if op.deadline <= now && op.retry_at.is_none() {
                        // Timed out: the node may be dead or partitioned; try another.
                        let n = self.cfg.nodes as NodeId;
                        let cl = &mut self.clients[c];
                        cl.leader_guess = cl.leader_guess % n + 1;
                    }
                    self.send_attempt(c);
                }
                _ => {}
            }
        }
    }

    fn start_op(&mut self, c: usize) {
        let key = format!("k{}", self.rng.gen_range(0..self.cfg.keys));
        let roll = self.rng.gen_range(0..100);
        let client = &mut self.clients[c];
        client.counter += 1;
        let unique = format!("c{}-{}", client.client_id, client.counter).into_bytes();
        let (input, req) = if roll < 35 {
            self.stats.reads += 1;
            (KvInput::Get, ClientRequest::Get { key: key.clone() })
        } else {
            self.stats.writes += 1;
            let command = if roll < 60 {
                Command::Put {
                    key: key.clone(),
                    value: unique.clone(),
                }
            } else if roll < 72 {
                Command::Delete { key: key.clone() }
            } else {
                let expected = if roll < 92 {
                    client.last_seen.get(&key).cloned().flatten()
                } else {
                    None
                };
                let new = if roll < 97 {
                    Some(unique.clone())
                } else {
                    None
                };
                Command::Cas {
                    key: key.clone(),
                    expected,
                    new,
                }
            };
            let input = match &command {
                Command::Put { value, .. } => KvInput::Put(value.clone()),
                Command::Delete { .. } => KvInput::Delete,
                Command::Cas { expected, new, .. } => KvInput::Cas {
                    expected: expected.clone(),
                    new: new.clone(),
                },
            };
            client.seq += 1;
            (
                input,
                ClientRequest::Write(WriteRequest {
                    client_id: client.client_id,
                    seq: client.seq,
                    command,
                }),
            )
        };
        self.event_seq += 1;
        self.history.push(HistEntry {
            client: self.clients[c].client_id,
            key,
            input,
            output: None,
            call: self.event_seq,
            ret: None,
        });
        let target = self.clients[c].leader_guess;
        self.clients[c].op = Some(ActiveOp {
            hist: self.history.len() - 1,
            req,
            attempt: 0,
            target,
            deadline: 0,
            retry_at: None,
        });
        self.send_attempt(c);
    }

    fn send_attempt(&mut self, c: usize) {
        let now = self.now;
        let timeout = self.tunables.client_timeout;
        let client = &mut self.clients[c];
        let target = client.leader_guess;
        client.attempts += 1;
        let attempt = client.attempts;
        let op = client.op.as_mut().expect("active op");
        op.attempt = attempt;
        op.target = target;
        op.deadline = now + timeout;
        op.retry_at = None;
        let env = Envelope::Request {
            client: c,
            node: target,
            attempt: op.attempt,
            req: op.req.clone(),
        };
        self.send(env);
    }

    fn on_response(
        &mut self,
        c: usize,
        node: NodeId,
        attempt: u64,
        resp: ClientResponse,
    ) -> SimResult {
        let n = self.cfg.nodes as NodeId;
        let now = self.now;
        let backoff = self.rng.gen_range(5..30);
        let think = self.rng.gen_range(0..30);
        let client = &mut self.clients[c];
        let Some(op) = client.op.as_mut() else {
            return Ok(()); // duplicate response for a finished op
        };
        if op.attempt != attempt || op.target != node {
            return Ok(()); // response to an abandoned attempt
        }
        match resp {
            ClientResponse::Ok(result) => {
                let hist = op.hist;
                client.op = None;
                client.leader_guess = node;
                client.next_op_at = now + think;
                let h = &mut self.history[hist];
                match (&h.input, &result) {
                    (KvInput::Get, KvResult::Value(v)) => {
                        client.last_seen.insert(h.key.clone(), v.clone());
                    }
                    (KvInput::Cas { .. }, KvResult::Cas { previous, .. }) => {
                        client.last_seen.insert(h.key.clone(), previous.clone());
                    }
                    (KvInput::Put(v), _) => {
                        client.last_seen.insert(h.key.clone(), Some(v.clone()));
                    }
                    _ => {}
                }
                self.event_seq += 1;
                h.output = Some(result);
                h.ret = Some(self.event_seq);
                self.stats.ops_completed += 1;
            }
            ClientResponse::NotLeader {
                leader_id: Some(l), ..
            } => {
                client.leader_guess = l;
                op.retry_at = Some(now + 1);
            }
            ClientResponse::NotLeader {
                leader_id: None, ..
            }
            | ClientResponse::Unavailable(_) => {
                client.leader_guess = client.leader_guess % n + 1;
                op.retry_at = Some(now + backoff);
            }
            ClientResponse::Error(e) => return Err(format!("unexpected error response: {e}")),
            ClientResponse::Status(_) => return Err("unexpected status response".into()),
        }
        Ok(())
    }

    fn trace_hash(&self) -> u64 {
        let mut h = fnv1a(b"raft-kv-sim");
        for e in &self.history {
            h = fnv1a_extend(h, e.client);
            h = fnv1a_extend(h, e.call);
            h = fnv1a_extend(h, e.ret.unwrap_or(0));
            h = fnv1a_extend(h, fnv1a(format!("{:?}{:?}", e.input, e.output).as_bytes()));
        }
        for n in &self.nodes {
            if let Some(kv) = &n.kv {
                h = fnv1a_extend(h, kv.raft().commit_index());
                h = fnv1a_extend(h, kv.raft().term());
                h = fnv1a_extend(h, kv.state_machine().digest());
            }
        }
        h
    }
}
