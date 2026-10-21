//! A complete key-value server node, minus the I/O.
//!
//! [`KvNode`] glues the Raft core to the KV state machine: it turns client requests into
//! proposals or ReadIndex requests, applies committed entries, matches results back to
//! waiting requests, and decides when to snapshot. Like [`RaftNode`] it is sans-I/O, so
//! the exact same code runs inside the tokio server ([`crate::server`]) and inside the
//! deterministic simulator ([`crate::sim`]).

use std::collections::BTreeMap;

use crate::kv::{KvStateMachine, WriteRequest};
use crate::protocol::{ClientRequest, ClientResponse, NodeStatus};
use crate::raft::{self, EntryPayload, LogIndex, Message, NodeId, PersistentState, RaftNode, Term};
use crate::storage::Persist;
use crate::util::fnv1a;

/// Opaque handle the driver uses to route a response back to the right client.
pub type RequestId = u64;

/// How `Get` requests are served.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadMode {
    /// Linearizable reads via the ReadIndex protocol (the default).
    ReadIndex,
    /// **Unsafe, for testing only**: a node that believes it is leader answers from its
    /// local state immediately. A deposed leader can return stale data; the simulator's
    /// linearizability checker is expected to catch this.
    UnsafeLeaderLocal,
}

#[derive(Clone, Debug)]
pub struct NodeConfig {
    pub raft: raft::Config,
    /// Snapshot once this many entries have been applied since the last snapshot.
    pub snapshot_threshold: u64,
    /// Client-facing addresses of all nodes, used to give clients leader hints.
    pub addrs: BTreeMap<NodeId, String>,
    pub read_mode: ReadMode,
}

impl NodeConfig {
    pub fn new(raft: raft::Config) -> Self {
        Self {
            raft,
            snapshot_threshold: 10_000,
            addrs: BTreeMap::new(),
            read_mode: ReadMode::ReadIndex,
        }
    }
}

/// An applied log entry, reported for safety checking.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AppliedEntry {
    pub index: LogIndex,
    pub term: Term,
    pub digest: u64,
}

/// Effects to carry out after [`KvNode::poll`], in this order: send
/// `pre_persist_messages`, make `persist` durable, call [`KvNode::advance`], then send
/// `messages` and `responses`. See [`raft::Ready`] for why.
#[derive(Debug, Default)]
pub struct Output {
    pub pre_persist_messages: Vec<Message>,
    pub persist: Option<Persist>,
    pub messages: Vec<Message>,
    pub responses: Vec<(RequestId, ClientResponse)>,
    pub applied: Vec<AppliedEntry>,
}

impl Output {
    pub fn is_empty(&self) -> bool {
        self.pre_persist_messages.is_empty()
            && self.persist.is_none()
            && self.messages.is_empty()
            && self.responses.is_empty()
            && self.applied.is_empty()
    }
}

#[derive(Clone, Copy, Debug)]
struct PendingWrite {
    term: Term,
    req: RequestId,
}

#[derive(Clone, Debug)]
struct PendingRead {
    term: Term,
    req: RequestId,
    key: String,
}

pub struct KvNode {
    raft: RaftNode,
    sm: KvStateMachine,
    snapshot_threshold: u64,
    addrs: BTreeMap<NodeId, String>,
    read_mode: ReadMode,

    pending_writes: BTreeMap<LogIndex, PendingWrite>,
    pending_reads: BTreeMap<u64, PendingRead>,
    /// Confirmed reads waiting for the state machine to catch up to their read index.
    waiting_reads: Vec<(LogIndex, RequestId, String)>,
    next_read_ctx: u64,
    responses: Vec<(RequestId, ClientResponse)>,
    /// The term in which this node was leader at the last poll.
    leader_term: Option<Term>,
    snapshots_taken: u64,
    snapshots_installed: u64,
}

impl KvNode {
    /// Builds a node from configuration and recovered durable state.
    pub fn new(cfg: NodeConfig, state: PersistentState) -> Result<Self, bincode::Error> {
        let sm = KvStateMachine::restore(&state.snapshot.data)?;
        Ok(Self {
            raft: RaftNode::new(cfg.raft, state),
            sm,
            snapshot_threshold: cfg.snapshot_threshold.max(1),
            addrs: cfg.addrs,
            read_mode: cfg.read_mode,
            pending_writes: BTreeMap::new(),
            pending_reads: BTreeMap::new(),
            waiting_reads: Vec::new(),
            next_read_ctx: 0,
            responses: Vec::new(),
            leader_term: None,
            snapshots_taken: 0,
            snapshots_installed: 0,
        })
    }

    pub fn raft(&self) -> &RaftNode {
        &self.raft
    }

    pub fn state_machine(&self) -> &KvStateMachine {
        &self.sm
    }

    pub fn snapshots_taken(&self) -> u64 {
        self.snapshots_taken
    }

    pub fn snapshots_installed(&self) -> u64 {
        self.snapshots_installed
    }

    pub fn status(&self) -> NodeStatus {
        NodeStatus {
            id: self.raft.id(),
            role: self.raft.role(),
            term: self.raft.term(),
            leader: self.raft.leader(),
            commit_index: self.raft.commit_index(),
            last_applied: self.raft.last_applied(),
            last_log_index: self.raft.log().last_index(),
            snapshot_index: self.raft.log().snapshot_index(),
            keys: self.sm.len(),
        }
    }

    pub fn tick(&mut self) {
        self.raft.tick();
    }

    pub fn step(&mut self, msg: Message) {
        self.raft.step(msg);
    }

    fn not_leader(&self) -> ClientResponse {
        let leader_id = self.raft.leader().filter(|&l| l != self.raft.id());
        ClientResponse::NotLeader {
            leader_id,
            leader_addr: leader_id.and_then(|id| self.addrs.get(&id).cloned()),
        }
    }

    /// Accepts a client request. The response is delivered through a later [`Output`].
    pub fn handle_client(&mut self, req_id: RequestId, req: ClientRequest) {
        match req {
            ClientRequest::Status => {
                let status = self.status();
                self.responses
                    .push((req_id, ClientResponse::Status(status)));
            }
            ClientRequest::Get { key } => {
                if self.read_mode == ReadMode::UnsafeLeaderLocal && self.raft.is_leader() {
                    let value = self.sm.get(&key).cloned();
                    self.responses.push((
                        req_id,
                        ClientResponse::Ok(crate::kv::KvResult::Value(value)),
                    ));
                    return;
                }
                let ctx = self.next_read_ctx;
                self.next_read_ctx += 1;
                match self.raft.read_index(ctx) {
                    Ok(()) => {
                        let term = self.raft.term();
                        self.pending_reads.insert(
                            ctx,
                            PendingRead {
                                term,
                                req: req_id,
                                key,
                            },
                        );
                    }
                    Err(_) => {
                        let resp = self.not_leader();
                        self.responses.push((req_id, resp));
                    }
                }
            }
            ClientRequest::Write(write) => {
                let data = bincode::serialize(&write).expect("serialization cannot fail");
                match self.raft.propose(data) {
                    Ok((index, term)) => {
                        self.pending_writes
                            .insert(index, PendingWrite { term, req: req_id });
                    }
                    Err(_) => {
                        let resp = self.not_leader();
                        self.responses.push((req_id, resp));
                    }
                }
            }
        }
    }

    /// Confirms that the `persist` of the last [`Output`] is durable. Call this before
    /// sending that output's `messages`, then poll again: it may commit new entries.
    pub fn advance(&mut self) {
        self.raft.advance();
    }

    /// Runs the Raft core and the state machine forward, collecting all effects.
    pub fn poll(&mut self) -> Output {
        let rd = self.raft.ready();
        let mut applied = Vec::new();
        let mut rewrite = false;

        if let Some(snapshot) = &rd.snapshot {
            self.sm = KvStateMachine::restore(&snapshot.data)
                .expect("snapshot from leader must be decodable");
            self.snapshots_installed += 1;
            rewrite = true;
            // These requests were committed, but their individual results are gone.
            // A retry with the same sequence number returns the cached result.
            let idx = snapshot.meta.last_index;
            let covered: Vec<LogIndex> =
                self.pending_writes.range(..=idx).map(|(&i, _)| i).collect();
            for i in covered {
                let p = self.pending_writes.remove(&i).expect("present");
                self.responses.push((
                    p.req,
                    ClientResponse::Unavailable("snapshot installed".into()),
                ));
            }
        }

        for entry in &rd.committed {
            let pending = self.pending_writes.remove(&entry.index);
            let digest = match &entry.payload {
                EntryPayload::Noop => {
                    if let Some(p) = pending {
                        self.responses.push((p.req, Self::superseded()));
                    }
                    0
                }
                EntryPayload::Command(data) => {
                    let write: WriteRequest =
                        bincode::deserialize(data).expect("log entries are produced by us");
                    let result = self.sm.apply(&write);
                    if let Some(p) = pending {
                        let resp = if p.term != entry.term {
                            Self::superseded()
                        } else {
                            match result {
                                Ok(r) => ClientResponse::Ok(r),
                                Err(e) => ClientResponse::Error(e.to_string()),
                            }
                        };
                        self.responses.push((p.req, resp));
                    }
                    fnv1a(data)
                }
            };
            applied.push(AppliedEntry {
                index: entry.index,
                term: entry.term,
                digest,
            });
        }

        let applied_index = self.raft.last_applied();
        for rs in &rd.read_states {
            if let Some(r) = self.pending_reads.remove(&rs.ctx) {
                self.waiting_reads.push((rs.index, r.req, r.key));
            }
        }
        let sm = &self.sm;
        let responses = &mut self.responses;
        self.waiting_reads.retain(|(index, req, key)| {
            if *index <= applied_index {
                let value = sm.get(key).cloned();
                responses.push((*req, ClientResponse::Ok(crate::kv::KvResult::Value(value))));
                false
            } else {
                true
            }
        });

        // Requests accepted under a leadership that has since ended can no longer be
        // answered reliably; fail them so clients retry (exactly-once via dedup).
        let leader_term = self.raft.is_leader().then(|| self.raft.term());
        if leader_term != self.leader_term {
            self.leader_term = leader_term;
            let responses = &mut self.responses;
            let lost = || ClientResponse::Unavailable("leadership changed".into());
            self.pending_writes.retain(|_, p| {
                let keep = Some(p.term) == leader_term;
                if !keep {
                    responses.push((p.req, lost()));
                }
                keep
            });
            self.pending_reads.retain(|_, r| {
                let keep = Some(r.term) == leader_term;
                if !keep {
                    responses.push((r.req, lost()));
                }
                keep
            });
        }

        // Log compaction.
        let snap_index = self.raft.log().snapshot_index();
        if applied_index - snap_index >= self.snapshot_threshold {
            self.raft.compact(applied_index, self.sm.snapshot());
            self.snapshots_taken += 1;
            rewrite = true;
        }

        let persist = if rewrite {
            Some(Persist::Rewrite {
                hard_state: self.raft.hard_state(),
                snapshot: self.raft.log().snapshot().clone(),
                entries: self.raft.log().entries().to_vec(),
            })
        } else if rd.hard_state.is_some() || rd.log_write.is_some() {
            Some(Persist::Append {
                hard_state: rd.hard_state,
                log_write: rd.log_write,
            })
        } else {
            None
        };

        Output {
            pre_persist_messages: rd.pre_persist_messages,
            persist,
            messages: rd.messages,
            responses: std::mem::take(&mut self.responses),
            applied,
        }
    }

    fn superseded() -> ClientResponse {
        ClientResponse::Unavailable("entry superseded by a new leader".into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kv::{Command, KvResult};
    use crate::storage::{MemStorage, Storage};

    fn single_node() -> (KvNode, MemStorage) {
        let mut cfg = NodeConfig::new(raft::Config::new(1, vec![1]));
        cfg.snapshot_threshold = 5;
        let node = KvNode::new(cfg, PersistentState::default()).unwrap();
        (node, MemStorage::new())
    }

    fn run(node: &mut KvNode, disk: &mut MemStorage) -> Vec<(RequestId, ClientResponse)> {
        let mut responses = Vec::new();
        loop {
            let out = node.poll();
            if out.is_empty() {
                return responses;
            }
            if let Some(p) = &out.persist {
                disk.persist(p).unwrap();
            }
            node.advance();
            responses.extend(out.responses);
        }
    }

    fn put(seq: u64, key: &str, value: &str) -> ClientRequest {
        ClientRequest::Write(WriteRequest {
            client_id: 1,
            seq,
            command: Command::Put {
                key: key.into(),
                value: value.into(),
            },
        })
    }

    fn elect(node: &mut KvNode, disk: &mut MemStorage) {
        for _ in 0..20 {
            node.tick();
        }
        run(node, disk);
        assert!(node.raft().is_leader());
    }

    #[test]
    fn write_then_read() {
        let (mut node, mut disk) = single_node();
        elect(&mut node, &mut disk);
        node.handle_client(1, put(1, "k", "v"));
        assert_eq!(
            run(&mut node, &mut disk),
            vec![(1, ClientResponse::Ok(KvResult::Written))]
        );
        node.handle_client(2, ClientRequest::Get { key: "k".into() });
        assert_eq!(
            run(&mut node, &mut disk),
            vec![(2, ClientResponse::Ok(KvResult::Value(Some(b"v".to_vec()))))]
        );
    }

    #[test]
    fn snapshots_and_recovers_from_disk() {
        let (mut node, mut disk) = single_node();
        elect(&mut node, &mut disk);
        for seq in 1..=12 {
            node.handle_client(seq, put(seq, &format!("k{seq}"), "v"));
            run(&mut node, &mut disk);
        }
        assert!(node.snapshots_taken() >= 2);
        assert!(disk.state().snapshot.meta.last_index >= 10);

        let mut cfg = NodeConfig::new(raft::Config::new(1, vec![1]));
        cfg.snapshot_threshold = 5;
        let mut restarted = KvNode::new(cfg, disk.state().clone()).unwrap();
        elect(&mut restarted, &mut disk);
        assert_eq!(restarted.state_machine().len(), 12);
    }

    #[test]
    fn follower_redirects_to_leader() {
        let mut cfg = NodeConfig::new(raft::Config::new(2, vec![1, 2, 3]));
        cfg.addrs.insert(1, "10.0.0.1:7001".into());
        let mut node = KvNode::new(cfg, PersistentState::default()).unwrap();
        node.handle_client(9, ClientRequest::Get { key: "x".into() });
        let out = node.poll();
        assert!(matches!(
            out.responses[0].1,
            ClientResponse::NotLeader {
                leader_id: None,
                ..
            }
        ));
    }
}
