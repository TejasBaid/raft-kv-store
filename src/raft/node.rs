//! The Raft consensus state machine.
//!
//! [`RaftNode`] performs no I/O and reads no clocks. The embedding application drives it
//! with three kinds of input:
//!
//! * [`RaftNode::tick`] advances logical time by one tick,
//! * [`RaftNode::step`] delivers a message from a peer,
//! * [`RaftNode::propose`] / [`RaftNode::read_index`] submit client work,
//!
//! and periodically collects the resulting effects with [`RaftNode::ready`]. Because
//! all randomness comes from a seeded RNG, a node's behaviour is a pure function of its
//! configuration, its persistent state and the sequence of inputs. That is what makes
//! the deterministic simulator in [`crate::sim`] possible.

use std::collections::{BTreeMap, VecDeque};

use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

use super::log::{LogWrite, RaftLog};
use super::message::{Message, MessageKind};
use super::types::{
    Entry, EntryPayload, HardState, LogIndex, NodeId, PersistentState, Role, Snapshot, Term,
};

/// Tunables for a Raft node. Durations are measured in ticks.
#[derive(Clone, Debug)]
pub struct Config {
    /// This node's id.
    pub id: NodeId,
    /// All voting members of the cluster, including this node.
    pub voters: Vec<NodeId>,
    /// Election timeouts are drawn uniformly from `[election_timeout_min, election_timeout_max)`.
    pub election_timeout_min: u32,
    pub election_timeout_max: u32,
    /// How often a leader sends heartbeats.
    pub heartbeat_interval: u32,
    /// Maximum entries carried by a single AppendEntries message.
    pub max_entries_per_append: usize,
    /// Maximum number of unacknowledged entries in flight to one follower.
    pub max_inflight_entries: u64,
    /// Run a pre-vote round before incrementing the term (Raft thesis §9.6). Prevents a
    /// node that was partitioned away from disrupting a healthy leader when it rejoins.
    pub pre_vote: bool,
    /// Leader steps down if it cannot reach a quorum within an election timeout.
    pub check_quorum: bool,
    /// Seed for randomized election timeouts.
    pub seed: u64,
}

impl Config {
    /// A configuration with sensible defaults for a node with the given voters.
    pub fn new(id: NodeId, voters: Vec<NodeId>) -> Self {
        Self {
            id,
            voters,
            election_timeout_min: 10,
            election_timeout_max: 20,
            heartbeat_interval: 2,
            max_entries_per_append: 512,
            max_inflight_entries: 8192,
            pre_vote: true,
            check_quorum: true,
            seed: id,
        }
    }

    fn validate(&self) {
        assert!(self.id != 0, "node id 0 is reserved");
        assert!(self.voters.contains(&self.id), "voters must include self");
        assert!(self.election_timeout_min > self.heartbeat_interval);
        assert!(self.election_timeout_max > self.election_timeout_min);
        assert!(self.max_entries_per_append > 0);
        assert!(self.max_inflight_entries > 0);
    }
}

/// A linearizable read point produced by the ReadIndex protocol: once the application
/// has applied up to `index`, it may serve the read identified by `ctx`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReadState {
    pub ctx: u64,
    pub index: LogIndex,
}

/// Effects produced by the node that the application must carry out, **in this order**:
///
/// 1. send `pre_persist_messages` (optional optimisation; they may also be sent later);
/// 2. durably persist `hard_state`, `snapshot` and `log_write`;
/// 3. call [`RaftNode::advance`];
/// 4. send `messages`;
/// 5. restore the state machine from `snapshot` (if any), then apply `committed` entries;
/// 6. serve `read_states` once the state machine has applied up to their index.
///
/// Persisting before sending `messages` is what makes a vote or an acknowledgement a
/// promise that survives crashes. `pre_persist_messages` are a leader's replication
/// traffic, which promises nothing about its own disk: sending them first overlaps the
/// leader's fsync with the followers' (Raft thesis §10.2.1). The leader counts itself
/// towards a commit quorum only for entries confirmed durable via `advance`.
#[derive(Debug, Default)]
pub struct Ready {
    pub pre_persist_messages: Vec<Message>,
    pub hard_state: Option<HardState>,
    pub log_write: Option<LogWrite>,
    pub snapshot: Option<Snapshot>,
    pub messages: Vec<Message>,
    pub committed: Vec<Entry>,
    pub read_states: Vec<ReadState>,
}

/// Returned when a request reaches a node that is not the leader.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error("not the leader (leader hint: {leader_hint:?})")]
pub struct NotLeader {
    pub leader_hint: Option<NodeId>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ProgressState {
    /// Unknown follower position: send one AppendEntries at a time and back off on rejection.
    Probe,
    /// Follower is tracking us: stream entries optimistically, up to the inflight window.
    Replicate,
    /// A snapshot has been sent; wait for it to be acknowledged.
    Snapshot,
}

/// Leader's view of one follower (Raft Figure 2 `nextIndex` / `matchIndex`, plus flow control).
#[derive(Clone, Debug)]
struct Progress {
    match_index: LogIndex,
    next_index: LogIndex,
    state: ProgressState,
    /// In `Probe`, an AppendEntries is outstanding.
    probe_sent: bool,
    /// In `Snapshot`, when (in ticks) the snapshot was last sent.
    snapshot_sent_at: u64,
    /// `match_index` at the previous heartbeat; used to detect lost AppendEntries.
    stall_mark: LogIndex,
    /// Highest ReadIndex sequence number this follower has acknowledged.
    read_ack: u64,
    /// Heard from this follower since the last check-quorum round.
    recent_active: bool,
}

impl Progress {
    fn new(next_index: LogIndex) -> Self {
        Self {
            match_index: 0,
            next_index,
            state: ProgressState::Probe,
            probe_sent: false,
            snapshot_sent_at: 0,
            stall_mark: 0,
            read_ack: 0,
            recent_active: true,
        }
    }

    fn become_probe(&mut self) {
        self.state = ProgressState::Probe;
        self.next_index = self.match_index + 1;
        self.probe_sent = false;
    }
}

#[derive(Clone, Copy, Debug)]
struct PendingRead {
    ctx: u64,
    index: LogIndex,
    seq: u64,
}

/// A single Raft peer.
pub struct RaftNode {
    cfg: Config,
    voters: Vec<NodeId>,

    // Persistent state (Figure 2).
    term: Term,
    voted_for: Option<NodeId>,
    log: RaftLog,

    // Volatile state.
    role: Role,
    leader: Option<NodeId>,
    commit_index: LogIndex,
    last_applied: LogIndex,

    ticks: u64,
    election_elapsed: u32,
    randomized_election_timeout: u32,
    heartbeat_elapsed: u32,

    /// Votes received in the current (pre-)election. `true` = granted.
    votes: BTreeMap<NodeId, bool>,

    // Leader state.
    progress: BTreeMap<NodeId, Progress>,
    replicate_pending: bool,

    // ReadIndex state.
    read_seq: u64,
    read_broadcast_pending: bool,
    pending_reads: VecDeque<PendingRead>,
    reads_awaiting_commit: Vec<u64>,
    ready_reads: Vec<ReadState>,

    // Outputs not yet collected by `ready()`.
    msgs: Vec<Message>,
    leader_msgs: Vec<Message>,
    pending_snapshot: Option<Snapshot>,
    persisted_hard_state: HardState,

    rng: ChaCha8Rng,
}

impl RaftNode {
    /// Creates a node from its configuration and whatever was recovered from stable storage.
    pub fn new(cfg: Config, state: PersistentState) -> Self {
        cfg.validate();
        let mut voters = cfg.voters.clone();
        voters.sort_unstable();
        voters.dedup();
        let log = RaftLog::new(state.snapshot, state.entries);
        let base = log.snapshot_index();
        let rng = ChaCha8Rng::seed_from_u64(cfg.seed);
        let mut node = Self {
            voters,
            term: state.hard_state.term,
            voted_for: state.hard_state.voted_for,
            log,
            role: Role::Follower,
            leader: None,
            commit_index: base,
            last_applied: base,
            ticks: 0,
            election_elapsed: 0,
            randomized_election_timeout: 0,
            heartbeat_elapsed: 0,
            votes: BTreeMap::new(),
            progress: BTreeMap::new(),
            replicate_pending: false,
            read_seq: 0,
            read_broadcast_pending: false,
            pending_reads: VecDeque::new(),
            reads_awaiting_commit: Vec::new(),
            ready_reads: Vec::new(),
            msgs: Vec::new(),
            leader_msgs: Vec::new(),
            pending_snapshot: None,
            persisted_hard_state: state.hard_state,
            rng,
            cfg,
        };
        node.reset_election_timer();
        node
    }

    // ------------------------------------------------------------------ accessors

    pub fn id(&self) -> NodeId {
        self.cfg.id
    }
    pub fn term(&self) -> Term {
        self.term
    }
    pub fn role(&self) -> Role {
        self.role
    }
    pub fn is_leader(&self) -> bool {
        self.role == Role::Leader
    }
    /// The leader this node currently believes in, if any.
    pub fn leader(&self) -> Option<NodeId> {
        self.leader
    }
    pub fn commit_index(&self) -> LogIndex {
        self.commit_index
    }
    pub fn last_applied(&self) -> LogIndex {
        self.last_applied
    }
    pub fn log(&self) -> &RaftLog {
        &self.log
    }
    pub fn voters(&self) -> &[NodeId] {
        &self.voters
    }
    pub fn hard_state(&self) -> HardState {
        HardState {
            term: self.term,
            voted_for: self.voted_for,
        }
    }

    fn quorum(&self) -> usize {
        self.voters.len() / 2 + 1
    }

    fn peers(&self) -> Vec<NodeId> {
        self.voters
            .iter()
            .copied()
            .filter(|&p| p != self.cfg.id)
            .collect()
    }

    // ------------------------------------------------------------------ inputs

    /// Advances logical time by one tick.
    pub fn tick(&mut self) {
        self.ticks += 1;
        if self.role == Role::Leader {
            self.tick_leader();
        } else {
            self.election_elapsed += 1;
            if self.election_elapsed >= self.randomized_election_timeout {
                self.campaign();
            }
        }
    }

    fn tick_leader(&mut self) {
        self.heartbeat_elapsed += 1;
        self.election_elapsed += 1;
        if self.cfg.check_quorum && self.election_elapsed >= self.cfg.election_timeout_max {
            self.election_elapsed = 0;
            let active = 1 + self.progress.values().filter(|p| p.recent_active).count();
            for p in self.progress.values_mut() {
                p.recent_active = false;
            }
            if active < self.quorum() {
                let term = self.term;
                self.become_follower(term, None);
                return;
            }
        }
        if self.heartbeat_elapsed >= self.cfg.heartbeat_interval {
            self.heartbeat_elapsed = 0;
            self.detect_stalled_followers();
            self.broadcast_heartbeat();
        }
    }

    /// Proposes a command. On success returns the (index, term) it was appended at; the
    /// command is committed only once it shows up in [`Ready::committed`] with that term.
    pub fn propose(&mut self, data: Vec<u8>) -> Result<(LogIndex, Term), NotLeader> {
        if self.role != Role::Leader {
            return Err(NotLeader {
                leader_hint: self.leader,
            });
        }
        let index = self.log.append(self.term, EntryPayload::Command(data));
        // Replication is deferred to `ready()` so that a burst of proposals is sent as one batch.
        self.replicate_pending = true;
        self.maybe_commit();
        Ok((index, self.term))
    }

    /// Starts a linearizable read using the ReadIndex protocol (Raft thesis §6.4).
    ///
    /// The read is identified by the caller-chosen `ctx`. A matching [`ReadState`] is
    /// emitted once this node has confirmed, via a round of heartbeats acknowledged by a
    /// quorum, that it was still the leader after the read arrived.
    pub fn read_index(&mut self, ctx: u64) -> Result<(), NotLeader> {
        if self.role != Role::Leader {
            return Err(NotLeader {
                leader_hint: self.leader,
            });
        }
        if self.log.term(self.commit_index) != Some(self.term) {
            // A new leader does not know the true commit index until an entry from its own
            // term commits; defer until the election no-op is committed.
            self.reads_awaiting_commit.push(ctx);
        } else {
            self.register_read(ctx);
        }
        Ok(())
    }

    fn register_read(&mut self, ctx: u64) {
        let read = PendingRead {
            ctx,
            index: self.commit_index,
            seq: self.read_seq + 1,
        };
        if self.quorum() == 1 {
            self.ready_reads.push(ReadState {
                ctx,
                index: read.index,
            });
            return;
        }
        self.pending_reads.push_back(read);
        self.read_broadcast_pending = true;
    }

    /// Replaces the log prefix up to `index` (which must already be applied) with `data`,
    /// a serialized image of the state machine at that index.
    pub fn compact(&mut self, index: LogIndex, data: Vec<u8>) {
        assert!(
            index <= self.last_applied,
            "cannot compact unapplied entries"
        );
        self.log.compact(index, data);
    }

    /// Handles a message from a peer.
    pub fn step(&mut self, m: Message) {
        if m.to != self.cfg.id || !self.voters.contains(&m.from) || m.from == self.cfg.id {
            return;
        }

        if m.term > self.term {
            match m.kind {
                // Pre-vote traffic never changes our term: the sender has not actually
                // started an election.
                MessageKind::VoteRequest { pre_vote: true, .. }
                | MessageKind::VoteResponse {
                    pre_vote: true,
                    granted: true,
                } => {}
                MessageKind::AppendEntries { .. }
                | MessageKind::Heartbeat { .. }
                | MessageKind::InstallSnapshot { .. } => self.become_follower(m.term, Some(m.from)),
                _ => self.become_follower(m.term, None),
            }
        } else if m.term < self.term {
            match m.kind {
                // A deposed leader must learn about the new term so that it steps down.
                MessageKind::AppendEntries { .. }
                | MessageKind::Heartbeat { .. }
                | MessageKind::InstallSnapshot { .. } => {
                    self.send(
                        m.from,
                        MessageKind::AppendResponse {
                            success: false,
                            match_index: 0,
                            rejected: 0,
                            hint_index: 0,
                            hint_term: None,
                        },
                    );
                }
                MessageKind::VoteRequest { pre_vote: true, .. } => {
                    self.send(
                        m.from,
                        MessageKind::VoteResponse {
                            pre_vote: true,
                            granted: false,
                        },
                    );
                }
                _ => {}
            }
            return;
        }

        let from = m.from;
        let term = m.term;
        match m.kind {
            MessageKind::VoteRequest {
                pre_vote,
                last_log_index,
                last_log_term,
            } => self.handle_vote_request(from, term, pre_vote, last_log_index, last_log_term),
            MessageKind::VoteResponse { pre_vote, granted } => {
                self.handle_vote_response(from, term, pre_vote, granted)
            }
            MessageKind::AppendEntries {
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            } => self.handle_append_entries(
                from,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            ),
            MessageKind::AppendResponse {
                success,
                match_index,
                rejected,
                hint_index,
                hint_term,
            } => self.handle_append_response(
                from,
                success,
                match_index,
                rejected,
                hint_index,
                hint_term,
            ),
            MessageKind::Heartbeat { commit, read_seq } => {
                self.handle_heartbeat(from, commit, read_seq)
            }
            MessageKind::HeartbeatResponse { read_seq } => {
                self.handle_heartbeat_response(from, read_seq)
            }
            MessageKind::InstallSnapshot { snapshot } => {
                self.handle_install_snapshot(from, snapshot)
            }
            MessageKind::InstallSnapshotResponse { last_index } => {
                self.handle_install_snapshot_response(from, last_index)
            }
        }
    }

    /// Whether [`ready`](Self::ready) has anything to return.
    pub fn has_ready(&self) -> bool {
        self.read_broadcast_pending
            || self.replicate_pending
            || !self.msgs.is_empty()
            || !self.leader_msgs.is_empty()
            || self.pending_snapshot.is_some()
            || !self.ready_reads.is_empty()
            || self.commit_index > self.last_applied
            || self.hard_state() != self.persisted_hard_state
            || self.log.has_unstable()
    }

    /// Collects all pending effects. See [`Ready`] for the contract the caller must follow.
    pub fn ready(&mut self) -> Ready {
        if self.read_broadcast_pending && self.role == Role::Leader {
            self.read_seq += 1;
            self.broadcast_heartbeat();
        }
        self.read_broadcast_pending = false;
        if self.replicate_pending && self.role == Role::Leader {
            self.broadcast_append(false);
        }
        self.replicate_pending = false;

        let hs = self.hard_state();
        let hard_state = (hs != self.persisted_hard_state).then(|| {
            self.persisted_hard_state = hs;
            hs
        });

        let committed = if self.commit_index > self.last_applied {
            let entries = self.log.slice(self.last_applied + 1, self.commit_index);
            self.last_applied = self.commit_index;
            entries
        } else {
            Vec::new()
        };

        Ready {
            pre_persist_messages: std::mem::take(&mut self.leader_msgs),
            hard_state,
            log_write: self.log.take_unstable(),
            snapshot: self.pending_snapshot.take(),
            messages: std::mem::take(&mut self.msgs),
            committed,
            read_states: std::mem::take(&mut self.ready_reads),
        }
    }

    /// Confirms that everything returned by the last [`ready`](Self::ready) is durable.
    /// May advance the commit index (a leader counts only its durable entries).
    pub fn advance(&mut self) {
        self.log.mark_stable();
        if self.maybe_commit() {
            self.broadcast_append(true);
        }
    }

    // ------------------------------------------------------------------ role transitions

    fn reset_election_timer(&mut self) {
        self.election_elapsed = 0;
        self.randomized_election_timeout = self
            .rng
            .gen_range(self.cfg.election_timeout_min..self.cfg.election_timeout_max);
    }

    fn clear_leader_state(&mut self) {
        self.progress.clear();
        self.replicate_pending = false;
        self.pending_reads.clear();
        self.reads_awaiting_commit.clear();
        self.read_broadcast_pending = false;
        self.votes.clear();
    }

    fn become_follower(&mut self, term: Term, leader: Option<NodeId>) {
        if term > self.term {
            self.term = term;
            self.voted_for = None;
        }
        self.role = Role::Follower;
        self.leader = leader;
        self.reset_election_timer();
        self.clear_leader_state();
    }

    fn campaign(&mut self) {
        if self.cfg.pre_vote {
            self.become_pre_candidate();
        } else {
            self.become_candidate();
        }
    }

    fn become_pre_candidate(&mut self) {
        self.clear_leader_state();
        self.role = Role::PreCandidate;
        self.leader = None;
        self.reset_election_timer();
        self.votes.insert(self.cfg.id, true);
        if self.quorum() == 1 {
            self.become_candidate();
            return;
        }
        let kind = MessageKind::VoteRequest {
            pre_vote: true,
            last_log_index: self.log.last_index(),
            last_log_term: self.log.last_term(),
        };
        for peer in self.peers() {
            self.send_with_term(peer, self.term + 1, kind.clone());
        }
    }

    fn become_candidate(&mut self) {
        self.clear_leader_state();
        self.term += 1;
        self.voted_for = Some(self.cfg.id);
        self.role = Role::Candidate;
        self.leader = None;
        self.reset_election_timer();
        self.votes.insert(self.cfg.id, true);
        if self.quorum() == 1 {
            self.become_leader();
            return;
        }
        let kind = MessageKind::VoteRequest {
            pre_vote: false,
            last_log_index: self.log.last_index(),
            last_log_term: self.log.last_term(),
        };
        for peer in self.peers() {
            self.send(peer, kind.clone());
        }
    }

    fn become_leader(&mut self) {
        self.clear_leader_state();
        self.role = Role::Leader;
        self.leader = Some(self.cfg.id);
        self.heartbeat_elapsed = 0;
        self.election_elapsed = 0;
        let next = self.log.last_index() + 1;
        self.progress = self
            .peers()
            .into_iter()
            .map(|p| (p, Progress::new(next)))
            .collect();
        self.log.append(self.term, EntryPayload::Noop);
        self.maybe_commit();
        self.broadcast_append(true);
    }

    // ------------------------------------------------------------------ elections

    fn handle_vote_request(
        &mut self,
        from: NodeId,
        term: Term,
        pre_vote: bool,
        last_log_index: LogIndex,
        last_log_term: Term,
    ) {
        let up_to_date = self.log.is_up_to_date(last_log_index, last_log_term);
        if pre_vote {
            // Reject if we have heard from a live leader recently ("leader stickiness").
            let in_lease = self.role == Role::Leader
                || (self.leader.is_some() && self.election_elapsed < self.cfg.election_timeout_min);
            let granted = term > self.term && up_to_date && !in_lease;
            let reply_term = if granted { term } else { self.term };
            self.send_with_term(
                from,
                reply_term,
                MessageKind::VoteResponse {
                    pre_vote: true,
                    granted,
                },
            );
            return;
        }
        // Here `term == self.term` (a higher term was adopted in `step`).
        let can_vote =
            self.voted_for == Some(from) || (self.voted_for.is_none() && self.leader.is_none());
        let granted = can_vote && up_to_date;
        if granted {
            self.voted_for = Some(from);
            self.election_elapsed = 0;
        }
        self.send(
            from,
            MessageKind::VoteResponse {
                pre_vote: false,
                granted,
            },
        );
    }

    fn handle_vote_response(&mut self, from: NodeId, term: Term, pre_vote: bool, granted: bool) {
        let relevant = match (self.role, pre_vote) {
            (Role::PreCandidate, true) => true,
            (Role::Candidate, false) => term == self.term,
            _ => false,
        };
        if !relevant {
            return;
        }
        self.votes.insert(from, granted);
        let yes = self.votes.values().filter(|&&g| g).count();
        let no = self.votes.len() - yes;
        if yes >= self.quorum() {
            if pre_vote {
                self.become_candidate();
            } else {
                self.become_leader();
            }
        } else if no >= self.quorum() {
            let t = self.term;
            self.become_follower(t, None);
        }
    }

    // ------------------------------------------------------------------ follower side

    /// Called on any valid message from the current term's leader.
    fn on_leader_contact(&mut self, leader: NodeId) {
        if self.role != Role::Follower {
            let t = self.term;
            self.become_follower(t, Some(leader));
        } else {
            self.leader = Some(leader);
            self.election_elapsed = 0;
        }
    }

    fn handle_append_entries(
        &mut self,
        from: NodeId,
        prev_log_index: LogIndex,
        prev_log_term: Term,
        entries: Vec<Entry>,
        leader_commit: LogIndex,
    ) {
        debug_assert!(
            self.role != Role::Leader,
            "two leaders in term {}",
            self.term
        );
        self.on_leader_contact(from);

        if prev_log_index < self.commit_index {
            // Everything up to our commit index is known to match the leader.
            let match_index = self.commit_index;
            self.send(from, append_ok(match_index));
            return;
        }

        match self.log.term(prev_log_index) {
            Some(t) if t == prev_log_term => {
                let last_new = prev_log_index + entries.len() as u64;
                self.log.append_entries(entries, self.commit_index);
                let new_commit = leader_commit.min(last_new);
                if new_commit > self.commit_index {
                    self.commit_index = new_commit;
                }
                self.send(from, append_ok(last_new));
            }
            Some(t) => {
                // Conflict: report the first index of the conflicting term so the leader
                // can skip the whole term in one round trip.
                let floor = self.log.first_index().max(self.commit_index + 1);
                let mut hint = prev_log_index;
                while hint > floor && self.log.term(hint - 1) == Some(t) {
                    hint -= 1;
                }
                self.send(
                    from,
                    MessageKind::AppendResponse {
                        success: false,
                        match_index: 0,
                        rejected: prev_log_index,
                        hint_index: hint,
                        hint_term: Some(t),
                    },
                );
            }
            None => {
                let hint_index = self.log.last_index() + 1;
                self.send(
                    from,
                    MessageKind::AppendResponse {
                        success: false,
                        match_index: 0,
                        rejected: prev_log_index,
                        hint_index,
                        hint_term: None,
                    },
                );
            }
        }
    }

    fn handle_heartbeat(&mut self, from: NodeId, commit: LogIndex, read_seq: u64) {
        self.on_leader_contact(from);
        let commit = commit.min(self.log.last_index());
        if commit > self.commit_index {
            self.commit_index = commit;
        }
        self.send(from, MessageKind::HeartbeatResponse { read_seq });
    }

    fn handle_install_snapshot(&mut self, from: NodeId, snapshot: Snapshot) {
        self.on_leader_contact(from);
        let idx = snapshot.meta.last_index;
        if idx <= self.commit_index {
            let last_index = self.commit_index;
            self.send(from, MessageKind::InstallSnapshotResponse { last_index });
            return;
        }
        self.log.install_snapshot(snapshot.clone());
        self.commit_index = idx;
        self.last_applied = idx;
        self.pending_snapshot = Some(snapshot);
        self.send(
            from,
            MessageKind::InstallSnapshotResponse { last_index: idx },
        );
    }

    // ------------------------------------------------------------------ leader side

    fn handle_append_response(
        &mut self,
        from: NodeId,
        success: bool,
        match_index: LogIndex,
        rejected: LogIndex,
        hint_index: LogIndex,
        hint_term: Option<Term>,
    ) {
        if self.role != Role::Leader {
            return;
        }
        let next_from_hint = self.next_from_hint(rejected, hint_index, hint_term);
        let Some(pr) = self.progress.get_mut(&from) else {
            return;
        };
        pr.recent_active = true;
        if success {
            pr.probe_sent = false;
            if match_index > pr.match_index {
                pr.match_index = match_index;
            }
            if pr.next_index <= pr.match_index {
                pr.next_index = pr.match_index + 1;
            }
            match pr.state {
                ProgressState::Probe => {
                    pr.state = ProgressState::Replicate;
                    pr.next_index = pr.match_index + 1;
                }
                ProgressState::Snapshot if pr.match_index >= self.log.snapshot_index() => {
                    pr.state = ProgressState::Replicate;
                    pr.next_index = pr.match_index + 1;
                }
                _ => {}
            }
            if self.maybe_commit() {
                self.broadcast_append(true);
            } else {
                self.send_append(from, false);
            }
        } else {
            if rejected <= pr.match_index {
                return; // Stale rejection; we already know the follower matches beyond it.
            }
            pr.next_index = next_from_hint.min(rejected).max(pr.match_index + 1);
            pr.state = ProgressState::Probe;
            pr.probe_sent = false;
            self.send_append(from, true);
        }
    }

    /// Picks the next index to probe after a rejection (fast backtracking).
    fn next_from_hint(
        &self,
        rejected: LogIndex,
        hint_index: LogIndex,
        hint_term: Option<Term>,
    ) -> LogIndex {
        let Some(t) = hint_term else {
            return hint_index.max(1);
        };
        // If we have entries from term `t`, resume just after our last one; otherwise
        // skip the follower's whole conflicting term.
        let mut idx = rejected.min(self.log.last_index());
        while idx >= self.log.first_index() {
            match self.log.term(idx) {
                Some(term) if term == t => return idx + 1,
                Some(term) if term < t => break,
                _ => idx -= 1,
            }
        }
        hint_index.max(1)
    }

    fn handle_heartbeat_response(&mut self, from: NodeId, read_seq: u64) {
        if self.role != Role::Leader {
            return;
        }
        let last = self.log.last_index();
        let Some(pr) = self.progress.get_mut(&from) else {
            return;
        };
        pr.recent_active = true;
        pr.read_ack = pr.read_ack.max(read_seq);
        pr.probe_sent = false;
        let behind = pr.match_index < last;
        if behind {
            self.send_append(from, false);
        }
        self.advance_reads();
    }

    fn handle_install_snapshot_response(&mut self, from: NodeId, last_index: LogIndex) {
        if self.role != Role::Leader {
            return;
        }
        let Some(pr) = self.progress.get_mut(&from) else {
            return;
        };
        pr.recent_active = true;
        if last_index > pr.match_index {
            pr.match_index = last_index;
        }
        if pr.state == ProgressState::Snapshot {
            pr.become_probe();
        }
        if self.maybe_commit() {
            self.broadcast_append(true);
        } else {
            self.send_append(from, false);
        }
    }

    /// Advances the commit index to the highest index replicated on a quorum, provided
    /// that entry is from the current term (Raft §5.4.2). Returns whether it moved.
    fn maybe_commit(&mut self) -> bool {
        if self.role != Role::Leader {
            return false;
        }
        let mut matches: Vec<LogIndex> = self.progress.values().map(|p| p.match_index).collect();
        matches.push(self.log.stable_index());
        matches.sort_unstable_by(|a, b| b.cmp(a));
        let candidate = matches[self.quorum() - 1];
        if candidate > self.commit_index && self.log.term(candidate) == Some(self.term) {
            self.commit_index = candidate;
            if !self.reads_awaiting_commit.is_empty() {
                for ctx in std::mem::take(&mut self.reads_awaiting_commit) {
                    self.register_read(ctx);
                }
            }
            true
        } else {
            false
        }
    }

    /// Emits every pending read whose heartbeat round has been acknowledged by a quorum.
    fn advance_reads(&mut self) {
        let quorum = self.quorum();
        while let Some(read) = self.pending_reads.front().copied() {
            let acks = 1 + self
                .progress
                .values()
                .filter(|p| p.read_ack >= read.seq)
                .count();
            if acks < quorum {
                break;
            }
            self.pending_reads.pop_front();
            self.ready_reads.push(ReadState {
                ctx: read.ctx,
                index: read.index,
            });
        }
    }

    fn broadcast_append(&mut self, allow_empty: bool) {
        for peer in self.peers() {
            self.send_append(peer, allow_empty);
        }
    }

    /// Sends the next batch of entries to `to`, a snapshot if the entries it needs were
    /// compacted, or nothing if flow control says to wait.
    fn send_append(&mut self, to: NodeId, allow_empty: bool) {
        let last = self.log.last_index();
        let max_entries = self.cfg.max_entries_per_append;
        let max_inflight = self.cfg.max_inflight_entries;
        let Some(pr) = self.progress.get(&to) else {
            return;
        };
        let has_entries = pr.next_index <= last;
        let paused = match pr.state {
            ProgressState::Snapshot => true,
            ProgressState::Probe => pr.probe_sent,
            ProgressState::Replicate => {
                has_entries && pr.next_index > pr.match_index + max_inflight
            }
        };
        if paused || (!has_entries && !allow_empty) {
            return;
        }
        let prev_log_index = pr.next_index - 1;
        let Some(prev_log_term) = self.log.term(prev_log_index) else {
            self.send_snapshot(to);
            return;
        };
        let entries = if has_entries {
            self.log.entries_from(pr.next_index, max_entries)
        } else {
            Vec::new()
        };
        let pr = self.progress.get_mut(&to).expect("checked above");
        match pr.state {
            ProgressState::Replicate => {
                if let Some(e) = entries.last() {
                    pr.next_index = e.index + 1;
                }
            }
            ProgressState::Probe => pr.probe_sent = true,
            ProgressState::Snapshot => unreachable!(),
        }
        let leader_commit = self.commit_index;
        self.send(
            to,
            MessageKind::AppendEntries {
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            },
        );
    }

    fn send_snapshot(&mut self, to: NodeId) {
        let ticks = self.ticks;
        let snapshot = self.log.snapshot().clone();
        if let Some(pr) = self.progress.get_mut(&to) {
            pr.state = ProgressState::Snapshot;
            pr.snapshot_sent_at = ticks;
        }
        self.send(to, MessageKind::InstallSnapshot { snapshot });
    }

    /// Recovers from lost AppendEntries or InstallSnapshot messages. Optimistic
    /// replication assumes delivery; if a follower made no progress over a whole
    /// heartbeat interval we fall back to probing from its last known match.
    fn detect_stalled_followers(&mut self) {
        let last = self.log.last_index();
        let resend_after = 2 * u64::from(self.cfg.election_timeout_max);
        let ticks = self.ticks;
        let mut resend = Vec::new();
        for (&id, pr) in self.progress.iter_mut() {
            let behind = pr.match_index < last;
            match pr.state {
                ProgressState::Replicate if behind && pr.match_index == pr.stall_mark => {
                    pr.become_probe();
                    resend.push(id);
                }
                ProgressState::Snapshot if ticks - pr.snapshot_sent_at >= resend_after => {
                    pr.become_probe();
                    resend.push(id);
                }
                _ => {}
            }
            pr.stall_mark = pr.match_index;
        }
        for id in resend {
            self.send_append(id, false);
        }
    }

    fn broadcast_heartbeat(&mut self) {
        let read_seq = self.read_seq;
        let peers: Vec<(NodeId, LogIndex)> = self
            .progress
            .iter()
            .map(|(&id, p)| (id, p.match_index.min(self.commit_index)))
            .collect();
        for (peer, commit) in peers {
            self.send(peer, MessageKind::Heartbeat { commit, read_seq });
        }
    }

    // ------------------------------------------------------------------ helpers

    fn send(&mut self, to: NodeId, kind: MessageKind) {
        self.send_with_term(to, self.term, kind);
    }

    fn send_with_term(&mut self, to: NodeId, term: Term, kind: MessageKind) {
        let leader_traffic = self.role == Role::Leader
            && matches!(
                kind,
                MessageKind::AppendEntries { .. }
                    | MessageKind::Heartbeat { .. }
                    | MessageKind::InstallSnapshot { .. }
            );
        let msg = Message {
            from: self.cfg.id,
            to,
            term,
            kind,
        };
        if leader_traffic {
            self.leader_msgs.push(msg);
        } else {
            self.msgs.push(msg);
        }
    }
}

fn append_ok(match_index: LogIndex) -> MessageKind {
    MessageKind::AppendResponse {
        success: true,
        match_index,
        rejected: 0,
        hint_index: 0,
        hint_term: None,
    }
}
