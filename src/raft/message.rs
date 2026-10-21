//! Raft RPCs, modelled as one-way messages.
//!
//! Requests and responses are independent messages rather than request/response pairs,
//! which lets the core stay free of I/O and lets the transport (or the simulator) drop,
//! delay, duplicate and reorder them freely.

use serde::{Deserialize, Serialize};

use super::types::{Entry, LogIndex, NodeId, Snapshot, Term};

/// A message between two Raft peers.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Message {
    pub from: NodeId,
    pub to: NodeId,
    /// The sender's term. For a pre-vote request this is the *prospective* term
    /// (`current + 1`); the sender has not actually advanced its term.
    pub term: Term,
    pub kind: MessageKind,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MessageKind {
    /// RequestVote RPC (or a pre-vote probe when `pre_vote` is set).
    VoteRequest {
        pre_vote: bool,
        last_log_index: LogIndex,
        last_log_term: Term,
    },
    VoteResponse {
        pre_vote: bool,
        granted: bool,
    },
    /// AppendEntries RPC carrying log entries (possibly none).
    AppendEntries {
        prev_log_index: LogIndex,
        prev_log_term: Term,
        entries: Vec<Entry>,
        leader_commit: LogIndex,
    },
    AppendResponse {
        success: bool,
        /// On success: the index of the last entry known to match the leader.
        match_index: LogIndex,
        /// On rejection: the `prev_log_index` that was rejected.
        rejected: LogIndex,
        /// On rejection: where the leader should resume probing (fast log backtracking).
        hint_index: LogIndex,
        /// On rejection: the follower's term at `rejected`, if it has an entry there.
        hint_term: Option<Term>,
    },
    /// Leader liveness signal. Carries no entries; `read_seq` lets the leader confirm
    /// it is still leader for ReadIndex requests.
    Heartbeat {
        /// `min(leader commit, follower match index)` so followers never commit
        /// entries they have not been shown to hold.
        commit: LogIndex,
        read_seq: u64,
    },
    HeartbeatResponse {
        read_seq: u64,
    },
    /// InstallSnapshot RPC. The whole snapshot is sent in one message.
    InstallSnapshot {
        snapshot: Snapshot,
    },
    InstallSnapshotResponse {
        /// The follower's log is known to match the leader's up to this index.
        last_index: LogIndex,
    },
}

impl MessageKind {
    /// Short name for logging and statistics.
    pub fn name(&self) -> &'static str {
        match self {
            MessageKind::VoteRequest { pre_vote: true, .. } => "PreVoteRequest",
            MessageKind::VoteRequest { .. } => "VoteRequest",
            MessageKind::VoteResponse { pre_vote: true, .. } => "PreVoteResponse",
            MessageKind::VoteResponse { .. } => "VoteResponse",
            MessageKind::AppendEntries { .. } => "AppendEntries",
            MessageKind::AppendResponse { .. } => "AppendResponse",
            MessageKind::Heartbeat { .. } => "Heartbeat",
            MessageKind::HeartbeatResponse { .. } => "HeartbeatResponse",
            MessageKind::InstallSnapshot { .. } => "InstallSnapshot",
            MessageKind::InstallSnapshotResponse { .. } => "InstallSnapshotResponse",
        }
    }
}
