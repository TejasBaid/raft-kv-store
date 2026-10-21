//! Core Raft data types shared by the consensus module, storage and transport.

use serde::{Deserialize, Serialize};

/// Identifier of a cluster member. Must be non-zero and unique.
pub type NodeId = u64;
/// A Raft term (logical election epoch).
pub type Term = u64;
/// Position of an entry in the replicated log. Index 0 is a sentinel; real entries start at 1.
pub type LogIndex = u64;

/// What a log entry carries.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum EntryPayload {
    /// Appended by every new leader so it can commit an entry from its own term
    /// (Raft §5.4.2 / §8). Also unblocks ReadIndex after an election.
    Noop,
    /// An opaque, application-defined command.
    Command(Vec<u8>),
}

/// A single replicated log entry.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entry {
    pub index: LogIndex,
    pub term: Term,
    pub payload: EntryPayload,
}

/// State that must be durable before a node responds to any RPC (Raft Figure 2).
/// The log itself is persisted separately via [`LogWrite`](crate::raft::LogWrite).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HardState {
    pub term: Term,
    pub voted_for: Option<NodeId>,
}

/// Identifies the last log entry covered by a snapshot.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotMeta {
    pub last_index: LogIndex,
    pub last_term: Term,
}

/// A point-in-time image of the application state machine, replacing the log prefix
/// up to and including `meta.last_index`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Snapshot {
    pub meta: SnapshotMeta,
    pub data: Vec<u8>,
}

/// Everything a node needs from stable storage to restart.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PersistentState {
    pub hard_state: HardState,
    pub snapshot: Snapshot,
    /// Entries following the snapshot, contiguous and in index order.
    pub entries: Vec<Entry>,
}

/// The role a node currently plays.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Role {
    Follower,
    /// Running a pre-vote round (Raft thesis §9.6); has not incremented its term.
    PreCandidate,
    Candidate,
    Leader,
}

impl std::fmt::Display for Role {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Role::Follower => "follower",
            Role::PreCandidate => "pre-candidate",
            Role::Candidate => "candidate",
            Role::Leader => "leader",
        };
        f.write_str(s)
    }
}
