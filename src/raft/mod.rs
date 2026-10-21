//! A from-scratch implementation of the Raft consensus algorithm.
//!
//! The module is deliberately free of I/O: [`RaftNode`] is a deterministic state machine
//! that consumes ticks, messages and proposals and produces [`Ready`] batches describing
//! what to persist, what to send and what to apply.

mod log;
mod message;
mod node;
mod types;

#[cfg(test)]
mod tests;

pub use log::{LogWrite, RaftLog};
pub use message::{Message, MessageKind};
pub use node::{Config, NotLeader, RaftNode, ReadState, Ready};
pub use types::{
    Entry, EntryPayload, HardState, LogIndex, NodeId, PersistentState, Role, Snapshot,
    SnapshotMeta, Term,
};
