//! # raft-kv
//!
//! A linearizable, replicated key-value store built on a from-scratch implementation of
//! the Raft consensus algorithm.
//!
//! The crate is layered so that everything that decides *what happens* is deterministic
//! and free of I/O, and everything that does I/O is a thin shell around it:
//!
//! | Layer | Module | I/O? |
//! |---|---|---|
//! | Consensus | [`raft`] | no |
//! | State machine (KV + client sessions) | [`kv`] | no |
//! | Server logic (requests ↔ Raft ↔ state machine) | [`node`] | no |
//! | Durable storage (WAL + snapshots) | [`storage`] | disk |
//! | Networked server, client, local cluster | [`server`], [`client`], [`cluster`] | TCP |
//! | Deterministic simulator + linearizability checker | [`sim`], [`linearizability`] | no |

pub mod bench;
pub mod client;
pub mod cluster;
pub mod kv;
pub mod linearizability;
pub mod node;
pub mod protocol;
pub mod raft;
pub mod server;
pub mod sim;
pub mod storage;
pub mod util;
