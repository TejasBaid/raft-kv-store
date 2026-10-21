//! The replicated key-value state machine.
//!
//! Commands are applied in log order. Each write carries a `(client_id, seq)` pair; the
//! state machine remembers the last sequence number and result per client, so a retried
//! request that was already applied returns the cached result instead of executing
//! twice. Combined with client retries this gives exactly-once semantics for writes.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// A mutating operation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Command {
    Put {
        key: String,
        value: Vec<u8>,
    },
    Delete {
        key: String,
    },
    /// Atomically replace the value of `key` with `new` if its current value equals
    /// `expected`. `None` means "absent", so CAS can also create or delete a key.
    Cas {
        key: String,
        expected: Option<Vec<u8>>,
        new: Option<Vec<u8>>,
    },
}

impl Command {
    pub fn key(&self) -> &str {
        match self {
            Command::Put { key, .. } | Command::Delete { key } | Command::Cas { key, .. } => key,
        }
    }
}

/// A write as it appears in the replicated log.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriteRequest {
    pub client_id: u64,
    /// Per-client sequence number. A client has at most one write outstanding and
    /// increments `seq` for each new write (retries reuse it).
    pub seq: u64,
    pub command: Command,
}

/// The result of a read or write.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum KvResult {
    /// Result of a get.
    Value(Option<Vec<u8>>),
    /// Result of a put.
    Written,
    /// Result of a delete: whether the key existed.
    Deleted(bool),
    /// Result of a compare-and-swap: whether it took effect, and the value before it.
    Cas {
        swapped: bool,
        previous: Option<Vec<u8>>,
    },
}

/// Why a write could not be applied.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
pub enum ApplyError {
    #[error("stale request: client {client_id} seq {seq} is older than last applied seq {last}")]
    StaleSequence { client_id: u64, seq: u64, last: u64 },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Session {
    last_seq: u64,
    last_result: KvResult,
}

/// The key-value store plus client sessions. Everything here is part of the snapshot.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct KvStateMachine {
    data: BTreeMap<String, Vec<u8>>,
    sessions: BTreeMap<u64, Session>,
}

impl KvStateMachine {
    pub fn new() -> Self {
        Self::default()
    }

    /// Applies a committed write, deduplicating retries.
    pub fn apply(&mut self, req: &WriteRequest) -> Result<KvResult, ApplyError> {
        if let Some(session) = self.sessions.get(&req.client_id) {
            if req.seq == session.last_seq {
                return Ok(session.last_result.clone());
            }
            if req.seq < session.last_seq {
                return Err(ApplyError::StaleSequence {
                    client_id: req.client_id,
                    seq: req.seq,
                    last: session.last_seq,
                });
            }
        }
        let result = self.execute(&req.command);
        self.sessions.insert(
            req.client_id,
            Session {
                last_seq: req.seq,
                last_result: result.clone(),
            },
        );
        Ok(result)
    }

    fn execute(&mut self, cmd: &Command) -> KvResult {
        match cmd {
            Command::Put { key, value } => {
                self.data.insert(key.clone(), value.clone());
                KvResult::Written
            }
            Command::Delete { key } => KvResult::Deleted(self.data.remove(key).is_some()),
            Command::Cas { key, expected, new } => {
                let previous = self.data.get(key).cloned();
                let swapped = previous == *expected;
                if swapped {
                    match new {
                        Some(v) => {
                            self.data.insert(key.clone(), v.clone());
                        }
                        None => {
                            self.data.remove(key);
                        }
                    }
                }
                KvResult::Cas { swapped, previous }
            }
        }
    }

    pub fn get(&self, key: &str) -> Option<&Vec<u8>> {
        self.data.get(key)
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Serializes the full state (data and sessions) for a Raft snapshot.
    pub fn snapshot(&self) -> Vec<u8> {
        bincode::serialize(self).expect("state machine serialization cannot fail")
    }

    /// Replaces the state with a snapshot produced by [`snapshot`](Self::snapshot).
    pub fn restore(data: &[u8]) -> Result<Self, bincode::Error> {
        if data.is_empty() {
            return Ok(Self::default());
        }
        bincode::deserialize(data)
    }

    /// Digest of the full state, used by tests to compare replicas.
    pub fn digest(&self) -> u64 {
        crate::util::fnv1a(&self.snapshot())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn put(client_id: u64, seq: u64, key: &str, value: &str) -> WriteRequest {
        WriteRequest {
            client_id,
            seq,
            command: Command::Put {
                key: key.into(),
                value: value.into(),
            },
        }
    }

    #[test]
    fn put_get_delete() {
        let mut sm = KvStateMachine::new();
        assert_eq!(sm.apply(&put(1, 1, "a", "x")).unwrap(), KvResult::Written);
        assert_eq!(sm.get("a"), Some(&b"x".to_vec()));
        let del = WriteRequest {
            client_id: 1,
            seq: 2,
            command: Command::Delete { key: "a".into() },
        };
        assert_eq!(sm.apply(&del).unwrap(), KvResult::Deleted(true));
        assert_eq!(sm.get("a"), None);
    }

    #[test]
    fn retried_write_is_applied_once() {
        let mut sm = KvStateMachine::new();
        let cas = WriteRequest {
            client_id: 9,
            seq: 1,
            command: Command::Cas {
                key: "k".into(),
                expected: None,
                new: Some(b"1".to_vec()),
            },
        };
        let first = sm.apply(&cas).unwrap();
        assert_eq!(
            first,
            KvResult::Cas {
                swapped: true,
                previous: None
            }
        );
        // The retry would fail if executed again (the key now exists), but returns the
        // original result instead.
        assert_eq!(sm.apply(&cas).unwrap(), first);
        assert_eq!(sm.get("k"), Some(&b"1".to_vec()));
    }

    #[test]
    fn stale_sequence_is_rejected() {
        let mut sm = KvStateMachine::new();
        sm.apply(&put(1, 5, "a", "x")).unwrap();
        assert!(matches!(
            sm.apply(&put(1, 4, "a", "y")),
            Err(ApplyError::StaleSequence { .. })
        ));
        assert_eq!(sm.get("a"), Some(&b"x".to_vec()));
    }

    #[test]
    fn cas_semantics() {
        let mut sm = KvStateMachine::new();
        sm.apply(&put(1, 1, "k", "a")).unwrap();
        let cas = |seq, expected: Option<&str>, new: Option<&str>| WriteRequest {
            client_id: 2,
            seq,
            command: Command::Cas {
                key: "k".into(),
                expected: expected.map(|s| s.as_bytes().to_vec()),
                new: new.map(|s| s.as_bytes().to_vec()),
            },
        };
        let r = sm.apply(&cas(1, Some("wrong"), Some("b"))).unwrap();
        assert_eq!(
            r,
            KvResult::Cas {
                swapped: false,
                previous: Some(b"a".to_vec())
            }
        );
        let r = sm.apply(&cas(2, Some("a"), None)).unwrap();
        assert!(matches!(r, KvResult::Cas { swapped: true, .. }));
        assert_eq!(sm.get("k"), None);
    }

    #[test]
    fn snapshot_round_trip_includes_sessions() {
        let mut sm = KvStateMachine::new();
        sm.apply(&put(1, 1, "a", "x")).unwrap();
        sm.apply(&put(2, 7, "b", "y")).unwrap();
        let restored = KvStateMachine::restore(&sm.snapshot()).unwrap();
        assert_eq!(restored, sm);
        // Dedup state survives: the retry is still recognized.
        let mut restored = restored;
        assert_eq!(
            restored.apply(&put(2, 7, "b", "z")).unwrap(),
            KvResult::Written
        );
        assert_eq!(restored.get("b"), Some(&b"y".to_vec()));
    }
}
