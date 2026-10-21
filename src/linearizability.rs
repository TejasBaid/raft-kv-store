//! A linearizability checker for concurrent histories.
//!
//! This implements the Wing & Gong search with the memoization introduced by Lowe
//! ("Testing for linearizability", 2017), the same algorithm used by
//! [Porcupine](https://github.com/anishathalye/porcupine). Call and return events are
//! kept in a doubly-linked list ordered by time. The search repeatedly tries to
//! linearize the earliest pending call; on reaching a return event whose call has not
//! yet been linearized it backtracks. A cache of `(linearized set, model state)` pairs
//! prunes states already proven fruitless.
//!
//! Operations that never returned (the client gave up, or the run ended) are given an
//! infinite return time and an unknown output: they may take effect at any point after
//! their call, or not at all.
//!
//! Key-value histories are checked per key, which is sound because linearizability is
//! a *local* property (Herlihy & Wing, 1990): a history is linearizable iff each
//! per-object sub-history is.

use std::collections::{BTreeMap, HashSet};
use std::hash::Hash;

use crate::kv::KvResult;

/// A sequential specification of an object.
pub trait Model {
    type State: Clone + Eq + Hash;
    type Input;
    type Output;

    fn init(&self) -> Self::State;

    /// If applying `input` in `state` can produce `output`, returns the resulting state.
    /// `output == None` means the output was never observed (any output is acceptable).
    fn step(
        &self,
        state: &Self::State,
        input: &Self::Input,
        output: Option<&Self::Output>,
    ) -> Option<Self::State>;
}

/// One operation in a history. Times only need to be comparable: a strictly
/// increasing event counter works better than wall time because it has no ties.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Operation<I, O> {
    pub client: u64,
    pub input: I,
    /// `None` if the operation never completed (or its result is unknown).
    pub output: Option<O>,
    pub call: u64,
    /// `None` if the operation never completed.
    pub ret: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CheckResult {
    Linearizable,
    /// No valid linearization exists. `op` is the index of an operation whose return
    /// could not be justified (useful as a starting point when debugging).
    Violation {
        op: usize,
    },
    /// The search exceeded its step budget without a verdict.
    Unknown,
}

const NIL: usize = usize::MAX;

#[derive(Clone, Copy)]
struct Node {
    op: usize,
    is_call: bool,
    /// For a call node: the index of its return node.
    partner: usize,
    prev: usize,
    next: usize,
}

struct EventList {
    nodes: Vec<Node>, // nodes[0] is the head sentinel
}

impl EventList {
    fn new(times: &[(u64, u64)]) -> Self {
        // (time, is_return, op): calls sort before returns at equal times, which only
        // makes the checker more permissive (it can never produce a false violation).
        let mut events: Vec<(u64, bool, usize)> = Vec::with_capacity(times.len() * 2);
        for (op, &(call, ret)) in times.iter().enumerate() {
            events.push((call, false, op));
            events.push((ret, true, op));
        }
        events.sort_unstable();
        let mut nodes = Vec::with_capacity(events.len() + 1);
        nodes.push(Node {
            op: NIL,
            is_call: false,
            partner: NIL,
            prev: NIL,
            next: NIL,
        });
        let mut ret_node = vec![NIL; times.len()];
        for (i, &(_, is_ret, op)) in events.iter().enumerate() {
            let idx = i + 1;
            nodes.push(Node {
                op,
                is_call: !is_ret,
                partner: NIL,
                prev: idx - 1,
                next: if idx == events.len() { NIL } else { idx + 1 },
            });
            if is_ret {
                ret_node[op] = idx;
            }
        }
        nodes[0].next = if events.is_empty() { NIL } else { 1 };
        for node in nodes.iter_mut().skip(1) {
            if node.is_call {
                node.partner = ret_node[node.op];
            }
        }
        Self { nodes }
    }

    fn unlink(&mut self, i: usize) {
        let Node { prev, next, .. } = self.nodes[i];
        self.nodes[prev].next = next;
        if next != NIL {
            self.nodes[next].prev = prev;
        }
    }

    fn relink(&mut self, i: usize) {
        let Node { prev, next, .. } = self.nodes[i];
        self.nodes[prev].next = i;
        if next != NIL {
            self.nodes[next].prev = i;
        }
    }

    /// Removes a call and its return from the list.
    fn lift(&mut self, call: usize) {
        self.unlink(call);
        self.unlink(self.nodes[call].partner);
    }

    /// Undoes [`lift`](Self::lift). Must be applied in reverse order of lifting.
    fn unlift(&mut self, call: usize) {
        self.relink(self.nodes[call].partner);
        self.relink(call);
    }
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct BitSet(Vec<u64>);

impl BitSet {
    fn new(n: usize) -> Self {
        Self(vec![0; n.div_ceil(64)])
    }
    fn set(&mut self, i: usize) {
        self.0[i / 64] |= 1 << (i % 64);
    }
    fn clear(&mut self, i: usize) {
        self.0[i / 64] &= !(1 << (i % 64));
    }
}

/// Checks whether `history` is linearizable with respect to `model`, giving up after
/// `budget` search steps.
pub fn check<M: Model>(
    model: &M,
    history: &[Operation<M::Input, M::Output>],
    budget: u64,
) -> CheckResult {
    let times: Vec<(u64, u64)> = history
        .iter()
        .map(|op| (op.call, op.ret.unwrap_or(u64::MAX)))
        .collect();
    let mut list = EventList::new(&times);
    let mut state = model.init();
    let mut linearized = BitSet::new(history.len());
    let mut cache: HashSet<(BitSet, M::State)> = HashSet::new();
    let mut stack: Vec<(usize, M::State)> = Vec::new();
    let mut entry = list.nodes[0].next;
    let mut steps = 0u64;

    while list.nodes[0].next != NIL {
        steps += 1;
        if steps > budget {
            return CheckResult::Unknown;
        }
        let node = list.nodes[entry];
        if node.is_call {
            let op = &history[node.op];
            if let Some(next_state) = model.step(&state, &op.input, op.output.as_ref()) {
                linearized.set(node.op);
                if cache.insert((linearized.clone(), next_state.clone())) {
                    stack.push((entry, std::mem::replace(&mut state, next_state)));
                    list.lift(entry);
                    entry = list.nodes[0].next;
                    continue;
                }
                linearized.clear(node.op);
            }
            entry = node.next;
        } else {
            // Reached the return of an operation we could not linearize: backtrack.
            let Some((call, prev_state)) = stack.pop() else {
                return CheckResult::Violation { op: node.op };
            };
            linearized.clear(list.nodes[call].op);
            state = prev_state;
            list.unlift(call);
            entry = list.nodes[call].next;
        }
    }
    CheckResult::Linearizable
}

/// An operation on a single key of the KV store.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KvInput {
    Get,
    Put(Vec<u8>),
    Delete,
    Cas {
        expected: Option<Vec<u8>>,
        new: Option<Vec<u8>>,
    },
}

/// Sequential specification of one register in the KV store.
pub struct KvModel;

impl Model for KvModel {
    type State = Option<Vec<u8>>;
    type Input = KvInput;
    type Output = KvResult;

    fn init(&self) -> Self::State {
        None
    }

    fn step(
        &self,
        state: &Self::State,
        input: &KvInput,
        output: Option<&KvResult>,
    ) -> Option<Self::State> {
        match input {
            KvInput::Get => match output {
                None => Some(state.clone()),
                Some(KvResult::Value(v)) if v == state => Some(state.clone()),
                _ => None,
            },
            KvInput::Put(v) => match output {
                None | Some(KvResult::Written) => Some(Some(v.clone())),
                _ => None,
            },
            KvInput::Delete => match output {
                None => Some(None),
                Some(KvResult::Deleted(existed)) if *existed == state.is_some() => Some(None),
                _ => None,
            },
            KvInput::Cas { expected, new } => {
                let swapped = state == expected;
                match output {
                    None => {}
                    Some(KvResult::Cas {
                        swapped: s,
                        previous,
                    }) if *s == swapped && previous == state => {}
                    _ => return None,
                }
                Some(if swapped { new.clone() } else { state.clone() })
            }
        }
    }
}

/// A KV operation tagged with its key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KvOperation {
    pub key: String,
    pub op: Operation<KvInput, KvResult>,
}

/// Outcome of checking a multi-key history.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KvCheck {
    Linearizable,
    Violation {
        key: String,
        history: Vec<KvOperation>,
    },
    Unknown {
        key: String,
    },
}

/// Checks a KV history key by key.
pub fn check_kv(history: &[KvOperation], budget_per_key: u64) -> KvCheck {
    let mut by_key: BTreeMap<&str, Vec<&KvOperation>> = BTreeMap::new();
    for op in history {
        // Reads that never returned constrain nothing.
        if op.op.output.is_none() && op.op.input == KvInput::Get {
            continue;
        }
        by_key.entry(&op.key).or_default().push(op);
    }
    for (key, ops) in by_key {
        let sub: Vec<Operation<KvInput, KvResult>> = ops.iter().map(|o| o.op.clone()).collect();
        match check(&KvModel, &sub, budget_per_key) {
            CheckResult::Linearizable => {}
            CheckResult::Violation { .. } => {
                return KvCheck::Violation {
                    key: key.to_string(),
                    history: ops.into_iter().cloned().collect(),
                };
            }
            CheckResult::Unknown => {
                return KvCheck::Unknown {
                    key: key.to_string(),
                };
            }
        }
    }
    KvCheck::Linearizable
}

#[cfg(test)]
mod tests {
    use super::*;

    fn op(
        client: u64,
        input: KvInput,
        output: Option<KvResult>,
        call: u64,
        ret: Option<u64>,
    ) -> Operation<KvInput, KvResult> {
        Operation {
            client,
            input,
            output,
            call,
            ret,
        }
    }

    fn val(s: &str) -> Option<Vec<u8>> {
        Some(s.as_bytes().to_vec())
    }

    fn get(v: Option<&str>) -> Option<KvResult> {
        Some(KvResult::Value(v.map(|s| s.as_bytes().to_vec())))
    }

    const BUDGET: u64 = 1_000_000;

    #[test]
    fn empty_history_is_linearizable() {
        assert_eq!(check(&KvModel, &[], BUDGET), CheckResult::Linearizable);
    }

    #[test]
    fn sequential_history() {
        let h = vec![
            op(
                1,
                KvInput::Put(b"a".to_vec()),
                Some(KvResult::Written),
                0,
                Some(1),
            ),
            op(2, KvInput::Get, get(Some("a")), 2, Some(3)),
            op(
                1,
                KvInput::Delete,
                Some(KvResult::Deleted(true)),
                4,
                Some(5),
            ),
            op(2, KvInput::Get, get(None), 6, Some(7)),
        ];
        assert_eq!(check(&KvModel, &h, BUDGET), CheckResult::Linearizable);
    }

    #[test]
    fn concurrent_reads_may_see_either_value() {
        // put(a) overlaps both reads; one sees the old value, one the new.
        let h = vec![
            op(
                1,
                KvInput::Put(b"a".to_vec()),
                Some(KvResult::Written),
                0,
                Some(10),
            ),
            op(2, KvInput::Get, get(None), 1, Some(2)),
            op(3, KvInput::Get, get(Some("a")), 3, Some(4)),
        ];
        assert_eq!(check(&KvModel, &h, BUDGET), CheckResult::Linearizable);
    }

    #[test]
    fn stale_read_after_completed_write_is_a_violation() {
        let h = vec![
            op(
                1,
                KvInput::Put(b"a".to_vec()),
                Some(KvResult::Written),
                0,
                Some(1),
            ),
            op(2, KvInput::Get, get(None), 2, Some(3)),
        ];
        assert!(matches!(
            check(&KvModel, &h, BUDGET),
            CheckResult::Violation { .. }
        ));
    }

    #[test]
    fn reads_cannot_go_back_in_time() {
        // The pending put may have happened, but once a read observed it, a later read
        // cannot observe the old value again.
        let h = vec![
            op(1, KvInput::Put(b"a".to_vec()), None, 0, None),
            op(2, KvInput::Get, get(Some("a")), 1, Some(2)),
            op(3, KvInput::Get, get(None), 3, Some(4)),
        ];
        assert!(matches!(
            check(&KvModel, &h, BUDGET),
            CheckResult::Violation { .. }
        ));
    }

    #[test]
    fn pending_write_may_never_take_effect() {
        let h = vec![
            op(1, KvInput::Put(b"a".to_vec()), None, 0, None),
            op(2, KvInput::Get, get(None), 1, Some(2)),
            op(2, KvInput::Get, get(None), 3, Some(4)),
        ];
        assert_eq!(check(&KvModel, &h, BUDGET), CheckResult::Linearizable);
    }

    #[test]
    fn cas_must_observe_the_linearized_state() {
        let ok = vec![
            op(
                1,
                KvInput::Put(b"1".to_vec()),
                Some(KvResult::Written),
                0,
                Some(1),
            ),
            op(
                2,
                KvInput::Cas {
                    expected: val("1"),
                    new: val("2"),
                },
                Some(KvResult::Cas {
                    swapped: true,
                    previous: val("1"),
                }),
                2,
                Some(3),
            ),
            op(3, KvInput::Get, get(Some("2")), 4, Some(5)),
        ];
        assert_eq!(check(&KvModel, &ok, BUDGET), CheckResult::Linearizable);

        // Two concurrent CASes from the same expected value cannot both succeed.
        let bad = vec![
            op(
                1,
                KvInput::Put(b"1".to_vec()),
                Some(KvResult::Written),
                0,
                Some(1),
            ),
            op(
                2,
                KvInput::Cas {
                    expected: val("1"),
                    new: val("2"),
                },
                Some(KvResult::Cas {
                    swapped: true,
                    previous: val("1"),
                }),
                2,
                Some(5),
            ),
            op(
                3,
                KvInput::Cas {
                    expected: val("1"),
                    new: val("3"),
                },
                Some(KvResult::Cas {
                    swapped: true,
                    previous: val("1"),
                }),
                3,
                Some(6),
            ),
        ];
        assert!(matches!(
            check(&KvModel, &bad, BUDGET),
            CheckResult::Violation { .. }
        ));
    }

    #[test]
    fn per_key_check_isolates_keys() {
        let mk = |key: &str, o| KvOperation {
            key: key.into(),
            op: o,
        };
        let h = vec![
            mk(
                "x",
                op(
                    1,
                    KvInput::Put(b"1".to_vec()),
                    Some(KvResult::Written),
                    0,
                    Some(1),
                ),
            ),
            mk("y", op(2, KvInput::Get, get(None), 2, Some(3))),
            mk("x", op(2, KvInput::Get, get(Some("1")), 4, Some(5))),
        ];
        assert_eq!(check_kv(&h, BUDGET), KvCheck::Linearizable);
        let mut bad = h.clone();
        bad.push(mk("x", op(3, KvInput::Get, get(None), 6, Some(7))));
        assert!(matches!(check_kv(&bad, BUDGET), KvCheck::Violation { key, .. } if key == "x"));
    }

    #[test]
    fn many_concurrent_writers_finish_quickly() {
        // 6 clients x 40 rounds of fully overlapping writes and reads.
        let mut h = Vec::new();
        let mut t = 0;
        for round in 0..40u64 {
            let base = t;
            for c in 0..6u64 {
                let v = format!("{round}-{c}");
                h.push(op(
                    c,
                    KvInput::Put(v.into_bytes()),
                    Some(KvResult::Written),
                    base + c,
                    Some(base + 20 + c),
                ));
            }
            let last = format!("{round}-5");
            h.push(op(
                9,
                KvInput::Get,
                get(Some(&last)),
                base + 30,
                Some(base + 31),
            ));
            t = base + 40;
        }
        assert_eq!(check(&KvModel, &h, BUDGET), CheckResult::Linearizable);
    }
}
