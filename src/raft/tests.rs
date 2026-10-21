//! Scenario tests for the Raft core, driven by a tiny synchronous in-memory network.
//! Randomized fault-injection testing lives in [`crate::sim`].

use std::collections::{BTreeMap, BTreeSet};

use super::*;

/// A lossless, synchronous test network with node isolation.
struct Net {
    nodes: BTreeMap<NodeId, RaftNode>,
    disks: BTreeMap<NodeId, PersistentState>,
    applied: BTreeMap<NodeId, Vec<Entry>>,
    reads: BTreeMap<NodeId, Vec<ReadState>>,
    isolated: BTreeSet<NodeId>,
}

const ELECTION_TIMEOUT_MAX: u32 = 20;

fn config(id: NodeId, n: u64) -> Config {
    let mut cfg = Config::new(id, (1..=n).collect());
    cfg.seed = id * 7919;
    cfg
}

fn persist(disk: &mut PersistentState, rd: &Ready) {
    if let Some(hs) = rd.hard_state {
        disk.hard_state = hs;
    }
    if let Some(snap) = &rd.snapshot {
        disk.snapshot = snap.clone();
        disk.entries.retain(|e| e.index > snap.meta.last_index);
    }
    if let Some(w) = &rd.log_write {
        disk.entries.retain(|e| e.index < w.truncate_from);
        disk.entries.extend(w.entries.iter().cloned());
    }
}

impl Net {
    fn new(n: u64) -> Self {
        let mut net = Net {
            nodes: BTreeMap::new(),
            disks: BTreeMap::new(),
            applied: BTreeMap::new(),
            reads: BTreeMap::new(),
            isolated: BTreeSet::new(),
        };
        for id in 1..=n {
            net.nodes
                .insert(id, RaftNode::new(config(id, n), PersistentState::default()));
            net.disks.insert(id, PersistentState::default());
            net.applied.insert(id, Vec::new());
            net.reads.insert(id, Vec::new());
        }
        net
    }

    fn node(&self, id: NodeId) -> &RaftNode {
        &self.nodes[&id]
    }

    fn node_mut(&mut self, id: NodeId) -> &mut RaftNode {
        self.nodes.get_mut(&id).unwrap()
    }

    /// Processes `Ready`s and delivers messages until the network is quiet.
    fn settle(&mut self) {
        for _ in 0..10_000 {
            let mut msgs = Vec::new();
            for (id, node) in self.nodes.iter_mut() {
                let rd = node.ready();
                persist(self.disks.get_mut(id).unwrap(), &rd);
                node.advance();
                msgs.extend(rd.pre_persist_messages.iter().cloned());
                self.applied
                    .get_mut(id)
                    .unwrap()
                    .extend(rd.committed.iter().cloned());
                self.reads.get_mut(id).unwrap().extend(rd.read_states);
                msgs.extend(rd.messages);
            }
            if msgs.is_empty() && !self.nodes.values().any(|n| n.has_ready()) {
                return;
            }
            for m in msgs {
                if self.isolated.contains(&m.from) || self.isolated.contains(&m.to) {
                    continue;
                }
                if let Some(n) = self.nodes.get_mut(&m.to) {
                    n.step(m);
                }
            }
        }
        panic!("network did not settle");
    }

    /// Ticks only `id` until it starts an election, then lets the network settle.
    fn campaign(&mut self, id: NodeId) {
        // `Config::new` draws election timeouts from [10, 20) ticks.
        for _ in 0..ELECTION_TIMEOUT_MAX {
            self.node_mut(id).tick();
        }
        self.settle();
    }

    fn tick_all(&mut self, n: usize) {
        for _ in 0..n {
            for node in self.nodes.values_mut() {
                node.tick();
            }
            self.settle();
        }
    }

    fn leaders(&self) -> Vec<NodeId> {
        self.nodes
            .values()
            .filter(|n| n.is_leader())
            .map(|n| n.id())
            .collect()
    }

    fn propose(&mut self, id: NodeId, data: &[u8]) -> LogIndex {
        let (idx, _) = self.node_mut(id).propose(data.to_vec()).unwrap();
        self.settle();
        idx
    }

    fn restart(&mut self, id: NodeId) {
        let n = self.nodes.len() as u64;
        let disk = self.disks[&id].clone();
        self.nodes.insert(id, RaftNode::new(config(id, n), disk));
        self.applied.insert(id, Vec::new());
    }

    fn commands(&self, id: NodeId) -> Vec<Vec<u8>> {
        self.applied[&id]
            .iter()
            .filter_map(|e| match &e.payload {
                EntryPayload::Command(c) => Some(c.clone()),
                EntryPayload::Noop => None,
            })
            .collect()
    }
}

#[test]
fn single_node_elects_itself_and_commits() {
    let mut net = Net::new(1);
    net.campaign(1);
    assert!(net.node(1).is_leader());
    assert_eq!(net.node(1).term(), 1);
    let idx = net.propose(1, b"x");
    assert_eq!(net.node(1).commit_index(), idx);
    assert_eq!(net.commands(1), vec![b"x".to_vec()]);
}

#[test]
fn three_nodes_elect_exactly_one_leader() {
    let mut net = Net::new(3);
    net.campaign(2);
    assert_eq!(net.leaders(), vec![2]);
    for id in [1, 3] {
        assert_eq!(net.node(id).leader(), Some(2));
        assert_eq!(net.node(id).term(), 1);
    }
}

#[test]
fn elections_happen_without_intervention() {
    let mut net = Net::new(5);
    net.tick_all(60);
    assert_eq!(net.leaders().len(), 1);
}

#[test]
fn replicates_and_commits_on_all_nodes() {
    let mut net = Net::new(3);
    net.campaign(1);
    for i in 0..10u8 {
        net.propose(1, &[i]);
    }
    net.tick_all(3); // heartbeats carry the final commit index to followers
    let expected: Vec<Vec<u8>> = (0..10u8).map(|i| vec![i]).collect();
    for id in 1..=3 {
        assert_eq!(net.commands(id), expected, "node {id}");
    }
}

#[test]
fn leader_commits_with_minority_down_but_not_majority() {
    let mut net = Net::new(5);
    net.campaign(1);
    net.isolated.extend([4, 5]);
    let idx = net.propose(1, b"ok");
    assert_eq!(net.node(1).commit_index(), idx);

    net.isolated.insert(3);
    let idx2 = net.propose(1, b"stuck");
    assert!(net.node(1).commit_index() < idx2);
}

#[test]
fn vote_denied_to_candidate_with_stale_log() {
    let mut net = Net::new(3);
    net.campaign(1);
    net.isolated.insert(3);
    net.propose(1, b"a");
    net.isolated.clear();

    // Node 3 missed an entry; it must not win an election even if it times out first.
    let mut cfg = config(3, 3);
    cfg.pre_vote = false;
    let disk = net.disks[&3].clone();
    net.nodes.insert(3, RaftNode::new(cfg, disk));
    // Make 1 and 2 forget the leader so they would otherwise vote.
    for id in [1, 2] {
        let mut cfg = config(id, 3);
        cfg.pre_vote = false;
        let disk = net.disks[&id].clone();
        net.nodes.insert(id, RaftNode::new(cfg, disk));
    }
    net.campaign(3);
    assert!(!net.node(3).is_leader());
}

#[test]
fn stale_leader_steps_down_when_it_sees_a_higher_term() {
    let mut net = Net::new(3);
    net.campaign(1);
    net.isolated.insert(1);
    net.tick_all(40); // 2 and 3 elect a new leader among themselves
    let new_leader = *net.leaders().iter().find(|&&l| l != 1).unwrap();
    assert!(net.node(new_leader).term() > 1);
    net.isolated.clear();
    net.tick_all(3);
    assert_eq!(net.leaders(), vec![new_leader]);
    assert_eq!(net.node(1).leader(), Some(new_leader));
}

#[test]
fn conflicting_uncommitted_entries_are_overwritten() {
    let mut net = Net::new(3);
    net.campaign(1);
    net.propose(1, b"committed");

    // Leader 1 appends entries that never reach anyone.
    net.isolated.insert(1);
    let _ = net.node_mut(1).propose(b"lost-1".to_vec());
    let _ = net.node_mut(1).propose(b"lost-2".to_vec());
    net.settle();

    // 2 or 3 becomes leader and commits different entries at the same indices.
    while !net.leaders().iter().any(|&l| l != 1) {
        net.tick_all(1);
    }
    let new_leader = *net.leaders().iter().find(|&&l| l != 1).unwrap();
    net.propose(new_leader, b"winner");

    net.isolated.clear();
    net.tick_all(5);
    for id in 1..=3 {
        assert_eq!(
            net.commands(id),
            vec![b"committed".to_vec(), b"winner".to_vec()],
            "node {id}"
        );
    }
}

/// Raft paper Figure 8: a leader must not commit an entry from a previous term just
/// because it is stored on a majority; only entries from its own term commit by counting.
#[test]
fn does_not_commit_previous_term_entries_by_counting_replicas() {
    let entry = |index, term| Entry {
        index,
        term,
        payload: EntryPayload::Command(vec![index as u8]),
    };
    let mut cfg = config(1, 3);
    cfg.pre_vote = false;
    let state = PersistentState {
        hard_state: HardState {
            term: 2,
            voted_for: None,
        },
        snapshot: Snapshot::default(),
        entries: vec![entry(1, 1), entry(2, 2)],
    };
    let mut leader = RaftNode::new(cfg, state);
    for _ in 0..20 {
        leader.tick();
    }
    assert_eq!(leader.role(), Role::Candidate);
    assert_eq!(leader.term(), 3);
    leader.step(Message {
        from: 2,
        to: 1,
        term: 3,
        kind: MessageKind::VoteResponse {
            pre_vote: false,
            granted: true,
        },
    });
    assert!(leader.is_leader());
    assert_eq!(leader.log().last_index(), 3); // election no-op at index 3, term 3

    let ack = |match_index| Message {
        from: 2,
        to: 1,
        term: 3,
        kind: MessageKind::AppendResponse {
            success: true,
            match_index,
            rejected: 0,
            hint_index: 0,
            hint_term: None,
        },
    };
    // Persist the leader's log (including the no-op) so it counts itself.
    let _ = leader.ready();
    leader.advance();
    // Entry 2 (term 2) is now on a majority (nodes 1 and 2) but must not be committed.
    leader.step(ack(2));
    assert_eq!(leader.commit_index(), 0);
    // Once the term-3 no-op is on a majority, it and everything before it commit.
    leader.step(ack(3));
    assert_eq!(leader.commit_index(), 3);
}

#[test]
fn lagging_follower_catches_up_through_snapshot() {
    let mut net = Net::new(3);
    net.campaign(1);
    net.isolated.insert(3);
    for i in 0..20u8 {
        net.propose(1, &[i]);
    }
    // Compact the leader's log past what node 3 has.
    let applied = net.node(1).last_applied();
    net.node_mut(1).compact(applied, b"SNAPSHOT".to_vec());
    assert!(net.node(1).log().first_index() > 2);

    // Within two heartbeat intervals the leader notices node 3 is stalled, finds the
    // entries it needs were compacted, and sends the snapshot instead.
    net.isolated.clear();
    net.tick_all(6);
    assert_eq!(net.node(3).log().snapshot_index(), applied);
    assert_eq!(net.disks[&3].snapshot.data, b"SNAPSHOT".to_vec());

    // And it keeps replicating normally afterwards.
    net.propose(1, b"after");
    net.tick_all(2);
    assert_eq!(net.commands(3), vec![b"after".to_vec()]);
}

#[test]
fn read_index_needs_a_quorum_of_heartbeat_acks() {
    let mut net = Net::new(3);
    net.campaign(1);
    net.propose(1, b"v");

    net.node_mut(1).read_index(42).unwrap();
    net.settle();
    let read = net.reads[&1].last().copied().unwrap();
    assert_eq!(read.ctx, 42);
    assert_eq!(read.index, net.node(1).commit_index());

    // An isolated leader cannot confirm its leadership, so the read never completes.
    net.isolated.insert(1);
    net.node_mut(1).read_index(43).unwrap();
    net.settle();
    assert!(net.reads[&1].iter().all(|r| r.ctx != 43));
}

#[test]
fn read_index_waits_for_leader_to_commit_in_its_term() {
    let mut net = Net::new(3);
    net.campaign(1);
    // Hold back all messages so the election no-op cannot commit yet.
    net.isolated.extend([2, 3]);
    let leader = net.node_mut(1);
    assert!(leader.is_leader());
    // Commit index still 0 < term-1 no-op.
    leader.read_index(7).unwrap();
    net.settle();
    assert!(net.reads[&1].is_empty());
    net.isolated.clear();
    net.tick_all(2);
    assert!(net.reads[&1].iter().any(|r| r.ctx == 7));
}

#[test]
fn non_leader_rejects_proposals_with_hint() {
    let mut net = Net::new(3);
    net.campaign(1);
    let err = net.node_mut(2).propose(b"x".to_vec()).unwrap_err();
    assert_eq!(err.leader_hint, Some(1));
    assert!(net.node_mut(2).read_index(1).is_err());
}

#[test]
fn pre_vote_prevents_partitioned_node_from_disrupting_leader() {
    let mut net = Net::new(3);
    net.campaign(1);
    let term = net.node(1).term();
    net.isolated.insert(3);
    for _ in 0..200 {
        net.node_mut(3).tick();
        net.settle();
    }
    // Pre-vote kept node 3 from inflating its term while isolated.
    assert_eq!(net.node(3).term(), term);
    net.isolated.clear();
    net.tick_all(5);
    assert_eq!(net.leaders(), vec![1]);
    assert_eq!(net.node(1).term(), term);
}

#[test]
fn check_quorum_makes_isolated_leader_step_down() {
    let mut net = Net::new(3);
    net.campaign(1);
    net.isolated.insert(1);
    // The quorum check runs once per maximum election timeout (20 ticks); the first
    // check after isolation sees no responses and steps down.
    for _ in 0..41 {
        net.node_mut(1).tick();
        net.settle();
    }
    assert!(!net.node(1).is_leader());
}

#[test]
fn restart_recovers_term_vote_and_log() {
    let mut net = Net::new(3);
    net.campaign(1);
    for i in 0..5u8 {
        net.propose(1, &[i]);
    }
    let before = (
        net.node(2).term(),
        net.node(2).hard_state(),
        net.node(2).log().last_index(),
    );
    net.restart(2);
    let after = (
        net.node(2).term(),
        net.node(2).hard_state(),
        net.node(2).log().last_index(),
    );
    assert_eq!(before, after);
    // Committed entries are re-applied after the leader re-establishes the commit index.
    net.tick_all(3);
    assert_eq!(net.commands(2).len(), 5);
}

#[test]
fn duplicated_and_reordered_messages_are_harmless() {
    let mut net = Net::new(3);
    net.campaign(1);
    for i in 0..3u8 {
        net.propose(1, &[i]);
    }
    // Replay an old AppendEntries carrying entry 2 after entries 3..=4 were accepted.
    let entry = net.node(1).log().entry(2).unwrap().clone();
    let old = Message {
        from: 1,
        to: 2,
        term: net.node(1).term(),
        kind: MessageKind::AppendEntries {
            prev_log_index: 1,
            prev_log_term: 1,
            entries: vec![entry],
            leader_commit: 0,
        },
    };
    let last = net.node(2).log().last_index();
    net.node_mut(2).step(old.clone());
    net.node_mut(2).step(old);
    net.settle();
    assert_eq!(net.node(2).log().last_index(), last);
}

/// Raft Figure 2, AppendEntries step 5: `commitIndex = min(leaderCommit, index of last
/// new entry)`. Bounding by the follower's *own* last index would commit a stale entry
/// that the leader has not yet shown to match. (The simulator cannot reach this case,
/// because our leader always resumes at the first conflicting index, so this unit test
/// is what kills the corresponding mutant in `scripts/mutation_test.py`.)
#[test]
fn follower_commit_is_bounded_by_last_new_entry() {
    let entry = |index, term| Entry {
        index,
        term,
        payload: EntryPayload::Noop,
    };
    let state = PersistentState {
        hard_state: HardState {
            term: 2,
            voted_for: None,
        },
        snapshot: Snapshot::default(),
        // Entry 3 came from a term-2 leader and was never committed.
        entries: vec![entry(1, 1), entry(2, 1), entry(3, 2)],
    };
    let mut follower = RaftNode::new(config(2, 3), state);
    // The term-3 leader has committed a *different* entry 3, but this message only
    // carries entry 2.
    follower.step(Message {
        from: 1,
        to: 2,
        term: 3,
        kind: MessageKind::AppendEntries {
            prev_log_index: 1,
            prev_log_term: 1,
            entries: vec![entry(2, 1)],
            leader_commit: 3,
        },
    });
    assert_eq!(follower.commit_index(), 2);
}
