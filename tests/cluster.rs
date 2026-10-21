//! End-to-end tests against real multi-node clusters over TCP.
//! Every listener binds to port 0 so tests can run in parallel with anything else.

use std::time::Duration;

use raft_kv::client::Client;
use raft_kv::cluster::{ClusterOptions, LocalCluster};
use raft_kv::raft::NodeId;

const LEADER_TIMEOUT: Duration = Duration::from_secs(10);

async fn cluster(nodes: usize, dir: &tempfile::TempDir, fsync: bool) -> LocalCluster {
    let mut opts = ClusterOptions::new(nodes, dir.path().to_path_buf());
    opts.fsync = fsync;
    opts.wal_prealloc = 1 << 20;
    let c = LocalCluster::start(opts).await.expect("cluster starts");
    c.wait_for_leader(LEADER_TIMEOUT)
        .await
        .expect("leader elected");
    c
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn basic_operations_and_follower_redirect() {
    let dir = tempfile::tempdir().unwrap();
    let c = cluster(3, &dir, false).await;
    let mut client = c.client();

    client.put("a", "1").await.unwrap();
    assert_eq!(client.get("a").await.unwrap(), Some(b"1".to_vec()));
    assert_eq!(client.get("missing").await.unwrap(), None);

    let (swapped, prev) = client
        .cas("a", Some(b"1".to_vec()), Some(b"2".to_vec()))
        .await
        .unwrap();
    assert!(swapped);
    assert_eq!(prev, Some(b"1".to_vec()));
    let (swapped, prev) = client
        .cas("a", Some(b"1".to_vec()), Some(b"3".to_vec()))
        .await
        .unwrap();
    assert!(!swapped);
    assert_eq!(prev, Some(b"2".to_vec()));

    assert!(client.delete("a").await.unwrap());
    assert!(!client.delete("a").await.unwrap());

    // A client that only knows a follower is redirected to the leader.
    let leader = c.leader().await.unwrap();
    let follower = c.addrs().keys().copied().find(|&id| id != leader).unwrap();
    let mut via_follower = Client::new(vec![c.addrs()[&follower].to_string()]);
    via_follower.put("b", "x").await.unwrap();
    assert_eq!(via_follower.get("b").await.unwrap(), Some(b"x".to_vec()));

    c.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn survives_leader_failure() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = cluster(3, &dir, false).await;
    let mut client = c.client();
    for i in 0..20 {
        client.put(&format!("k{i}"), format!("v{i}")).await.unwrap();
    }

    let old = c.leader().await.unwrap();
    c.stop(old).await;
    let new = c.wait_for_leader(LEADER_TIMEOUT).await.unwrap();
    assert_ne!(old, new);

    for i in 0..20 {
        let v = client.get(&format!("k{i}")).await.unwrap();
        assert_eq!(v, Some(format!("v{i}").into_bytes()));
    }
    client.put("after", "failover").await.unwrap();

    // The old leader rejoins as a follower and catches up.
    c.restart(old).await.unwrap();
    wait_until_caught_up(&c, old).await;
    c.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn full_cluster_restart_recovers_from_disk() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = cluster(3, &dir, true).await;
    let mut client = c.client();
    for i in 0..10 {
        client.put(&format!("k{i}"), format!("v{i}")).await.unwrap();
    }
    for id in 1..=3 {
        c.stop(id).await;
    }
    for id in 1..=3 {
        c.restart(id).await.unwrap();
    }
    c.wait_for_leader(LEADER_TIMEOUT).await.unwrap();
    let mut client = c.client();
    for i in 0..10 {
        let v = client.get(&format!("k{i}")).await.unwrap();
        assert_eq!(v, Some(format!("v{i}").into_bytes()));
    }
    c.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lagging_node_catches_up_via_install_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = ClusterOptions::new(3, dir.path().to_path_buf());
    opts.fsync = false;
    opts.snapshot_threshold = 50;
    opts.wal_prealloc = 1 << 20;
    let mut c = LocalCluster::start(opts).await.unwrap();
    let leader = c.wait_for_leader(LEADER_TIMEOUT).await.unwrap();
    let lagger: NodeId = (1..=3).find(|&id| id != leader).unwrap();
    c.stop(lagger).await;

    let mut client = c.client();
    for i in 0..300 {
        client.put(&format!("k{i}"), format!("v{i}")).await.unwrap();
    }
    let s = c.status(leader).await.unwrap();
    assert!(s.snapshot_index > 1, "leader compacted its log");

    c.restart(lagger).await.unwrap();
    wait_until_caught_up(&c, lagger).await;
    let s = c.status(lagger).await.unwrap();
    assert!(s.snapshot_index > 0, "lagging node received a snapshot");

    // Take down the third node: the lagging node is now needed for every quorum, so
    // reads succeeding proves it holds the full state.
    let third = (1..=3).find(|&id| id != leader && id != lagger).unwrap();
    c.stop(third).await;
    for i in (0..300).step_by(37) {
        let v = client.get(&format!("k{i}")).await.unwrap();
        assert_eq!(v, Some(format!("v{i}").into_bytes()));
    }
    c.shutdown().await;
}

/// Concurrent CAS increments across a leader failover. Each logical increment must be
/// applied exactly once: if a retried CAS were re-executed after its first attempt had
/// already succeeded, the client would see a spurious failure and increment again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_cas_increments_are_exactly_once_across_failover() {
    const CLIENTS: usize = 6;
    const INCREMENTS: usize = 25;
    let dir = tempfile::tempdir().unwrap();
    let mut c = cluster(3, &dir, false).await;
    let addrs = c.client_addrs();
    c.client().put("counter", "0").await.unwrap();

    let mut tasks = Vec::new();
    for _ in 0..CLIENTS {
        let addrs = addrs.clone();
        tasks.push(tokio::spawn(async move {
            let mut client = Client::new(addrs);
            for _ in 0..INCREMENTS {
                loop {
                    let cur = client.get("counter").await.unwrap().unwrap();
                    let n: u64 = String::from_utf8(cur.clone()).unwrap().parse().unwrap();
                    let next = (n + 1).to_string().into_bytes();
                    let (swapped, _) = client.cas("counter", Some(cur), Some(next)).await.unwrap();
                    if swapped {
                        break;
                    }
                }
            }
        }));
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    let leader = c.leader().await.unwrap();
    c.stop(leader).await;
    for t in tasks {
        t.await.unwrap();
    }
    let v = c.client().get("counter").await.unwrap().unwrap();
    assert_eq!(
        String::from_utf8(v).unwrap(),
        (CLIENTS * INCREMENTS).to_string()
    );
    c.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn minority_cannot_serve_requests() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = cluster(3, &dir, false).await;
    c.client().put("k", "v").await.unwrap();
    let leader = c.leader().await.unwrap();
    for id in (1..=3).filter(|&id| id != leader) {
        c.stop(id).await;
    }
    let mut client = Client::new(vec![c.addrs()[&leader].to_string()]);
    client.operation_timeout = Duration::from_millis(1500);
    client.request_timeout = Duration::from_millis(300);
    assert!(
        client.get("k").await.is_err(),
        "read must not succeed without a quorum"
    );
    assert!(
        client.put("k", "w").await.is_err(),
        "write must not commit without a quorum"
    );
    c.shutdown().await;
}

async fn wait_until_caught_up(c: &LocalCluster, id: NodeId) {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let leader = c.leader().await;
        if let (Some(l), Some(s)) = (leader, c.status(id).await) {
            if let Some(ls) = c.status(l).await {
                if s.last_applied >= ls.commit_index && s.term == ls.term {
                    return;
                }
            }
        }
        assert!(
            std::time::Instant::now() < deadline,
            "node {id} did not catch up"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
