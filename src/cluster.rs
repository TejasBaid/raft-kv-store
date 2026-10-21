//! Launches a cluster of servers inside one process (for demos, tests and benchmarks).

use std::collections::BTreeMap;
use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use tokio::net::TcpListener;

use crate::client::Client;
use crate::protocol::NodeStatus;
use crate::raft::{NodeId, Role};
use crate::server::{self, ServerConfig, ServerHandle};

#[derive(Clone, Debug)]
pub struct ClusterOptions {
    pub nodes: usize,
    /// Node `i` listens on `base_port + i - 1`; `None` picks ephemeral ports.
    pub base_port: Option<u16>,
    pub data_dir: PathBuf,
    pub fsync: bool,
    pub snapshot_threshold: u64,
    pub tick: Duration,
    pub wal_prealloc: u64,
}

impl ClusterOptions {
    pub fn new(nodes: usize, data_dir: PathBuf) -> Self {
        Self {
            nodes,
            base_port: None,
            data_dir,
            fsync: true,
            snapshot_threshold: 10_000,
            tick: Duration::from_millis(10),
            wal_prealloc: crate::storage::DEFAULT_PREALLOC,
        }
    }
}

pub struct LocalCluster {
    opts: ClusterOptions,
    addrs: BTreeMap<NodeId, SocketAddr>,
    handles: BTreeMap<NodeId, ServerHandle>,
}

impl LocalCluster {
    /// Binds every listener first (so ephemeral ports are known), then starts the nodes.
    pub async fn start(opts: ClusterOptions) -> io::Result<Self> {
        let mut listeners = BTreeMap::new();
        for i in 1..=opts.nodes as NodeId {
            let port = opts.base_port.map_or(0, |p| p + (i - 1) as u16);
            let listener = TcpListener::bind(("127.0.0.1", port)).await?;
            listeners.insert(i, listener);
        }
        let addrs = listeners
            .iter()
            .map(|(&id, l)| Ok((id, l.local_addr()?)))
            .collect::<io::Result<BTreeMap<_, _>>>()?;
        let mut cluster = Self {
            opts,
            addrs,
            handles: BTreeMap::new(),
        };
        for (id, listener) in listeners {
            cluster.launch(id, listener)?;
        }
        Ok(cluster)
    }

    fn server_config(&self, id: NodeId) -> ServerConfig {
        let mut cfg = ServerConfig::new(
            id,
            self.addrs.clone(),
            self.opts.data_dir.join(format!("node-{id}")),
        );
        cfg.fsync = self.opts.fsync;
        cfg.snapshot_threshold = self.opts.snapshot_threshold;
        cfg.tick = self.opts.tick;
        cfg.wal_prealloc = self.opts.wal_prealloc;
        cfg
    }

    fn launch(&mut self, id: NodeId, listener: TcpListener) -> io::Result<()> {
        let handle = server::start(self.server_config(id), listener)?;
        self.handles.insert(id, handle);
        Ok(())
    }

    pub fn addrs(&self) -> &BTreeMap<NodeId, SocketAddr> {
        &self.addrs
    }

    /// Client-facing addresses of all nodes.
    pub fn client_addrs(&self) -> Vec<String> {
        self.addrs.values().map(|a| a.to_string()).collect()
    }

    pub fn client(&self) -> Client {
        Client::new(self.client_addrs())
    }

    pub fn is_running(&self, id: NodeId) -> bool {
        self.handles.contains_key(&id)
    }

    /// Stops a node (simulating a crash; its data directory is kept).
    pub async fn stop(&mut self, id: NodeId) {
        if let Some(h) = self.handles.remove(&id) {
            h.shutdown().await;
        }
    }

    /// Restarts a stopped node on its original address, recovering from its data directory.
    pub async fn restart(&mut self, id: NodeId) -> io::Result<()> {
        let addr = self.addrs[&id];
        let mut last_err = None;
        for _ in 0..50 {
            match TcpListener::bind(addr).await {
                Ok(listener) => return self.launch(id, listener),
                Err(e) => {
                    last_err = Some(e);
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
        Err(last_err.expect("at least one attempt"))
    }

    pub async fn status(&self, id: NodeId) -> Option<NodeStatus> {
        if !self.is_running(id) {
            return None;
        }
        Client::status(&self.addrs[&id].to_string(), Duration::from_millis(500))
            .await
            .ok()
    }

    /// The current leader, if running nodes agree on one.
    pub async fn leader(&self) -> Option<NodeId> {
        let mut best: Option<(u64, NodeId)> = None;
        for &id in self.handles.keys() {
            if let Some(s) = self.status(id).await {
                if s.role == Role::Leader && best.is_none_or(|(t, _)| s.term > t) {
                    best = Some((s.term, id));
                }
            }
        }
        best.map(|(_, id)| id)
    }

    pub async fn wait_for_leader(&self, timeout: Duration) -> io::Result<NodeId> {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if let Some(l) = self.leader().await {
                return Ok(l);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        Err(io::Error::new(io::ErrorKind::TimedOut, "no leader elected"))
    }

    pub async fn shutdown(mut self) {
        let ids: Vec<NodeId> = self.handles.keys().copied().collect();
        for id in ids {
            self.stop(id).await;
        }
    }
}
