//! The networked server: drives a [`KvNode`] with real time, real disk and real TCP.
//!
//! Threading model:
//!
//! * One dedicated OS thread owns the [`KvNode`] and its [`FileStorage`]. It is the only
//!   place where consensus state changes, so no locks are needed. It wakes up for
//!   incoming events or the next tick, drains every queued event (natural batching:
//!   many proposals share one fsync and one AppendEntries), then persists, sends and
//!   responds.
//! * Tokio tasks handle sockets: an accept loop, one reader per inbound connection, and
//!   one sender per peer that keeps a connection open and streams Raft messages.
//!
//! Raft messages are fire-and-forget: if a peer is slow or down, its queue fills and
//! messages are dropped, which Raft tolerates by design.

use std::collections::BTreeMap;
use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::mpsc as std_mpsc;
use std::time::{Duration, Instant};

use tokio::io::{AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::task::{JoinHandle, JoinSet};

use crate::node::{KvNode, NodeConfig, RequestId};
use crate::protocol::{
    ClientRequest, Hello, RequestFrame, ResponseFrame, encode_frame, read_frame, write_frame,
};
use crate::raft::{self, Message, NodeId};
use crate::storage::{FileStorage, Storage};

/// Configuration for one server process.
#[derive(Clone, Debug)]
pub struct ServerConfig {
    pub id: NodeId,
    /// Addresses of every node in the cluster, including this one.
    pub peers: BTreeMap<NodeId, SocketAddr>,
    pub data_dir: PathBuf,
    /// Duration of one Raft tick.
    pub tick: Duration,
    /// Election timeout range, in ticks.
    pub election_ticks: (u32, u32),
    /// Heartbeat interval, in ticks.
    pub heartbeat_ticks: u32,
    /// fsync the WAL before acknowledging anything. Disable only for benchmarking.
    pub fsync: bool,
    pub snapshot_threshold: u64,
    /// WAL preallocation step in bytes (see [`FileStorage`]).
    pub wal_prealloc: u64,
}

impl ServerConfig {
    pub fn new(id: NodeId, peers: BTreeMap<NodeId, SocketAddr>, data_dir: PathBuf) -> Self {
        Self {
            id,
            peers,
            data_dir,
            tick: Duration::from_millis(10),
            election_ticks: (15, 30),
            heartbeat_ticks: 3,
            fsync: true,
            snapshot_threshold: 10_000,
            wal_prealloc: crate::storage::DEFAULT_PREALLOC,
        }
    }
}

enum Event {
    Raft(Message),
    Client {
        id: u64,
        request: ClientRequest,
        reply: mpsc::UnboundedSender<ResponseFrame>,
    },
    Shutdown,
}

/// A running server. Dropping the handle does not stop it; call [`shutdown`](Self::shutdown).
pub struct ServerHandle {
    id: NodeId,
    addr: SocketAddr,
    events: std_mpsc::Sender<Event>,
    tasks: Vec<JoinHandle<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl ServerHandle {
    pub fn id(&self) -> NodeId {
        self.id
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Stops the server: closes the listener and all connections and joins the node thread.
    /// Durable state is left on disk, so the node can be restarted from it.
    pub async fn shutdown(mut self) {
        for t in &self.tasks {
            t.abort();
        }
        for t in self.tasks.drain(..) {
            let _ = t.await;
        }
        let _ = self.events.send(Event::Shutdown);
        if let Some(thread) = self.thread.take() {
            let _ = tokio::task::spawn_blocking(move || thread.join()).await;
        }
    }

    /// Whether the node thread has exited (only happens on shutdown or storage failure).
    pub fn is_finished(&self) -> bool {
        self.thread.as_ref().is_none_or(|t| t.is_finished())
    }
}

/// Starts a server on an already-bound listener (bind to port 0 for tests).
/// Must be called from within a tokio runtime.
pub fn start(cfg: ServerConfig, listener: TcpListener) -> io::Result<ServerHandle> {
    let addr = listener.local_addr()?;
    let (storage, state) =
        FileStorage::open_with_prealloc(&cfg.data_dir, cfg.fsync, cfg.wal_prealloc)?;

    let voters: Vec<NodeId> = cfg.peers.keys().copied().collect();
    let mut raft_cfg = raft::Config::new(cfg.id, voters);
    raft_cfg.election_timeout_min = cfg.election_ticks.0;
    raft_cfg.election_timeout_max = cfg.election_ticks.1;
    raft_cfg.heartbeat_interval = cfg.heartbeat_ticks;
    raft_cfg.seed = rand::random();
    let mut node_cfg = NodeConfig::new(raft_cfg);
    node_cfg.snapshot_threshold = cfg.snapshot_threshold;
    node_cfg.addrs = cfg
        .peers
        .iter()
        .map(|(&id, a)| (id, a.to_string()))
        .collect();
    let node =
        KvNode::new(node_cfg, state).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    let (events_tx, events_rx) = std_mpsc::channel();
    let mut tasks = Vec::new();
    let mut peer_queues = BTreeMap::new();
    for (&peer, &peer_addr) in cfg.peers.iter().filter(|(id, _)| **id != cfg.id) {
        let (tx, rx) = mpsc::channel(8192);
        peer_queues.insert(peer, tx);
        tasks.push(tokio::spawn(peer_sender(cfg.id, peer_addr, rx)));
    }
    tasks.push(tokio::spawn(accept_loop(listener, events_tx.clone())));

    let id = cfg.id;
    let tick = cfg.tick;
    let thread = std::thread::Builder::new()
        .name(format!("raft-node-{id}"))
        .spawn(move || run_node(node, storage, events_rx, peer_queues, tick))?;

    tracing::info!(id, %addr, "server started");
    Ok(ServerHandle {
        id,
        addr,
        events: events_tx,
        tasks,
        thread: Some(thread),
    })
}

/// State owned by the node thread.
struct NodeLoop {
    node: KvNode,
    storage: FileStorage,
    peers: BTreeMap<NodeId, mpsc::Sender<Message>>,
    replies: BTreeMap<RequestId, (u64, mpsc::UnboundedSender<ResponseFrame>)>,
    next_req: RequestId,
}

impl NodeLoop {
    /// Feeds one event to the node. Returns `false` on shutdown.
    fn handle(&mut self, ev: Event) -> bool {
        match ev {
            Event::Raft(msg) => self.node.step(msg),
            Event::Client { id, request, reply } => {
                let req = self.next_req;
                self.next_req += 1;
                self.replies.insert(req, (id, reply));
                self.node.handle_client(req, request);
            }
            Event::Shutdown => return false,
        }
        true
    }

    /// Persists, then sends messages and responses, until the node has nothing left to
    /// do. Returns `false` on storage failure.
    fn flush(&mut self) -> bool {
        loop {
            let out = self.node.poll();
            if out.is_empty() {
                return true;
            }
            // Leader replication traffic goes out before the local disk write, so the
            // leader's fsync overlaps with the followers'.
            self.send(out.pre_persist_messages);
            if let Some(p) = &out.persist {
                if let Err(e) = self.storage.persist(p) {
                    // Continuing without durable state could violate safety: crash-stop.
                    tracing::error!(error = %e, "storage failure; stopping node");
                    return false;
                }
            }
            self.node.advance();
            self.send(out.messages);
            for (req, response) in out.responses {
                if let Some((id, reply)) = self.replies.remove(&req) {
                    let _ = reply.send(ResponseFrame { id, response });
                }
            }
        }
    }

    fn send(&self, msgs: Vec<Message>) {
        for msg in msgs {
            if let Some(q) = self.peers.get(&msg.to) {
                let _ = q.try_send(msg); // full queue => drop; Raft retransmits
            }
        }
    }
}

/// The node thread's event loop.
fn run_node(
    node: KvNode,
    storage: FileStorage,
    events: std_mpsc::Receiver<Event>,
    peers: BTreeMap<NodeId, mpsc::Sender<Message>>,
    tick: Duration,
) {
    const MAX_BATCH: usize = 4096;
    let mut st = NodeLoop {
        node,
        storage,
        peers,
        replies: BTreeMap::new(),
        next_req: 0,
    };
    let mut next_tick = Instant::now() + tick;
    loop {
        let now = Instant::now();
        if now >= next_tick {
            st.node.tick();
            next_tick += tick;
            if next_tick < now {
                next_tick = now + tick; // we fell behind; don't fire a burst of ticks
            }
        } else {
            match events.recv_timeout(next_tick - now) {
                Ok(ev) => {
                    if !st.handle(ev) {
                        return;
                    }
                    // Drain whatever else is queued so it shares one persist + send.
                    for _ in 0..MAX_BATCH {
                        let Ok(ev) = events.try_recv() else { break };
                        if !st.handle(ev) {
                            return;
                        }
                    }
                }
                Err(std_mpsc::RecvTimeoutError::Timeout) => continue,
                Err(std_mpsc::RecvTimeoutError::Disconnected) => return,
            }
        }
        if !st.flush() {
            return;
        }
    }
}

async fn accept_loop(listener: TcpListener, events: std_mpsc::Sender<Event>) {
    let mut conns = JoinSet::new();
    loop {
        while conns.try_join_next().is_some() {}
        match listener.accept().await {
            Ok((stream, _)) => {
                let _ = stream.set_nodelay(true);
                conns.spawn(handle_conn(stream, events.clone()));
            }
            Err(e) => {
                tracing::warn!(error = %e, "accept failed");
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
}

async fn handle_conn(stream: TcpStream, events: std_mpsc::Sender<Event>) {
    let (r, w) = stream.into_split();
    let mut r = BufReader::with_capacity(64 * 1024, r);
    match read_frame::<_, Hello>(&mut r).await {
        Ok(Some(Hello::Peer { .. })) => {
            while let Ok(Some(msg)) = read_frame::<_, Message>(&mut r).await {
                if events.send(Event::Raft(msg)).is_err() {
                    return;
                }
            }
        }
        Ok(Some(Hello::Client)) => {
            let (tx, rx) = mpsc::unbounded_channel();
            let writer = AbortOnDrop(tokio::spawn(client_writer(w, rx)));
            while let Ok(Some(frame)) = read_frame::<_, RequestFrame>(&mut r).await {
                let ev = Event::Client {
                    id: frame.id,
                    request: frame.request,
                    reply: tx.clone(),
                };
                if events.send(ev).is_err() {
                    return;
                }
            }
            drop(writer);
        }
        _ => {}
    }
}

/// Writes responses for one client connection, coalescing whatever is ready into one write.
async fn client_writer(
    mut w: tokio::net::tcp::OwnedWriteHalf,
    mut rx: mpsc::UnboundedReceiver<ResponseFrame>,
) {
    let mut buf = Vec::new();
    while let Some(frame) = rx.recv().await {
        buf.clear();
        if encode_frame(&mut buf, &frame).is_err() {
            return;
        }
        while let Ok(frame) = rx.try_recv() {
            if encode_frame(&mut buf, &frame).is_err() {
                return;
            }
        }
        if w.write_all(&buf).await.is_err() {
            return;
        }
    }
}

/// Keeps a connection to one peer and streams Raft messages to it.
async fn peer_sender(me: NodeId, addr: SocketAddr, mut rx: mpsc::Receiver<Message>) {
    let mut buf = Vec::new();
    loop {
        let connect = tokio::time::timeout(Duration::from_millis(500), TcpStream::connect(addr));
        let mut stream = match connect.await {
            Ok(Ok(s)) => s,
            _ => {
                // Peer unreachable: discard queued (soon stale) messages and back off.
                while rx.try_recv().is_ok() {}
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };
        let _ = stream.set_nodelay(true);
        if write_frame(&mut stream, &Hello::Peer { from: me })
            .await
            .is_err()
        {
            continue;
        }
        loop {
            let Some(msg) = rx.recv().await else {
                return; // node shut down
            };
            buf.clear();
            let mut ok = encode_frame(&mut buf, &msg).is_ok();
            while let Ok(msg) = rx.try_recv() {
                ok &= encode_frame(&mut buf, &msg).is_ok();
                if buf.len() > 1 << 20 {
                    break;
                }
            }
            if !ok || stream.write_all(&buf).await.is_err() {
                break; // reconnect
            }
        }
    }
}

struct AbortOnDrop(JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}
