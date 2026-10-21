//! Async client library.
//!
//! The client finds the leader by following `NotLeader` hints, retries on timeouts and
//! leadership changes, and tags every write with `(client_id, seq)`. Retries reuse the
//! sequence number, so a write that was applied but whose response was lost is not
//! applied a second time: the server returns the cached result instead.
//!
//! A `Client` issues one request at a time; use several clients for concurrency.

use std::io;
use std::time::{Duration, Instant};

use tokio::io::BufReader;
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

use crate::kv::{Command, KvResult, WriteRequest};
use crate::protocol::{
    ClientRequest, ClientResponse, Hello, NodeStatus, RequestFrame, ResponseFrame, read_frame,
    write_frame,
};

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("no response from the cluster within {0:?}")]
    Timeout(Duration),
    #[error("server error: {0}")]
    Server(String),
    #[error("unexpected response: {0:?}")]
    Unexpected(Box<ClientResponse>),
    #[error("i/o error: {0}")]
    Io(#[from] io::Error),
}

pub type Result<T> = std::result::Result<T, ClientError>;

struct Conn {
    r: BufReader<OwnedReadHalf>,
    w: OwnedWriteHalf,
}

impl Conn {
    async fn open(addr: &str, timeout: Duration) -> io::Result<Self> {
        let stream = tokio::time::timeout(timeout, TcpStream::connect(addr))
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "connect timed out"))??;
        stream.set_nodelay(true)?;
        let (r, mut w) = stream.into_split();
        write_frame(&mut w, &Hello::Client).await?;
        Ok(Self {
            r: BufReader::new(r),
            w,
        })
    }
}

/// A connection to a raft-kv cluster.
pub struct Client {
    addrs: Vec<String>,
    current: usize,
    conn: Option<Conn>,
    client_id: u64,
    seq: u64,
    next_frame: u64,
    /// Per-attempt timeout before trying another node.
    pub request_timeout: Duration,
    /// Overall deadline for one logical operation, across retries.
    pub operation_timeout: Duration,
}

impl Client {
    /// Creates a client for a cluster reachable at any of `addrs`. Connects lazily.
    pub fn new(addrs: Vec<String>) -> Self {
        assert!(!addrs.is_empty(), "need at least one address");
        Self {
            addrs,
            current: 0,
            conn: None,
            client_id: rand::random::<u64>() | 1,
            seq: 0,
            next_frame: 0,
            request_timeout: Duration::from_millis(1000),
            operation_timeout: Duration::from_secs(10),
        }
    }

    pub fn client_id(&self) -> u64 {
        self.client_id
    }

    /// Linearizable read.
    pub async fn get(&mut self, key: &str) -> Result<Option<Vec<u8>>> {
        match self.call(ClientRequest::Get { key: key.into() }).await? {
            KvResult::Value(v) => Ok(v),
            other => Err(unexpected(other)),
        }
    }

    pub async fn put(&mut self, key: &str, value: impl Into<Vec<u8>>) -> Result<()> {
        let command = Command::Put {
            key: key.into(),
            value: value.into(),
        };
        match self.write(command).await? {
            KvResult::Written => Ok(()),
            other => Err(unexpected(other)),
        }
    }

    /// Deletes `key`; returns whether it existed.
    pub async fn delete(&mut self, key: &str) -> Result<bool> {
        match self.write(Command::Delete { key: key.into() }).await? {
            KvResult::Deleted(existed) => Ok(existed),
            other => Err(unexpected(other)),
        }
    }

    /// Compare-and-swap. Returns whether the swap happened and the value before it.
    pub async fn cas(
        &mut self,
        key: &str,
        expected: Option<Vec<u8>>,
        new: Option<Vec<u8>>,
    ) -> Result<(bool, Option<Vec<u8>>)> {
        let command = Command::Cas {
            key: key.into(),
            expected,
            new,
        };
        match self.write(command).await? {
            KvResult::Cas { swapped, previous } => Ok((swapped, previous)),
            other => Err(unexpected(other)),
        }
    }

    /// Asks one specific node for its status.
    pub async fn status(addr: &str, timeout: Duration) -> Result<NodeStatus> {
        let mut conn = Conn::open(addr, timeout).await?;
        let frame = RequestFrame {
            id: 0,
            request: ClientRequest::Status,
        };
        write_frame(&mut conn.w, &frame).await?;
        let resp = tokio::time::timeout(timeout, read_frame::<_, ResponseFrame>(&mut conn.r))
            .await
            .map_err(|_| ClientError::Timeout(timeout))??;
        match resp.map(|f| f.response) {
            Some(ClientResponse::Status(s)) => Ok(s),
            Some(other) => Err(ClientError::Unexpected(Box::new(other))),
            None => Err(io::Error::from(io::ErrorKind::UnexpectedEof).into()),
        }
    }

    async fn write(&mut self, command: Command) -> Result<KvResult> {
        self.seq += 1;
        let req = ClientRequest::Write(WriteRequest {
            client_id: self.client_id,
            seq: self.seq,
            command,
        });
        self.call(req).await
    }

    async fn call(&mut self, req: ClientRequest) -> Result<KvResult> {
        let deadline = Instant::now() + self.operation_timeout;
        let mut backoff = Duration::from_millis(5);
        let mut hops = 0;
        loop {
            if Instant::now() >= deadline {
                return Err(ClientError::Timeout(self.operation_timeout));
            }
            match self.attempt(&req).await {
                Ok(ClientResponse::Ok(result)) => return Ok(result),
                Ok(ClientResponse::NotLeader {
                    leader_addr: Some(addr),
                    ..
                }) => {
                    self.switch_to(&addr);
                    hops += 1;
                    if hops <= 3 {
                        continue; // follow the hint immediately (bounded, in case hints loop)
                    }
                }
                Ok(ClientResponse::NotLeader { .. }) | Ok(ClientResponse::Unavailable(_)) => {
                    self.rotate();
                }
                Ok(ClientResponse::Error(e)) => return Err(ClientError::Server(e)),
                Ok(other @ ClientResponse::Status(_)) => {
                    return Err(ClientError::Unexpected(Box::new(other)));
                }
                Err(_) => {
                    self.conn = None;
                    self.rotate();
                }
            }
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(Duration::from_millis(200));
        }
    }

    async fn attempt(&mut self, req: &ClientRequest) -> io::Result<ClientResponse> {
        if self.conn.is_none() {
            let addr = &self.addrs[self.current];
            self.conn = Some(Conn::open(addr, self.request_timeout).await?);
        }
        let conn = self.conn.as_mut().expect("connected above");
        self.next_frame += 1;
        let id = self.next_frame;
        let frame = RequestFrame {
            id,
            request: req.clone(),
        };
        write_frame(&mut conn.w, &frame).await?;
        let read = async {
            loop {
                match read_frame::<_, ResponseFrame>(&mut conn.r).await? {
                    Some(f) if f.id == id => return Ok(f.response),
                    Some(_) => continue, // late response to an earlier attempt
                    None => return Err(io::Error::from(io::ErrorKind::UnexpectedEof)),
                }
            }
        };
        match tokio::time::timeout(self.request_timeout, read).await {
            Ok(r) => r,
            Err(_) => Err(io::Error::new(io::ErrorKind::TimedOut, "request timed out")),
        }
    }

    fn rotate(&mut self) {
        self.current = (self.current + 1) % self.addrs.len();
        self.conn = None;
    }

    fn switch_to(&mut self, addr: &str) {
        let idx = match self.addrs.iter().position(|a| a == addr) {
            Some(i) => i,
            None => {
                self.addrs.push(addr.to_string());
                self.addrs.len() - 1
            }
        };
        if idx != self.current || self.conn.is_none() {
            self.current = idx;
            self.conn = None;
        }
    }
}

fn unexpected(r: KvResult) -> ClientError {
    ClientError::Unexpected(Box::new(ClientResponse::Ok(r)))
}
