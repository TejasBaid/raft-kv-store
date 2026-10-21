//! Wire protocol: client request/response types and length-prefixed framing.
//!
//! Every frame is a big-endian `u32` payload length followed by a bincode payload. The
//! first frame on a connection is a [`Hello`] identifying it as a peer link (a stream
//! of Raft [`Message`](crate::raft::Message)s) or a client link (pipelined
//! [`RequestFrame`]/[`ResponseFrame`] pairs matched by id).

use std::io;

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::kv::{KvResult, WriteRequest};
use crate::raft::{LogIndex, NodeId, Role, Term};

/// Frames larger than this are rejected as corrupt (snapshots are the largest messages).
pub const MAX_FRAME_LEN: usize = 256 * 1024 * 1024;

/// First frame on every connection.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Hello {
    Peer { from: NodeId },
    Client,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClientRequest {
    /// Linearizable read (served by the leader via ReadIndex).
    Get { key: String },
    /// A write that goes through the replicated log.
    Write(WriteRequest),
    /// Introspection; answered locally by any node.
    Status,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClientResponse {
    Ok(KvResult),
    /// This node is not the leader. Retry at `leader_addr` if known, otherwise elsewhere.
    NotLeader {
        leader_id: Option<NodeId>,
        leader_addr: Option<String>,
    },
    /// Leadership changed while the request was in flight. The outcome is unknown; the
    /// client should retry with the same sequence number (dedup makes this safe).
    Unavailable(String),
    Status(NodeStatus),
    /// A non-retryable error.
    Error(String),
}

/// A snapshot of a node's internal state, for operators and tests.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeStatus {
    pub id: NodeId,
    pub role: Role,
    pub term: Term,
    pub leader: Option<NodeId>,
    pub commit_index: LogIndex,
    pub last_applied: LogIndex,
    pub last_log_index: LogIndex,
    pub snapshot_index: LogIndex,
    pub keys: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestFrame {
    pub id: u64,
    pub request: ClientRequest,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResponseFrame {
    pub id: u64,
    pub response: ClientResponse,
}

/// Appends one encoded frame to `buf` (lets callers batch several frames per write).
pub fn encode_frame<T: Serialize>(buf: &mut Vec<u8>, value: &T) -> io::Result<()> {
    let start = buf.len();
    buf.extend_from_slice(&[0; 4]);
    bincode::serialize_into(&mut *buf, value).map_err(io::Error::other)?;
    let len = buf.len() - start - 4;
    if len > MAX_FRAME_LEN {
        buf.truncate(start);
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "frame too large",
        ));
    }
    buf[start..start + 4].copy_from_slice(&(len as u32).to_be_bytes());
    Ok(())
}

pub async fn write_frame<W, T>(w: &mut W, value: &T) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
    T: Serialize,
{
    let mut buf = Vec::new();
    encode_frame(&mut buf, value)?;
    w.write_all(&buf).await
}

/// Reads one frame. Returns `Ok(None)` on a clean end-of-stream between frames.
pub async fn read_frame<R, T>(r: &mut R) -> io::Result<Option<T>>
where
    R: AsyncRead + Unpin,
    T: DeserializeOwned,
{
    let mut len = [0u8; 4];
    match r.read_exact(&mut len).await {
        Ok(_) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    let len = u32::from_be_bytes(len) as usize;
    if len > MAX_FRAME_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "frame too large",
        ));
    }
    let mut payload = vec![0u8; len];
    r.read_exact(&mut payload).await?;
    bincode::deserialize(&payload)
        .map(Some)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kv::Command;

    #[tokio::test]
    async fn frames_round_trip() {
        let frames = vec![
            RequestFrame {
                id: 1,
                request: ClientRequest::Get { key: "a".into() },
            },
            RequestFrame {
                id: 2,
                request: ClientRequest::Write(WriteRequest {
                    client_id: 3,
                    seq: 4,
                    command: Command::Delete { key: "b".into() },
                }),
            },
        ];
        let mut buf = Vec::new();
        for f in &frames {
            encode_frame(&mut buf, f).unwrap();
        }
        let mut r = buf.as_slice();
        for f in &frames {
            let got: RequestFrame = read_frame(&mut r).await.unwrap().unwrap();
            assert_eq!(&got, f);
        }
        assert!(
            read_frame::<_, RequestFrame>(&mut r)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn rejects_oversized_length_prefix() {
        let buf = (u32::MAX).to_be_bytes();
        let mut r = &buf[..];
        assert!(read_frame::<_, Hello>(&mut r).await.is_err());
    }
}
