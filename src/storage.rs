//! Durable storage for Raft state: hard state (term, vote), the log, and the snapshot.
//!
//! [`FileStorage`] is a single append-only write-ahead log of checksummed records in a
//! preallocated file. Normal operation appends `HardState` / `LogWrite` records and
//! (optionally) fsyncs once per batch. Taking or installing a snapshot rewrites the file as
//! `[Snapshot, HardState, LogWrite]` into a temporary file that is atomically renamed
//! into place, which also bounds the file's size. On startup the records are replayed;
//! a torn or corrupt tail (from a crash mid-write) is detected by its CRC and cut off.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::raft::{Entry, HardState, LogIndex, LogWrite, PersistentState, Snapshot};

/// A batch of state changes to make durable, produced by [`KvNode::poll`](crate::node::KvNode::poll).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Persist {
    /// Incremental update.
    Append {
        hard_state: Option<HardState>,
        log_write: Option<LogWrite>,
    },
    /// Replace everything: used whenever the snapshot changes.
    Rewrite {
        hard_state: HardState,
        snapshot: Snapshot,
        entries: Vec<Entry>,
    },
}

/// Applies `p` to an in-memory image of durable state.
pub fn apply_persist(state: &mut PersistentState, p: &Persist) {
    match p {
        Persist::Append {
            hard_state,
            log_write,
        } => {
            if let Some(hs) = hard_state {
                state.hard_state = *hs;
            }
            if let Some(w) = log_write {
                apply_log_write(state, w.truncate_from, &w.entries);
            }
        }
        Persist::Rewrite {
            hard_state,
            snapshot,
            entries,
        } => {
            state.hard_state = *hard_state;
            state.snapshot = snapshot.clone();
            state.entries = entries.clone();
        }
    }
}

fn apply_log_write(state: &mut PersistentState, truncate_from: LogIndex, entries: &[Entry]) {
    state.entries.retain(|e| e.index < truncate_from);
    let base = state.snapshot.meta.last_index;
    state
        .entries
        .extend(entries.iter().filter(|e| e.index > base).cloned());
}

/// Something that can make [`Persist`] batches durable.
pub trait Storage {
    fn persist(&mut self, p: &Persist) -> io::Result<()>;
}

/// In-memory storage. The simulator keeps one per node and uses it to rebuild the node
/// after a simulated crash, so it models exactly what would have reached disk.
#[derive(Clone, Debug, Default)]
pub struct MemStorage {
    state: PersistentState,
}

impl MemStorage {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn state(&self) -> &PersistentState {
        &self.state
    }
}

impl Storage for MemStorage {
    fn persist(&mut self, p: &Persist) -> io::Result<()> {
        apply_persist(&mut self.state, p);
        Ok(())
    }
}

/// Owned form of a WAL record (used when reading).
#[derive(Deserialize)]
enum Record {
    HardState(HardState),
    LogWrite {
        truncate_from: LogIndex,
        entries: Vec<Entry>,
    },
    Snapshot(Snapshot),
}

/// Borrowed form of a WAL record (used when writing, to avoid cloning entries).
/// Must keep the same variant order as [`Record`].
#[derive(Serialize)]
enum RecordRef<'a> {
    HardState(&'a HardState),
    LogWrite {
        truncate_from: LogIndex,
        entries: &'a [Entry],
    },
    Snapshot(&'a Snapshot),
}

const RECORD_HEADER: usize = 8; // u32 length + u32 crc32

fn encode_record(buf: &mut Vec<u8>, rec: &RecordRef<'_>) {
    let start = buf.len();
    buf.extend_from_slice(&[0; RECORD_HEADER]);
    bincode::serialize_into(&mut *buf, rec).expect("record serialization cannot fail");
    let payload = &buf[start + RECORD_HEADER..];
    let len = payload.len() as u32;
    let crc = crc32fast::hash(payload);
    buf[start..start + 4].copy_from_slice(&len.to_le_bytes());
    buf[start + 4..start + 8].copy_from_slice(&crc.to_le_bytes());
}

/// Default growth step for the WAL's zero-filled preallocation.
pub const DEFAULT_PREALLOC: u64 = 16 << 20;

/// Durable, file-backed storage.
///
/// The WAL file is grown in zero-filled steps of `prealloc` bytes and records are
/// written into that space at an explicit offset. Overwriting already-allocated blocks
/// avoids a file-size/extent metadata update on every write: on APFS, appends that
/// extend a file were measured to stall for 1-3 ms each, versus microseconds for
/// writes into preallocated space (etcd preallocates its WAL segments for the same
/// reason). Replay stops at the first all-zero record header.
pub struct FileStorage {
    dir: PathBuf,
    file: File,
    fsync: bool,
    buf: Vec<u8>,
    /// Offset just past the last valid record.
    write_pos: u64,
    /// Length of the file; everything in `write_pos..allocated` is zeros.
    allocated: u64,
    prealloc: u64,
}

const WAL_FILE: &str = "raft.wal";
const WAL_TMP: &str = "raft.wal.tmp";

impl FileStorage {
    /// Opens (or creates) storage in `dir` and recovers its contents.
    ///
    /// With `fsync == false` writes still reach the OS page cache before any message is
    /// sent, so the node survives a process crash but not a power failure.
    pub fn open(dir: &Path, fsync: bool) -> io::Result<(Self, PersistentState)> {
        Self::open_with_prealloc(dir, fsync, DEFAULT_PREALLOC)
    }

    /// Like [`open`](Self::open) with an explicit preallocation step (in bytes).
    pub fn open_with_prealloc(
        dir: &Path,
        fsync: bool,
        prealloc: u64,
    ) -> io::Result<(Self, PersistentState)> {
        fs::create_dir_all(dir)?;
        let path = dir.join(WAL_FILE);
        let _ = fs::remove_file(dir.join(WAL_TMP)); // leftover from a crash mid-rewrite
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        let (state, valid_len) = replay(&bytes);
        if bytes[valid_len..].iter().any(|&b| b != 0) {
            tracing::warn!(
                path = %path.display(),
                offset = valid_len,
                "discarding torn or corrupt WAL tail"
            );
        }
        // Cut off torn records and stale padding, then re-pad with zeros so that no
        // garbage can ever follow the valid prefix.
        let valid_len = valid_len as u64;
        file.set_len(valid_len)?;
        let mut storage = Self {
            dir: dir.to_path_buf(),
            file,
            fsync,
            buf: Vec::new(),
            write_pos: valid_len,
            allocated: valid_len,
            prealloc: prealloc.max(4096),
        };
        storage.grow_to(valid_len + storage.prealloc)?;
        if fsync {
            storage.file.sync_all()?;
        }
        Ok((storage, state))
    }

    /// Zero-fills the file from its current end up to `target` bytes.
    fn grow_to(&mut self, target: u64) -> io::Result<()> {
        if target <= self.allocated {
            return Ok(());
        }
        static ZEROS: [u8; 64 * 1024] = [0; 64 * 1024];
        self.file.seek(SeekFrom::Start(self.allocated))?;
        let mut remaining = target - self.allocated;
        while remaining > 0 {
            let n = remaining.min(ZEROS.len() as u64) as usize;
            self.file.write_all(&ZEROS[..n])?;
            remaining -= n as u64;
        }
        self.allocated = target;
        Ok(())
    }

    fn append(&mut self) -> io::Result<()> {
        let len = self.buf.len() as u64;
        if self.write_pos + len > self.allocated {
            self.grow_to(self.write_pos + len + self.prealloc)?;
        }
        self.file.seek(SeekFrom::Start(self.write_pos))?;
        self.file.write_all(&self.buf)?;
        self.write_pos += len;
        if self.fsync {
            self.file.sync_data()?;
        }
        Ok(())
    }

    fn rewrite(
        &mut self,
        hard_state: &HardState,
        snapshot: &Snapshot,
        entries: &[Entry],
    ) -> io::Result<()> {
        self.buf.clear();
        encode_record(&mut self.buf, &RecordRef::Snapshot(snapshot));
        encode_record(&mut self.buf, &RecordRef::HardState(hard_state));
        encode_record(
            &mut self.buf,
            &RecordRef::LogWrite {
                truncate_from: snapshot.meta.last_index + 1,
                entries,
            },
        );
        let tmp = self.dir.join(WAL_TMP);
        let path = self.dir.join(WAL_FILE);
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&tmp)?;
        let old = std::mem::replace(&mut self.file, file);
        self.allocated = 0;
        self.write_pos = 0;
        let result = (|| {
            self.grow_to(self.buf.len() as u64 + self.prealloc)?;
            self.file.seek(SeekFrom::Start(0))?;
            self.file.write_all(&self.buf)?;
            self.write_pos = self.buf.len() as u64;
            if self.fsync {
                self.file.sync_all()?;
            }
            fs::rename(&tmp, &path)?;
            if self.fsync {
                File::open(&self.dir)?.sync_all()?;
            }
            Ok(())
        })();
        drop(old);
        result
    }
}

impl Storage for FileStorage {
    fn persist(&mut self, p: &Persist) -> io::Result<()> {
        match p {
            Persist::Append {
                hard_state,
                log_write,
            } => {
                self.buf.clear();
                if let Some(hs) = hard_state {
                    encode_record(&mut self.buf, &RecordRef::HardState(hs));
                }
                if let Some(w) = log_write {
                    encode_record(
                        &mut self.buf,
                        &RecordRef::LogWrite {
                            truncate_from: w.truncate_from,
                            entries: &w.entries,
                        },
                    );
                }
                if self.buf.is_empty() {
                    return Ok(());
                }
                self.append()
            }
            Persist::Rewrite {
                hard_state,
                snapshot,
                entries,
            } => self.rewrite(hard_state, snapshot, entries),
        }
    }
}

/// Replays WAL bytes. Returns the recovered state and the length of the valid prefix.
fn replay(bytes: &[u8]) -> (PersistentState, usize) {
    let mut state = PersistentState::default();
    let mut pos = 0;
    while bytes.len() - pos >= RECORD_HEADER {
        let len = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
        let crc = u32::from_le_bytes(bytes[pos + 4..pos + 8].try_into().unwrap());
        if len == 0 {
            break; // preallocated (zeroed) space: end of log
        }
        let start = pos + RECORD_HEADER;
        let Some(payload) = bytes.get(start..start + len) else {
            break; // torn write
        };
        if crc32fast::hash(payload) != crc {
            break;
        }
        let Ok(record) = bincode::deserialize::<Record>(payload) else {
            break;
        };
        match record {
            Record::HardState(hs) => state.hard_state = hs,
            Record::LogWrite {
                truncate_from,
                entries,
            } => apply_log_write(&mut state, truncate_from, &entries),
            Record::Snapshot(snap) => {
                state.entries.retain(|e| e.index > snap.meta.last_index);
                state.snapshot = snap;
            }
        }
        pos = start + len;
    }
    (state, pos)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::{EntryPayload, SnapshotMeta};

    fn entry(index: LogIndex, term: u64) -> Entry {
        Entry {
            index,
            term,
            payload: EntryPayload::Command(vec![index as u8; 16]),
        }
    }

    fn append(from: LogIndex, entries: Vec<Entry>, hs: Option<HardState>) -> Persist {
        Persist::Append {
            hard_state: hs,
            log_write: Some(LogWrite {
                truncate_from: from,
                entries,
            }),
        }
    }

    #[test]
    fn recovers_appends_truncations_and_hard_state() {
        let dir = tempfile::tempdir().unwrap();
        let hs = HardState {
            term: 3,
            voted_for: Some(2),
        };
        {
            let (mut s, state) = FileStorage::open(dir.path(), true).unwrap();
            assert_eq!(state, PersistentState::default());
            s.persist(&append(1, (1..=5).map(|i| entry(i, 1)).collect(), Some(hs)))
                .unwrap();
            // Conflict: replace entries 4.. with term-2 entries.
            s.persist(&append(4, vec![entry(4, 2)], None)).unwrap();
        }
        let (_, state) = FileStorage::open(dir.path(), true).unwrap();
        assert_eq!(state.hard_state, hs);
        let terms: Vec<_> = state.entries.iter().map(|e| (e.index, e.term)).collect();
        assert_eq!(terms, vec![(1, 1), (2, 1), (3, 1), (4, 2)]);
    }

    const SMALL: u64 = 4096;

    /// Writes two batches and returns the WAL offsets where the second batch starts and ends.
    fn two_batches(dir: &Path) -> (u64, u64) {
        let (mut s, _) = FileStorage::open_with_prealloc(dir, false, SMALL).unwrap();
        s.persist(&append(1, vec![entry(1, 1), entry(2, 1)], None))
            .unwrap();
        let mid = s.write_pos;
        s.persist(&append(3, vec![entry(3, 1)], None)).unwrap();
        (mid, s.write_pos)
    }

    #[test]
    fn torn_tail_is_discarded_and_scrubbed() {
        let dir = tempfile::tempdir().unwrap();
        let (mid, end) = two_batches(dir.path());
        // Simulate a crash in the middle of the second write: only part of the record
        // reached the disk, followed by stale garbage.
        let path = dir.path().join(WAL_FILE);
        let mut bytes = fs::read(&path).unwrap();
        for b in &mut bytes[(end - 5) as usize..end as usize + 64] {
            *b = 0xAB;
        }
        fs::write(&path, &bytes).unwrap();

        let (mut s, state) = FileStorage::open_with_prealloc(dir.path(), false, SMALL).unwrap();
        assert_eq!(state.entries.len(), 2);
        assert_eq!(s.write_pos, mid);
        // New appends land after the valid prefix, and the garbage beyond them is gone.
        s.persist(&append(3, vec![entry(3, 2)], None)).unwrap();
        drop(s);
        let after = fs::read(&path).unwrap();
        assert!(!after.windows(4).any(|w| w == [0xAB; 4]));
        let (_, state) = FileStorage::open_with_prealloc(dir.path(), false, SMALL).unwrap();
        assert_eq!(state.entries.len(), 3);
        assert_eq!(state.entries.last().unwrap().term, 2);
    }

    #[test]
    fn corrupt_record_stops_replay() {
        let dir = tempfile::tempdir().unwrap();
        let (_, end) = two_batches(dir.path());
        let path = dir.path().join(WAL_FILE);
        let mut bytes = fs::read(&path).unwrap();
        bytes[end as usize - 1] ^= 0xff; // flip bits in the second record's payload
        fs::write(&path, &bytes).unwrap();
        let (_, state) = FileStorage::open_with_prealloc(dir.path(), false, SMALL).unwrap();
        assert_eq!(state.entries.len(), 2);
    }

    #[test]
    fn grows_preallocated_file_as_needed() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (mut s, _) = FileStorage::open_with_prealloc(dir.path(), false, SMALL).unwrap();
            for i in 1..=500 {
                s.persist(&append(i, vec![entry(i, 1)], None)).unwrap();
            }
            assert!(s.allocated > SMALL && s.allocated >= s.write_pos);
            let len = fs::metadata(dir.path().join(WAL_FILE)).unwrap().len();
            assert_eq!(len, s.allocated);
        }
        let (_, state) = FileStorage::open_with_prealloc(dir.path(), false, SMALL).unwrap();
        assert_eq!(state.entries.len(), 500);
    }

    #[test]
    fn rewrite_replaces_contents_with_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let snapshot = Snapshot {
            meta: SnapshotMeta {
                last_index: 10,
                last_term: 2,
            },
            data: b"state".to_vec(),
        };
        let hs = HardState {
            term: 2,
            voted_for: Some(1),
        };
        {
            let (mut s, _) = FileStorage::open(dir.path(), true).unwrap();
            s.persist(&append(1, (1..=12).map(|i| entry(i, 2)).collect(), None))
                .unwrap();
            s.persist(&Persist::Rewrite {
                hard_state: hs,
                snapshot: snapshot.clone(),
                entries: vec![entry(11, 2), entry(12, 2)],
            })
            .unwrap();
            s.persist(&append(13, vec![entry(13, 2)], None)).unwrap();
        }
        let (_, state) = FileStorage::open(dir.path(), true).unwrap();
        assert_eq!(state.snapshot, snapshot);
        assert_eq!(state.hard_state, hs);
        let idx: Vec<_> = state.entries.iter().map(|e| e.index).collect();
        assert_eq!(idx, vec![11, 12, 13]);
    }

    #[test]
    fn mem_storage_matches_file_semantics() {
        let mut mem = MemStorage::new();
        mem.persist(&append(1, vec![entry(1, 1), entry(2, 1)], None))
            .unwrap();
        mem.persist(&append(2, vec![entry(2, 3)], None)).unwrap();
        let terms: Vec<_> = mem.state().entries.iter().map(|e| e.term).collect();
        assert_eq!(terms, vec![1, 3]);
    }
}
