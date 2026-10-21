//! In-memory view of the replicated log, including the compacted prefix (snapshot).

use super::types::{Entry, EntryPayload, LogIndex, Snapshot, Term};

/// A batch of log mutations that must be made durable.
///
/// Semantics: delete every stored entry with `index >= truncate_from`, then append
/// `entries` (which start at `truncate_from` when non-empty).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogWrite {
    pub truncate_from: LogIndex,
    pub entries: Vec<Entry>,
}

/// The Raft log: a snapshot covering `1..=snapshot.meta.last_index` followed by
/// contiguous in-memory entries.
#[derive(Debug)]
pub struct RaftLog {
    snapshot: Snapshot,
    /// `entries[i].index == snapshot.meta.last_index + 1 + i`.
    entries: Vec<Entry>,
    /// Lowest index whose durable copy may be stale, if any.
    unstable_from: Option<LogIndex>,
    /// Highest index known to be durable (the log up to here matches disk).
    stable_index: LogIndex,
    /// Last index of the most recent [`LogWrite`] handed out but not yet confirmed durable.
    handed_out: Option<LogIndex>,
}

impl RaftLog {
    /// Rebuilds a log from durable state. Entries already covered by the snapshot are dropped.
    pub fn new(snapshot: Snapshot, entries: Vec<Entry>) -> Self {
        let base = snapshot.meta.last_index;
        let entries: Vec<Entry> = entries.into_iter().filter(|e| e.index > base).collect();
        for (i, e) in entries.iter().enumerate() {
            assert_eq!(
                e.index,
                base + 1 + i as u64,
                "log entries must be contiguous"
            );
        }
        let stable_index = base + entries.len() as u64;
        Self {
            snapshot,
            entries,
            unstable_from: None,
            stable_index,
            handed_out: None,
        }
    }

    /// Highest index known to be durable. A leader counts itself towards a commit quorum
    /// only up to here, which lets it replicate entries before writing them locally.
    pub fn stable_index(&self) -> LogIndex {
        self.stable_index
    }

    /// Confirms that the last [`LogWrite`] returned by [`take_unstable`](Self::take_unstable)
    /// is durable.
    pub fn mark_stable(&mut self) {
        if let Some(h) = self.handed_out.take() {
            self.stable_index = h.max(self.snapshot_index());
        }
    }

    pub fn snapshot(&self) -> &Snapshot {
        &self.snapshot
    }

    pub fn snapshot_index(&self) -> LogIndex {
        self.snapshot.meta.last_index
    }

    /// Index of the first entry still held in memory.
    pub fn first_index(&self) -> LogIndex {
        self.snapshot_index() + 1
    }

    pub fn last_index(&self) -> LogIndex {
        self.snapshot_index() + self.entries.len() as u64
    }

    pub fn last_term(&self) -> Term {
        self.entries
            .last()
            .map_or(self.snapshot.meta.last_term, |e| e.term)
    }

    /// Term of the entry at `index`, or `None` if it is compacted away or beyond the end.
    /// The snapshot's last index is always answerable.
    pub fn term(&self, index: LogIndex) -> Option<Term> {
        let base = self.snapshot_index();
        if index == base {
            return Some(self.snapshot.meta.last_term);
        }
        self.entry(index).map(|e| e.term)
    }

    pub fn entry(&self, index: LogIndex) -> Option<&Entry> {
        let base = self.snapshot_index();
        if index <= base {
            return None;
        }
        self.entries.get((index - base - 1) as usize)
    }

    /// All entries held in memory (i.e. after the snapshot).
    pub fn entries(&self) -> &[Entry] {
        &self.entries
    }

    /// Up to `max` entries starting at `lo` (which must be `>= first_index`).
    pub fn entries_from(&self, lo: LogIndex, max: usize) -> Vec<Entry> {
        debug_assert!(lo >= self.first_index());
        let start = (lo - self.first_index()) as usize;
        if start >= self.entries.len() {
            return Vec::new();
        }
        let end = (start + max).min(self.entries.len());
        self.entries[start..end].to_vec()
    }

    /// Entries in `lo..=hi`, both of which must be held in memory.
    pub fn slice(&self, lo: LogIndex, hi: LogIndex) -> Vec<Entry> {
        if lo > hi {
            return Vec::new();
        }
        let first = self.first_index();
        assert!(lo >= first && hi <= self.last_index(), "slice out of range");
        self.entries[(lo - first) as usize..=(hi - first) as usize].to_vec()
    }

    /// Raft §5.4.1: is a log ending at (`last_index`, `last_term`) at least as up to date as ours?
    pub fn is_up_to_date(&self, last_index: LogIndex, last_term: Term) -> bool {
        (last_term, last_index) >= (self.last_term(), self.last_index())
    }

    /// Leader-side append of a fresh entry. Returns its index.
    pub fn append(&mut self, term: Term, payload: EntryPayload) -> LogIndex {
        let index = self.last_index() + 1;
        self.mark_unstable(index);
        self.entries.push(Entry {
            index,
            term,
            payload,
        });
        index
    }

    /// Follower-side append (Raft Figure 2, AppendEntries steps 3 and 4).
    ///
    /// Entries that already match are skipped, so stale or duplicated messages never
    /// truncate the log. A conflicting entry truncates the log from that point.
    /// `commit` is used only to assert that committed entries are never removed.
    pub fn append_entries(&mut self, entries: Vec<Entry>, commit: LogIndex) {
        for entry in entries {
            if entry.index <= self.snapshot_index() {
                continue; // Covered by the snapshot, therefore committed and matching.
            }
            if entry.index <= self.last_index() {
                if self.term(entry.index) == Some(entry.term) {
                    continue;
                }
                assert!(
                    entry.index > commit,
                    "attempted to truncate committed entry {} (commit {commit})",
                    entry.index
                );
                self.truncate_from(entry.index);
            }
            debug_assert_eq!(entry.index, self.last_index() + 1);
            self.mark_unstable(entry.index);
            self.entries.push(entry);
        }
    }

    fn truncate_from(&mut self, index: LogIndex) {
        let keep = (index - self.first_index()) as usize;
        self.entries.truncate(keep);
        self.mark_unstable(index);
        self.stable_index = self.stable_index.min(index - 1);
        self.handed_out = self.handed_out.map(|h| h.min(index - 1));
    }

    fn mark_unstable(&mut self, index: LogIndex) {
        self.unstable_from = Some(self.unstable_from.map_or(index, |u| u.min(index)));
    }

    /// Whether there are mutations not yet handed out by [`take_unstable`](Self::take_unstable).
    pub fn has_unstable(&self) -> bool {
        self.unstable_from.is_some()
    }

    /// Returns (and clears) the mutations made since the last call.
    pub fn take_unstable(&mut self) -> Option<LogWrite> {
        let from = self.unstable_from.take()?;
        self.handed_out = Some(self.last_index());
        let start = from.max(self.first_index());
        let entries = if start <= self.last_index() {
            self.slice(start, self.last_index())
        } else {
            Vec::new()
        };
        Some(LogWrite {
            truncate_from: from,
            entries,
        })
    }

    /// Discards entries up to and including `index`, replacing them with `data`.
    /// `index` must be held in memory (callers only compact applied entries).
    pub fn compact(&mut self, index: LogIndex, data: Vec<u8>) {
        if index <= self.snapshot_index() {
            return;
        }
        let term = self
            .term(index)
            .expect("compaction index must be in the log");
        let drop = (index - self.snapshot_index()) as usize;
        self.entries.drain(..drop);
        self.snapshot.meta.last_index = index;
        self.snapshot.meta.last_term = term;
        self.snapshot.data = data;
        self.stable_index = self.stable_index.max(index);
    }

    /// Installs a snapshot received from the leader (Raft Figure 13, steps 6 and 7).
    ///
    /// If the log contains an entry matching the snapshot's last index and term, the
    /// suffix after it is retained; otherwise the whole log is discarded.
    pub fn install_snapshot(&mut self, snapshot: Snapshot) {
        let idx = snapshot.meta.last_index;
        debug_assert!(idx > self.snapshot_index());
        if self.term(idx) == Some(snapshot.meta.last_term) {
            let drop = (idx - self.snapshot_index()) as usize;
            self.entries.drain(..drop);
        } else {
            self.entries.clear();
            self.mark_unstable(idx + 1);
            self.stable_index = self.stable_index.min(idx);
            self.handed_out = self.handed_out.map(|h| h.min(idx));
        }
        self.snapshot = snapshot;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn e(index: LogIndex, term: Term) -> Entry {
        Entry {
            index,
            term,
            payload: EntryPayload::Noop,
        }
    }

    fn log_with(terms: &[Term]) -> RaftLog {
        let entries = terms
            .iter()
            .enumerate()
            .map(|(i, t)| e(i as u64 + 1, *t))
            .collect();
        RaftLog::new(Snapshot::default(), entries)
    }

    #[test]
    fn term_lookup_and_bounds() {
        let log = log_with(&[1, 1, 2]);
        assert_eq!(log.term(0), Some(0));
        assert_eq!(log.term(3), Some(2));
        assert_eq!(log.term(4), None);
        assert_eq!(log.last_index(), 3);
        assert_eq!(log.last_term(), 2);
    }

    #[test]
    fn append_entries_skips_matching_and_truncates_conflicts() {
        let mut log = log_with(&[1, 1, 2, 2]);
        // A duplicate of an old message must not truncate.
        log.append_entries(vec![e(2, 1)], 0);
        assert_eq!(log.last_index(), 4);
        assert!(log.take_unstable().is_none());

        // Conflict at index 3: truncate and replace.
        log.append_entries(vec![e(3, 3), e(4, 3), e(5, 3)], 2);
        assert_eq!(log.last_index(), 5);
        assert_eq!(log.term(3), Some(3));
        let w = log.take_unstable().unwrap();
        assert_eq!(w.truncate_from, 3);
        assert_eq!(w.entries.len(), 3);
    }

    #[test]
    #[should_panic(expected = "truncate committed")]
    fn refuses_to_truncate_committed_entries() {
        let mut log = log_with(&[1, 1, 2]);
        log.append_entries(vec![e(2, 5)], 2);
    }

    #[test]
    fn compaction_preserves_terms_at_boundary() {
        let mut log = log_with(&[1, 2, 3, 4]);
        log.compact(2, b"state".to_vec());
        assert_eq!(log.first_index(), 3);
        assert_eq!(log.term(2), Some(2));
        assert_eq!(log.term(1), None);
        assert_eq!(log.last_index(), 4);
        assert_eq!(log.entries_from(3, 10).len(), 2);
    }

    #[test]
    fn install_snapshot_retains_matching_suffix() {
        let mut log = log_with(&[1, 1, 2, 2]);
        log.install_snapshot(Snapshot {
            meta: super::super::types::SnapshotMeta {
                last_index: 2,
                last_term: 1,
            },
            data: vec![],
        });
        assert_eq!(log.last_index(), 4);
        assert!(log.take_unstable().is_none());
    }

    #[test]
    fn install_snapshot_discards_conflicting_log() {
        let mut log = log_with(&[1, 1, 2, 2]);
        log.install_snapshot(Snapshot {
            meta: super::super::types::SnapshotMeta {
                last_index: 3,
                last_term: 3,
            },
            data: vec![],
        });
        assert_eq!(log.last_index(), 3);
        assert_eq!(log.last_term(), 3);
        let w = log.take_unstable().unwrap();
        assert_eq!(w.truncate_from, 4);
        assert!(w.entries.is_empty());
    }

    #[test]
    fn stable_index_tracks_durability_and_truncation() {
        let mut log = log_with(&[1, 1]);
        assert_eq!(log.stable_index(), 2);
        log.append(2, EntryPayload::Noop);
        assert_eq!(log.stable_index(), 2);
        let _ = log.take_unstable();
        log.mark_stable();
        assert_eq!(log.stable_index(), 3);
        // A conflicting append truncates; the durable prefix shrinks with it.
        log.append_entries(vec![e(2, 3)], 1);
        assert_eq!(log.stable_index(), 1);
        let _ = log.take_unstable();
        log.mark_stable();
        assert_eq!(log.stable_index(), 2);
    }

    #[test]
    fn up_to_date_compares_term_then_index() {
        let log = log_with(&[1, 2]);
        assert!(log.is_up_to_date(2, 2));
        assert!(log.is_up_to_date(1, 3));
        assert!(!log.is_up_to_date(5, 1));
        assert!(!log.is_up_to_date(1, 2));
    }
}
