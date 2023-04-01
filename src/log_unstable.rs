//! A representation of not-yet-commited log entries and state.

use slog::Logger;

use crate::{eraftpb::{Snapshot, Entry}, util::entry_approximate_size};


/// The `unstable.entries[i]` has raft log position `i + unstable.offset`. Note that `unstable.offset` may be less than the highest log position in storage might need
/// to truncate the log before persisting untable.entries.
#[derive(Debug)]
pub struct Unstable{
    /// The incoming unstable snapshot, if any.
    pub snapshot: Option<Snapshot>,
    /// All entries that have not yet been written to storage.
    pub entries: Vec<Entry>,
    /// The size of entries.
    pub entries_size: usize,
    /// The offset from the vector index.
    pub offset: u64,
    /// The tag to use when logging
    pub logger: Logger,
}

impl Unstable {
    /// Creates a new log of unstabe entries.
    pub fn new(offset: u64, logger: Logger) -> Self {
        Unstable { snapshot: None, entries: vec![], entries_size: 0, offset, logger }
    }

    /// Returns the index of the first possible entry in entries if it has a sanpshot.
    pub fn maybe_first_index(&self) -> Option<u64>{
        self.snapshot.as_ref().map(|snap| snap.get_metadata().index + 1)
    }

    /// Returns the last index if it has a least one unsatble entry or snapshot.
    pub fn maybe_last_index(&self) -> Option<u64>{
        match self.entries.len() {
            0 => self.snapshot.as_ref().map(|snap| snap.get_metadata().index),
            len => Some(self.offset + len as u64 - 1),
        }
    }

    /// Returns the term of the entry at index idx, if there is any.
    pub fn maybe_term(&self, idx: u64) -> Option<u64>{
        if idx < self.offset {
            let snapshot = self.snapshot.as_ref()?;
            let meta = snapshot.get_metadata();
            if idx == meta.index {
                Some(meta.term)
            } else {
                None
            }
        } else {
            self.maybe_last_index().and_then(|last| {
                if idx > last {
                    return None;
                }
                Some(self.entries[(idx - self.offset) as usize].term)
            })
        }
    }

    /// Clears the unsatble entries and moves the satble offset up to the last index, if there is any.
    pub fn stable_entries(&mut self, index: u64, term: u64){
        // The snapshot must be stable before entries
        assert!(self.snapshot.is_none());
        if let Some(entry) = self.entries.last()  {
            if entry.get_index() != index || entry.get_term() != term{
                fatal!(self.logger, "the last one of unsatble.slince has different index {} and term {}, expect {} {}", entry.get_index(), entry.get_term(), index, term);
            }
            self.offset = entry.get_index() + 1;
            self.entries.clear();
            self.entries_size = 0;
        } else {
            fatal!(self.logger, "unsatble.slice is empty, expect its last one's index and term are {} and {}", index, term);
        }
    }

    /// Clear the unsatble snapshot.
    pub fn stable_snap(&mut self, index: u64){
        if let Some(snap) = &self.snapshot {
            if snap.get_metadata().index != index {
                fatal!(self.logger, "unstable.snap has different index {}, expect {}", snap.get_metadata().index, index);
            }
            self.snapshot = None;
        } else {
            fatal!(self.logger, "unstable.snap is none, expect a snapshot with index {}", index);
        }
    }

    /// From a given snapshot, restores the snapshot to self, but doesn't unpack(解压缩).
    pub fn restore(&mut self, snap: Snapshot){
        self.entries.clear();
        self.entries_size = 0;
        self.offset = snap.get_metadata().index + 1;
        self.snapshot = Some(snap);
    }

    /// Append entries to unstable, truncate local block first if overlapped. Panics if truncate logs to the entry before snapshot.
    pub fn truncate_and_append(&mut self, ents: &[Entry]) {
        let after = ents[0].index;
        if after == self.offset + self.entries.len() as u64 {
            
        } else if after <= self.offset {
            self.offset = after;
            self.entries.clear();
            self.entries_size = 0;
        } else {
            let off = self.offset;
            self.must_check_outofbounds(off, after);
            for e in &self.entries[(after - off) as usize..] {
                self.entries_size -= entry_approximate_size(e);
            }
            self.entries.truncate((after - off) as usize);
        }
        self.entries.extend_from_slice(ents);
        self.entries_size += ents.iter().map(entry_approximate_size).sum::<usize>();
    }

    /// Returns a slice of entries between the high and low. Panics if the `hi` or `lo` are out of buonds. Panics if `lo > hi`
    pub fn slice(&self,lo: u64, hi: u64) -> &[Entry] {
        self.must_check_outofbounds(lo, hi);
        let l = lo as usize;
        let h = hi as usize;
        let off = self.offset as usize;
        &self.entries[l - off..h-off]
    }

    /// Asserts the `hi` or `lo` values against each other and against the entries themselves.
    pub fn must_check_outofbounds(&self, lo: u64, hi: u64) {
        if lo > hi {
            fatal!(self.logger, "invalid unstable.slice {} > {}", lo, hi);
        }
        let upper = self.offset + self.entries.len() as u64;
        if lo < self.offset || hi > upper {
            fatal!(self.logger, "unsatble.slice[{}, {}] out of bound[{},{}]", lo, hi, self.offset, upper);
        }
    }
}


