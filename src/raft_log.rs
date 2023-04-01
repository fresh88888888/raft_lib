use std::cmp;

use raft_proto::prelude::Snapshot;
use slog::{Logger, info, warn, trace, debug};

use crate::storage::GetEntriesFor;
use crate::{GetEntriesContext, util};
use crate::{log_unstable::Unstable, Storage, Result, StorageError, Error};
use  crate::eraftpb::{Entry};

/// Raft log implementation
pub struct RaftLog<T: Storage> {
    // Contains all stable entries since the last snapshot.
    pub store: T,
    // Contains all unsatble entries and snapshot. they will be saved into storage.
    pub unstable: Unstable,
    /// The highest log position that is known to be in stable storage on a quorum of nodes. Invariant: applied <= committed
    pub committed: u64,
    // The highest log position that is known to be persisted in satble storage. it's used for limiting the upper bound of commited and persisted entries.
    // Invariant: persisted < unstable.offset && applied <= persisted
    pub persisted: u64,
    // The highest log position that the application has been instructed to apply to its state machine. Invariant: applied <= min(committed, persisted)
    pub applied: u64,
}

impl<T> ToString for RaftLog<T>
where
    T: Storage,
{
    fn to_string(&self) -> String {
        format!(
            "commited={}, persisted={}, applied={}, unstable.offset={}, unsatble.entries.len()={}",
            self.committed,
            self.persisted,
            self.applied,
            self.unstable.offset,
            self.unstable.entries.len()
        )
    }
}

impl <T: Storage> RaftLog<T> {
    /// Creates a new raft log with a given storage and tag.
    pub fn new(store: T, logger: Logger) -> RaftLog<T> {
        let first_index = store.first_idx().unwrap();
        let last_index = store.last_idx().unwrap();
        // Initialize committed and applied pointers to the time of the last compaction.
        RaftLog { store, unstable: Unstable::new(last_index + 1, logger), committed: first_index - 1, persisted: last_index, applied: first_index - 1}
    }

    /// Grabs(获取) the term from the last entry. Panics if there are entries but the last term has been discarded.
    pub fn last_term(&self) -> u64{
        match self.term(self.last_index()) {
            Ok(t) => t,
            Err(e) => fatal!(self.unstable.logger, "unexpected error when getting the last term: {:?}", e),
        }
    }

    pub fn last_index(&self) -> u64 {
        match self.unsatble().maybe_last_index() {
            Some(idx) => idx,
            None => self.store.last_idx().unwrap(),
        }
    }
    /// Grab a read-only reference to the underlying storage.
    #[inline]
    pub fn store(&self) -> &T {
        &self.store
    }

    /// Grab a mutable reference to the underlying storage.
    #[inline]
    pub fn mut_store(&mut self) -> &mut T {
        &mut self.store
    }

    /// For a given index, finds the term associated with it.(对于给定的索引，查找与之相关的项)
    pub fn term(&self, idx:u64) -> Result<u64> {
        // the valid term range is [index of dummy entry , last_index]
        let dummy_idx = self.first_idx() - 1;
        if idx < dummy_idx || idx > self.last_idx() {
            return Ok(0u64);
        }

        match self.unstable.maybe_term(idx) {
            Some(term) => Ok(term),
            _ => self.store.term(idx).map_err(|e| {
                match e {
                    Error::Store(StorageError::Compacted) | Error::Store(StorageError::Unavailable) => {},
                    _ => fatal!(self.unstable.logger, "unexpected error: {:?}", e),
                }
                e
            }),
        }

    }

    /// Returns the first index in the store that is avaviable via entries. Panics if the store dosen't have a first index.
    pub fn first_idx(&self) -> u64{
        match self.unstable.maybe_first_index() {
            Some(idx) => idx,
            None => self.store.first_idx().unwrap(),
        }
    }

    /// Returns the last index in the store that is avaiable via entries. Panics if the store doesn't have a last index.
    pub fn last_idx(&self) -> u64 {
        match self.unstable.maybe_last_index() {
            Some(idx) => idx,
            None => self.store.last_idx().unwrap(),
        }
    }

    /// Finds the index of the conflict(冲突).
    /// It returns the first index of conflicting entries between the existing entries and the given enteries, if there are any.
    /// If there are no conflicting entries, and the existing entries contain all the given entries, zero will be returned.
    /// If there are no conflicting entries, but the given entries contains new entries, the index of the first new entry will be returned.
    /// An entry is considered to be conflicting if it has the same index but a different term.
    /// The first entry MUST have an index equal to the argument `from` . The index of the given entries MUST be continuously increasing. 
    pub fn find_conflict(&self, ents: &[Entry]) -> u64{
        for e in ents {
            if !self.match_term(e.index, e.term) {
                if e.index <= self.last_idx() {
                    info!(self.unstable.logger, "found conflict at index {index}", index = e.index; "existing term" => self.term(e.index).unwrap_or(0), "conflicting term" => e.term,);
                }
                return e.index;
            }
        }
        0
    }

    /// Answers the question: Does this index belong to this term?
    pub fn match_term(&self, idx: u64, term: u64) -> bool {
        self.term(idx).map(|t| t == term).unwrap_or(false)
    }

    /// find_conflict_by_term takes an (`index`, `term`) pair (indicating a confflicting log entry on a leader/follower during an append) and finds the largest index in log with log.term <= `term`
    /// and log.index <= `index`. If no such index exists in the log, the log's first index is returned. The index provided MUST be equal to or less than self.last_index(). Invaid inputs log a 
    /// warning and the input index is returned. Return (index, term)
    pub fn find_conflict_by_term(&self, index: u64, term: u64) -> (u64, Option<u64>){
        let mut conflict_index = index;

        let last_idx = self.last_idx();
        if index > last_idx {
            warn!(self.unstable.logger, "index({}) is out of range [0, last_index({})] in find_conflict_by_term", index, last_idx);
            return (index, None);
        }

        loop {
            match self.term(conflict_index) {
                Ok(t) => {
                    if t > term {
                        conflict_index -= 1;
                    } else {
                        return (conflict_index, Some(t));
                    }
                },
                Err(_) => return (conflict_index, None),
            }
        }
    }

    /// Returns None if the entries cannot be appened. Otherwise. it returns Some((conflix_index, last_new_index))
    pub fn maybe_append(&mut self, idx: u64, term: u64, commited: u64, ents: &[Entry]) -> Option<(u64, u64)> {
        if self.match_term(idx, term) {
            let conflict_index = self.find_conflict(ents);
            if conflict_index == 0 {
            } else if conflict_index <= self.committed {
                fatal!(self.unstable.logger, "entry {} conflict with commited entry {}", conflict_index, self.committed);
            } else {
                let start = (conflict_index - (idx + 1)) as usize;
                self.append(&ents[start..]);
                // persisted should be decreased because entries are changed
                if self.persisted > conflict_index - 1 {
                    self.persisted = conflict_index - 1;
                }
            }
            let last_new_index = idx + ents.len() as u64;
            self.commit_to(cmp::min(commited, last_new_index));
            return Some((conflict_index, last_new_index));
        }
        None
    }

    /// Sets the last commited value to passed in value. Panics if the index goes pass the last index.
    pub fn commit_to(&mut self, to_commit: u64) {
        // never decrese commit
        if self.committed >= to_commit {
            return;
        }
        if self.last_idx() < to_commit {
            fatal!(self.unstable.logger, "to_commit {} is out of range [last_idex {}]", to_commit, self.last_idx())
        }
        self.committed = to_commit;
    }

    /// Appends a set of entries to the unstable list
    pub fn append(&mut self, ents: &[Entry]) -> u64 {
        trace!(self.unstable.logger, "Entries being appended to unstable list"; "ents" => ?ents);
        if ents.is_empty() {
            return self.last_idx();
        }
        
        let after = ents[0].index - 1;
        if after < self.committed {
            fatal!(self.unstable.logger, "after {} is out of range [commited {}]", after, self.committed)
        } 
        self.unstable.truncate_and_append(ents);
        self.last_idx()
    }

    pub fn applied(&self) -> u64{
        self.applied
    }

    pub fn stable_entries(&mut self, index: u64, term: u64){
        self.unstable.stable_entries(index, term);
    }

    pub fn satble_snap(&mut self, index: u64){
        self.unstable.stable_snap(index);
    }

    pub fn unsatble(&self) -> &Unstable{
        &self.unstable
    }

    pub fn unstable_entries(&self) -> &[Entry]{
        &self.unstable.entries
    }

    pub fn unsatble_snap(&self) -> &Option<Snapshot> {
        &self.unstable.snapshot
    }

    /// Returns entries starting from a particular index and not exceeding a byteesize
    pub fn entries(&self, idx: u64, max_size: impl Into<Option<u64>>, context: GetEntriesContext) -> Result<Vec<Entry>>{
        let max_size = max_size.into();
        let last = self.last_idx();
        if idx > last {
            return Ok(Vec::new());
        }
        self.slice(idx, last + 1, max_size, context)
    }

    /// Grabs a slice of entries from the raft. Unlike a rust slice pointer, these are returned by value. The result is truncated to the max_size in bytes.
    pub fn slice(&self, low: u64, high: u64, max_size: impl Into<Option<u64>>, context: GetEntriesContext) -> Result<Vec<Entry>>{
        let max_size = max_size.into();
        if let Some(err) = self.must_check_outofbounds(low, high) {
            return Err(err);
        }
        let mut ents = vec![];
        if low == high {
            return Ok(ents);
        }

        if low < self.unstable.offset {
            let unstable_high = cmp::min(high, self.unstable.offset);
            match self.store.entries(low, unstable_high, max_size, context) {
                Err(e) => match e {
                    Error::Store(StorageError::Compacted) | Error::Store(StorageError::LogTemporarilyUnavailable) => return Err(e),
                    Error::Store(StorageError::Unavailable) => fatal!(self.unsatble().logger, "entries[{},{}] is unavailable from storage", low, unstable_high),
                    _ => fatal!(self.unsatble().logger, "unexpected error: {:?}", e),
                },
                Ok(entries) => {
                    ents = entries;
                    if (ents.len() as u64) < unstable_high - low {
                        return Ok(ents);
                    }
                }
            }
        }

        if high > self.unsatble().offset {
            let offset = self.unsatble().offset;
            let unstable = self.unstable.slice(cmp::min(low, offset), high);
            ents.extend_from_slice(unstable);
        }
        util::limit_size(&mut ents, max_size);
        Ok(ents)
    }

    fn must_check_outofbounds(&self, low: u64, high: u64) -> Option<Error> {
        if low > high {
            fatal!(self.unsatble().logger, "invalid slice {} > {}", low, high)
        }
        let first_idx = self.first_idx();
        if low < first_idx {
            return Some(Error::Store(StorageError::Compacted));
        }
        let length = self.last_idx() + 1 - first_idx;
        if low < first_idx || high > first_idx + length {
            fatal!(self.unsatble().logger, "slice[{},{}] out of bound[{},{}]", low, high, first_idx, self.last_idx())
        }
        None
    }

    /// Returns all the entries, Only used by tests.
    #[doc(hidden)]
    pub fn all_entries(&self) -> Vec<Entry>{
        let first_idx = self.first_idx();
        match self.entries(first_idx, None, GetEntriesContext::empty(false)) {
            Err(e) => {
                // try again if there was a racing compaction
                if e == Error::Store(StorageError::Compacted) {
                    return self.all_entries();
                }
                fatal!(self.unsatble().logger, "unexpected Error: {:?}", e)
            },
            Ok(ents) => ents,
        }
    }

    /// Determines if the given (lastIndex, term) log is more up-to-date by comparing the index and term of the last entry in the existing logs. If the logs have last entry with different terms, than
    /// the log with the later term is more up-to-date. If the logs end with the same term, then wichever log has the larger last_index is more up-to-date. If the logs are the same, The given log is 
    /// up-to-date. 
    pub fn is_up_to_date(&self, last_idx: u64, term: u64) -> bool {
        term > self.last_term() || (term == self.last_term() && last_idx >= self.last_idx())
    }

    /// Returns commited and persisted(持久化) entries since max(`since_idx` + 1, first_index)
    pub fn next_entries_since(&self, since_index: u64, max_size: Option<u64>) -> Option<Vec<Entry>> {
        let offset = cmp::max(since_index + 1, self.first_idx());
        let high = cmp::max(self.committed, self.persisted) + 1;
        if high > offset {
            match self.slice(offset, high, max_size, GetEntriesContext(GetEntriesFor::GenReady)) {
                Ok(vec) => return Some(vec),
                Err(e) => fatal!(self.unsatble().logger, "{}", e),
            }
        }
        None
    }

    /// Returns all the available entries for execution. If applied is smaller than the index of sanpshot, It returned all commited entries after the index of snapshot.
    pub fn next_entries(&self, max_size: Option<u64>) -> Option<Vec<Entry>> {
        self.next_entries_since(self.applied, max_size)
    }

    /// Returns whether there are commited and persisted entries since max(`since_index` + 1, first_idx)
    pub fn has_next_entries_since(&self, since_index: u64) -> bool{
        let offset = cmp::max(since_index + 1, self.first_idx());
        let high = cmp::min(self.committed, self.persisted) + 1;
        high > offset
    }

    /// Returns whether there are new entries
    pub fn has_next_entries(&self) -> bool {
        self.has_next_entries_since(self.applied)
    }

    /// Returns the current snapshot
    pub fn snapshot(&self, request_idx: u64, to: u64) -> Result<Snapshot>{
        if let Some(snap) = self.unsatble().snapshot.as_ref() {
            if snap.get_metadata().index >= request_idx {
                return Ok(snap.clone());
            }
        }
        self.store.snapshot(request_idx, to)
    }

    pub(crate) fn pending_snapshot(&self) -> Option<&Snapshot> {
        self.unstable.snapshot.as_ref()
    }

    /// Attempts(尝试) to commit the index and term and returns whether it did. 
    pub fn maybe_commit(&mut self, max_index: u64, term: u64) -> bool{
        if max_index > self.committed && self.term(max_index).map_or(false, |t| t == term) {
            debug!(self.unstable.logger, "commiting index {index}", index = max_index);
            self.commit_to(max_index);
            true
        } else {
            false
        }
    }

    /// Attempts to persisted the index and term and returns whether it did.
    pub fn maybe_persist(&mut self, index: u64, term: u64) -> bool {
        // It's possible that the term check can be passed but index is greater than or equal to the first_update_index in some corner cases. For example, there are 5 nodes, A B C D E.
        // 1. A is leader and it proposes some raft logs but only B receives these logs.
        // 2. B gets the Ready and the logs are persisted asynchronously.
        // 2. A crashes and C becomes leader after getting the vote from D and E.
        // 3. C proposes some raft logs and B receives these logs.
        // 4. C crashes and A restarts and becomes leader again after getting the vote from D and E.
        // 5. B receives the logs from A which are the same to the ones from step 1.
        // 6. The logs from Ready has been persisted on B so it calls on_persist_ready and comes to here.
        //
        // We solve this problem by not forwarding the persisted index. It's pretty intuitive because the first_update_index means there are snapshot or some entries whose indexes are greater 
        // than or equal to the first_update_index have not been persisted yet.
        let first_update_index = match &self.unstable.snapshot {
            Some(s) => s.get_metadata().index,
            None => self.unstable.offset,
        };
        if index > self.persisted && index < first_update_index && self.store.term(index).map_or(false, |t| t == term){
            debug!(self.unstable.logger, "persisted index {}", index);
            self.persisted = index;
            true
        } else {
            false
        }
    }

    /// Attempts to persist the snapshot and returns whether it did.
    pub fn maybe_persist_snap(&mut self, index: u64) -> bool {
        if index > self.persisted {
            // commit index should not be less than snapshot's index.
            if index > self.committed {
                fatal!(self.unstable.logger, "snapshot's index {} > commited {}", index, self.committed)
            }

            // All of the indexes of later entries must be greater than snapshot's index
            if index >= self.unstable.offset {
                fatal!(self.unstable.logger, "snapshot's index {} >= offset {}", index, self.unstable.offset)
            }
            
            debug!(self.unstable.logger, "snapshot's persisted index {}", index);
            self.persisted = index;
            true
        } else {
            false
        }
    }

    /// Restores the current log from a snapshot.(从快照恢复当前日志)
    pub fn restore(&mut self, snapshot: Snapshot) {
        info!(self.unstable.logger, "log [{log}] starts to restore snapshot [index:{snapshot_index}, term:{snapshot_term}]", log = self.to_string(), 
        snapshot_index = snapshot.get_metadata().index,
        snapshot_term = snapshot.get_metadata().term);
        let index = snapshot.get_metadata().index;
        assert!(index >= self.committed, "{} < {}", index, self.committed);
        // If `persisted` is greater than `committed`, reset it to `committed`. It's because only the persisted entries whose index are less than `commited` can be considered the same as the data 
        // from snapshot. Although there may be some persisted entries with greater index are also committed, we can not judge them nor do we care about them because these entries can not be applied
        // thus the invariant which is `applied` <= min(`persisted`, `committed`) is satisfied.
        if self.persisted > self.committed {
            self.persisted = self.committed;
        }
        self.committed = index;
        self.unstable.restore(snapshot)
    }

    /// Returns the commited index and its terms.
    pub fn commit_info(&self) -> (u64, u64) {
        match self.term(self.committed) {
            Ok(t) => (self.committed, t),
            Err(e) => fatal!(self.unstable.logger, "last commited entry at {} is missing: {:?}", self.committed, e),
        }
    }

}