
//! Repersents the storage trait and example implementation
//! 
//! The storage traitis used to house and eventually serialize the state of the system .
//! Customer implementations of this are normal and this is likely to be a key intergation
//! point for your distribbution storage.

use getset::{Getters, Setters};
use std::cmp;
use std::cmp::Ordering;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::StorageError;
use crate::Error;
use crate::eraftpb::*;
use crate::Result;
use crate::util::limit_size;


/// Holds both the hard state (commit index, vote leader, term) and the configuration state(Current node IDs)
#[derive(Debug, Clone, Default, Getters, Setters)]
pub struct RaftState {
    /// Contains last meta information including commit index , the vote leader, and the vote term.
     pub hard_state: HardState,
    /// Record the current node IDs like `[1,2,3]` in the cluster . Every raft node must have a unique ID in the cluster. 
    pub conf_state: ConfState,
}

impl RaftState {
    /// Create a new RaftSate
    pub fn new(hard_state: HardState, conf_state: ConfState) -> RaftState{
        RaftState { hard_state, conf_state}
    }

    /// Indicates(指示) the `RaftState` is initialized or not.
    pub fn initilize(&self) -> bool {
        self.conf_state != ConfState::default()
    }
}

/// Records the context of the caller who calls entries() of Storages trait.
#[derive(Debug)]
pub struct GetEntriesContext(pub(crate) GetEntriesFor);

impl GetEntriesContext {
    /// Used for callers out of raft. Caller can customerizeif it supports async
    pub fn empty(can_async: bool) -> Self{
        GetEntriesContext(GetEntriesFor::Empty(can_async))
    }

    /// Check if the caller's context support fetching entries asynchronously.(检查调用者的上下文是否支持异步获取条目)
    pub fn can_async(&self) -> bool{
        match self.0 {
            GetEntriesFor::SendAppend {..}=> true,
            GetEntriesFor::Empty(can_async) => can_async,
            _ => false,
        }
    }
}

#[derive(Debug)]
pub(crate) enum GetEntriesFor {
    // for sending entries to followers
    SendAppend{
        // the peer id to which entriesare going to send
        to: u64,
        // the term when request is issued()
        term: u64,
        // whether to exhaust all the entries(是否穷尽所有条目)
        aggressively: bool,
    },
    // for geting commited entries in a ready
    GenReady,
    // for getting entries to check pending conf when transferring leader
    TransferLeader,
    // for getting entries to check pending conf when forwaring commit index by vote messages
    CommitByVote,
    // it's not called by the raft itself.
    Empty(bool),
}
/// Storage saves all the information about the current Raft implementation, including raft Log, commit index , The leader to vote for, etc.
/// 
/// If any Storage method returns an error. The raft instance will become inoperable(无法运行) and refause to participate(参与) in elections; 
/// The application is responsible(负责) for cleanup recovery(清理恢复) in this case.
pub trait Storage{
    /// `initial_state` is called when Raft is initialized. this interface will return a `RaftSate` which contains `HardState` and `ConfState`.
    /// `RaftState could be initialized or not, If it's initialized it means the `Storage` is created with a configuration ，And its last index  
    /// and term should be greater than 0.  
    fn initial_state(&self) -> Result<RaftState> ;

    /// Returns a slice of log entries in the range `[low, high)`. max_size limit the total size of the log entries returned if not `None`, however 
    /// The slice of entries returned will always have length at least 1 if entries are found in the range.  
    /// 
    /// Entries are supported to be fetched asynchronously depending on the conetxt, Async is optional, Storage should check context.can_async() first and 
    /// decide whether to fetch entries asynchronously base on its own imolementation, If the entries are fetched asynchronously, Storage should return 
    /// LogTemporarilyUnavaiable, and application need to call `on_enries_fetched(context)` to trigger re-fetch of the entries after the storage finishes 
    /// fetching the entries.  
    fn entries(&self, low: u64, high: u64, max_size: impl Into<Option<u64>>, context: GetEntriesContext) -> Result<Vec<Entry>>;

    /// returns the term of entries index , witch must be in the range [first_index -1, last_index]. The term of the entry before first_index is retained for 
    /// matchig purpose even through  the rest of that entry may not be availabble. 
    fn term(&self, idx: u64) -> Result<u64>;

    /// Returns the index of the first log entry is possibale available via entries which will always equal to truncated index plus 1, new create `Storage` can
    /// be considered as truncated at 0 so that 1 will be returned ib this case.
    fn first_idx(&self) -> Result<u64>;

    /// The index of the last entry replicatedin the `Storage`
    fn last_idx(&self) -> Result<u64>;

    /// Returns the most recent snapshot. if sanpshou If snapshot is temporarily unavaiable, it should return SnapshotTemporarilyUnavailable, so raft state 
    /// machinecould known that Storage needs some time to prepare A snapshot's index must not less than the `request_index`  `to` indicates which peer is 
    /// requesting the snapshot. 
    fn snapshot(&self, request_idx: u64, to: u64) ->Result<Snapshot>;
}

/// The memory storage core instance holds the actual state of the storage struct. To access this value, use the `rl` and `wl` functions on the main MemStorage 
/// implementation.
#[derive(Default)]
pub struct MemStorageCore {
    raft_state: RaftState,
    // entries[i] has raft log position i + snapshot.get_metadata().index
    entries: Vec<Entry>,
    // Meatedata of the last snapshot received.
    snapshot_metadata: SnapshotMetadata,
    // If ti is true , the next snapshot will return a SnapshotTemporarilyUnavailable error.
    trigger_snap_unavailable: bool,
    // Peers that are fetching entries asynchronously.
    trigger_log_unavailable: bool,
    // Stores get entries context.
    get_entries_context: Option<GetEntriesContext>,
}

impl MemStorageCore {
    /// Save the current HardState.
    pub fn set_hardstate(&mut self, hs: HardState) {
        self.raft_state.hard_state = hs;
    }
    /// Get the hard state
    pub fn hard_state(&self) -> &HardState{
        &self.raft_state.hard_state
    }
    /// Get the mut hard state
    pub fn  mut_hard_state(&mut self) -> &mut HardState{
        &mut self.raft_state.hard_state
    }

    /// Commit to an index
    /// # Panics 
    /// Panics if there is no such entry in raft logs 
    pub fn commit_to(&mut self, index: u64) -> Result<()> {
        assert!(self.has_entry_at(index), "commit_to {} but the entry does not exist", index);

        let diff = (index - self.entries[0].index) as usize;
        self.raft_state.hard_state.commit = index;
        self.raft_state.hard_state.term = self.entries[diff].term;
        Ok(())
    }

    /// Save the current conf state
    pub fn set_conf_state(&mut self, cs: ConfState) {
        self.raft_state.conf_state = cs;
    }

    fn has_entry_at(&self, index: u64) -> bool {
        !self.entries.is_empty() && index >= self.first_index() && index <= self.last_index()
    }

    fn first_index(&self) -> u64 {
        match self.entries.first() {
            Some(e) => e.index,
            None => self.snapshot_metadata.index + 1,
        }
    }

    fn last_index(&self) -> u64 {
        match self.entries.last() {
            Some(e) => e.index,
            None => self.snapshot_metadata.index,
        }
    }

    /// Overwrites the contexts of this storage object with those of the given snapshot
    /// 
    /// # Panics 
    /// Panics if the snapshot index is less than the storage's first index
    pub fn apply_snapshot(&mut self, mut snapshot: Snapshot) -> Result<()> {
        let mut meta = snapshot.take_metadata();
        let index = meta.index;

        if self.first_index() > index {
            return Err(crate::Error::Store(StorageError::SnapshotOutOfDate));
        }

        self.snapshot_metadata = meta.clone();
        self.raft_state.hard_state.term = cmp::max(self.raft_state.hard_state.term, meta.term);
        self.raft_state.hard_state.commit = index;
        self.entries.clear();

        // update conf state
        self.raft_state.conf_state = meta.take_conf_state();
        Ok(())
    }

    fn snapshot(&self) -> Snapshot {
        let mut snapshot = Snapshot::default();

        // we assume all entries whole indexes are less than `hard_state.commit` have been applied, so use the latest commit index to construct the snapshot.
        let meta = snapshot.mut_metadata();
        meta.index = self.raft_state.hard_state.commit;
        meta.term = match meta.index.cmp(&self.snapshot_metadata.index) {
            Ordering::Equal => self.snapshot_metadata.term,
            Ordering::Greater => {
                let offset = self.entries[0].index;
                self.entries[(meta.index - offset) as usize].term
            },
            Ordering::Less => {
                panic!("commit {} < snapshot_metadata.index {}", meta.index, self.snapshot_metadata.index);
            },
        };
        meta.set_conf_state(self.raft_state.conf_state.clone());
        snapshot
    }

    /// Discards all log entries prior to compact_index. It is the application's responsibility to not attempt to compact an index greater than RaftLog.applied.
    ///
    /// # Panics
    ///
    /// Panics if `compact_index` is higher than `Storage::last_index(&self) + 1`.
    pub fn compact(&mut self, compact_index: u64) -> Result<()> {
        if compact_index <= self.first_index() {
            // Don't need to treat this case as an error.
            return Ok(());
        }

        if compact_index >  self.last_index() + 1 {
            panic!("compact not received raft log {}, last index {}", compact_index, self.last_index());
        }

        if let Some(entry) = self.entries.first(){
            let offset = compact_index - entry.index;
            self.entries.drain(..offset as usize);
        }
        Ok(())
    }

    /// Append the new entries to storage.
    ///
    /// # Panics
    ///
    /// Panics if `ents` contains compacted entries, or there's a gap between `ents` and the last received entry in the storage.
    pub fn append(&mut self, ents: &[Entry]) -> Result<()> {
        if ents.is_empty() {
            return Ok(());
        }
        if self.first_index() > ents[0].index {
            panic!("raft logs should be continuous, last index: {}, new appended: {}", self.last_index(), ents[0].index);
        }

        if self.last_index() + 1 < ents[0].index {
            panic!("raft logs should be continuous, last index: {}, new appended: {}", self.last_index(), ents[0].index);
        }

        // Remove all entries overwritten by `ents`.
        let diff = ents[0].index -  self.first_index();
        self.entries.drain(diff as usize ..);
        self.entries.extend_from_slice(ents);
        Ok(())
    }

    /// Commit to `idx` and set configuration to the given states. Only used for tests.
    pub fn commit_to_and_set_conf_states(&mut self, idx: u64, cs: Option<ConfState>) -> Result<()> {
        self.commit_to(idx)?;
        if let Some(cs) = cs {
            self.raft_state.conf_state = cs;
        }
        Ok(())
    }

    /// Trigger a SnapshotTemporarilyUnavailable error.
    pub fn trigger_snap_unavailable(&mut self){
        self.trigger_snap_unavailable = true;
    }

    /// Set a LogTemporarilyUnavailable error.
    pub fn trigger_log_unavailable(&mut self, v: bool){
        self.trigger_log_unavailable = v;
    }

    /// Take get entries context.
    pub fn take_get_entries_context(&mut self) -> Option<GetEntriesContext> {
        self.get_entries_context.take()
    }


}

/// `MemStorage` is a thread-safe but incomplete implementation of `Storage`, mainly for tests.
///
/// A real `Storage` should save both raft logs and applied data. However `MemStorage` only
/// contains raft logs. So you can call `MemStorage::append` to persist new received unstable raft
/// logs and then access them with `Storage` APIs. The only exception is `Storage::snapshot`. There
/// is no data in `Snapshot` returned by `MemStorage::snapshot` because applied data is not stored
/// in `MemStorage`.
#[derive(Clone, Default)]
pub struct MemStorage {
    core: Arc<RwLock<MemStorageCore>>,
}

impl MemStorage {
    /// Returns a new memory storage value.
    pub fn new() -> MemStorage{
        MemStorage { ..Default::default() }
    }

    /// Create a new `MemStorage` with a given `Config`. The given `Config` will be used to initialize the storage.
    ///
    /// You should use the same input to initialize all nodes.
    pub fn new_with_conf_state<T>(conf_state: T) -> MemStorage 
    where
        ConfState: From<T>,
    {   
        let store = MemStorage::new();
        store.initialize_with_conf_state(conf_state);
        store
    }

    /// Initialize a `MemStorage` with a given `Config`.
    ///
    /// You should use the same input to initialize all nodes.
    pub fn initialize_with_conf_state<T>(&self, conf_state: T)
    where
        ConfState: From<T>,
    {
        assert!(!self.initial_state().unwrap().initilize());
        let mut core =  self.wl();
        // Setting initial state is very important to build a correct raft, as raft algorithm itself only guarantees logs consistency. Typically, 
        // you need to ensure either all start states are the same on all nodes, or new nodes always catch up logs by snapshot first.
        //
        // In practice, we choose the second way by assigning non-zero index to first index. Here we choose the first way for historical reason and 
        // easier to write tests.
        core.raft_state.conf_state = ConfState::from(conf_state);
    }

    /// Opens up a read lock on the storage and returns a guard handle. Use this with functions that don't require mutation.
    pub fn rl(&self) -> RwLockReadGuard<'_, MemStorageCore>{
        self.core.read().unwrap()
    }

    /// Opens up a write lock on the storage and returns guard handle. Use this with functions that take a mutable reference to self.
    pub fn wl(&self) -> RwLockWriteGuard<'_, MemStorageCore> {
        self.core.write().unwrap()
    }

}

impl Storage for MemStorage {
    fn initial_state(&self) -> Result<RaftState>  {
        Ok(self.rl().raft_state.clone())
    }

    fn entries(&self, low: u64, high: u64, max_size: impl Into<Option<u64>>, context: GetEntriesContext) -> Result<Vec<Entry>> {
        let max_size = max_size.into();
        let mut core = self.wl();
        if low < core.first_index() {
            return Err(Error::Store(StorageError::Compacted));
        }
        if high > core.last_index() + 1 {
            panic!("index out of bound (last: {}, high: {})", core.last_index() + 1, high);
        }

        if core.trigger_log_unavailable && context.can_async() {
            core.get_entries_context = Some(context);
            return Err(Error::Store(StorageError::LogTemporarilyUnavailable));
        }

        let offset = core.entries[0].index;
        let lo  = (low - offset) as usize;
        let hi = (high - offset) as usize;
        let mut ents = core.entries[lo..hi].to_vec();
        limit_size(&mut ents, max_size);
        Ok(ents)
    }

    fn term(&self, idx: u64) -> Result<u64> {
        let core  = self.rl();
        if idx == core.snapshot_metadata.index {
            return Ok(core.snapshot_metadata.term);
        }
        let offset  = core.first_index();
                if idx < offset {
            return Err(Error::Store(StorageError::Compacted));
        }

        if idx > core.last_index() {
            return Err(Error::Store(StorageError::Unavailable));
        }

        Ok(core.entries[(idx - offset) as usize].term)
    }

    fn first_idx(&self) -> Result<u64> {
        Ok(self.rl().first_index())
    }

    fn last_idx(&self) -> Result<u64> {
        Ok(self.rl().last_index())
    }

    fn snapshot(&self, request_idx: u64, _to: u64) ->Result<Snapshot> {
        let mut core = self.wl();
        if core.trigger_snap_unavailable {
            core.trigger_log_unavailable = false;
            Err(Error::Store(StorageError::SnapshotTemporarilyUnavailable))
        }else{
            let mut snap = core.snapshot();
            if snap.get_metadata().index < request_idx {
                snap.mut_metadata().index = request_idx;
            }
            Ok(snap)
        }
    }
}