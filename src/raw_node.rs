//! The raw node of the raft module. This module contains the value types for the node and it's connection to other nodes but not the raft consensus itself. 
//! Generally, you'll interact(交互) with the RawNode first and use it to access the inner workings of the consensus protocol.
//! Raft 模块的原始节点，这个模块包含了节点的类型值和与其他节点的连接关系，但不包括raft的共识机制。通常，你首先与RawNode进行交互，并使用共识协议的工作原理来访问。


use std::{collections::VecDeque, mem};
use protobuf::Message as PbMessage;
use raft_proto::{ ConfChangeI};
use crate::{eraftpb::{Message, MessageType, Entry, EntryType, ConfState, Snapshot, HardState}, GetEntriesContext, storage::GetEntriesFor, StateRole, status::Status};
use slog::{Logger, info};
use crate::read_only::ReadState;
use crate::{Config, Raft, Storage, Result, SoftState};


/// ReadyRecord encapsulates some needed data from the corresponding Ready.
#[derive(Debug, Default, PartialEq)]
pub struct ReadyRecord {
    number: u64,
    last_entry: Option<(u64, u64)>,
    snapshot: Option<(u64, u64)>,
}

/// Checks if certain message type should be used internally.
pub fn is_local_message(t: MessageType) -> bool {
    matches!(t, MessageType::MsgHup | MessageType::MsgBeat | MessageType::MsgUnreachable | MessageType::MsgSnapStatus | MessageType::MsgCheckQuorum)
}

fn is_response_msg(t: MessageType) -> bool {
    matches!(t, MessageType::MsgAppendResponse | MessageType::MsgRequestVoteResponse | MessageType::MsgHeartbeatResponse | MessageType::MsgUnreachable
        | MessageType::MsgRequestPreVoteResponse)
}

/// Represnents a peer(对等) node in the cluster.
#[derive(Debug, Default)]
pub struct Peer{
    /// The ID of the peer.
    pub id: u64,
    /// If there is context associated with the peer (like connection information), it can be serialized and stored here.
    pub context: Option<Vec<u8>>,
}

#[derive(Debug, PartialEq, Clone, Copy)]
/// The status of the snapshot.
pub enum SnapshotStatus {
    /// Represents that the snapshot is finished being created.
    Finish,
    /// Represents that the snapshot failed to build or is not ready
    Failure,
}

/// Ready encapsulates the entries and messages that are ready to read, be saved to stable storage, committed or sent to other peers.
#[derive(Debug,Default, PartialEq)]
pub struct Ready{
    number: u64,
    ss: Option<SoftState>,
    hs: Option<HardState>,
    read_states: Vec<ReadState>,
    entries: Vec<Entry>,
    snapshot: Snapshot,
    is_persisted_msg: bool,
    light: LightReady,
    must_sync: bool,
}

impl Ready {
    /// The number of current Ready.
    /// It is used for identifying the different Ready and ReadyRecord.
    #[inline]
    pub fn number(&self) -> u64 {
        self.number
    }

    /// The current volatile state of a Node.
    /// SoftState will be None if there is no update.
    /// It is not required to consume or store SoftState.
    #[inline]
    pub fn ss(&self) -> Option<&SoftState> {
        self.ss.as_ref()
    }

    /// The current state of a Node to be saved to stable storage.
    /// HardState will be None state if there is no update.
    #[inline]
    pub fn hs(&self) -> Option<&HardState> {
        self.hs.as_ref()
    }

    /// ReadStates specifies the state for read only query.
    #[inline]
    pub fn read_states(&self) -> &Vec<ReadState> {
        &self.read_states
    }

    /// Take the ReadStates.
    #[inline]
    pub fn take_read_states(&mut self) -> Vec<ReadState> {
        mem::take(&mut self.read_states)
    }

    /// Entries specifies entries to be saved to stable storage.
    #[inline]
    pub fn entries(&self) -> &Vec<Entry> {
        &self.entries
    }

    /// Take the Entries.
    #[inline]
    pub fn take_entries(&mut self) -> Vec<Entry>{
        mem::take(&mut self.entries)
    }

    /// Snapshot specifies the snapshot to be saved to stable storage.
    #[inline]
    pub fn snapshot(&self) -> &Snapshot {
        &self.snapshot
    }

    /// CommittedEntries specifies entries to be committed to a
    /// store/state-machine. These have previously been committed to stable
    /// store.
    #[inline]
    pub fn committed_entries(&self) -> &Vec<Entry> {
        &self.light.committed_entries
    }

    /// Take the CommitEntries.
    #[inline]
    pub fn take_committed_entries(&mut self) -> Vec<Entry> {
        mem::take(&mut self.light.committed_entries)
    }

    /// Messages specifies outbound messages to be sent.
    /// If it contains a MsgSnap message, the application MUST report back to raft
    /// when the snapshot has been received or has failed by calling ReportSnapshot.
    #[inline]
    pub fn messages(&self) -> &[Message] {
        if !self.is_persisted_msg {
            self.light.messages()
        }else {
            &[]
        }
    }

    /// Take the Messages.
    #[inline]
    pub fn take_message(&mut self) -> Vec<Message> {
        if !self.is_persisted_msg {
            self.light.take_message()
        }else {
            Vec::new()
        }
    }

    /// Persisted Messages specifies outbound messages to be sent AFTER the HardState,
    /// Entries and Snapshot are persisted to stable storage.
    #[inline]
    pub fn persisted_messages(&self) -> &[Message] {
        if self.is_persisted_msg {
            self.messages()
        }else {
            &[]
        }
    }

    /// Take the Persisted Messages.
    #[inline]
    pub fn take_persisted_messages(&mut self) -> Vec<Message> {
        if self.is_persisted_msg {
            self.take_message()
        }else {
            Vec::new()
        }
    }

    /// MustSync is false if and only if
    /// 1. no HardState or only its commit is different from before
    /// 2. no Entries and Snapshot
    /// If it's false, an asynchronous write of HardState is permissible before calling
    /// [`RawNode::on_persist_ready`] or [`RawNode::advance`] or its families.
    #[inline]
    pub fn must_sync(&self) -> bool {
        self.must_sync
    }
}

/// LightReady encapsulates the commit index, committed entries and messages that are ready to be applied or be sent to other peers.
#[derive(Debug, Default, PartialEq)]
pub struct LightReady{
    commit_index: Option<u64>,
    committed_entries: Vec<Entry>,
    messages: Vec<Message>,
}

impl LightReady {

    /// The current commit index. It will be None state if there is no update. It will not required save it to stable storage.
    #[inline]
    pub fn commit_index(&self) -> Option<u64> {
        self.commit_index
    }

    /// CommittedEntries specifies entries to be committed to a store/state-machine, These have previously(之前的) been committed stable store.
    #[inline]
    pub fn committed_entries(&self) -> &Vec<Entry> {
        &self.committed_entries
    }

    /// Take the CommittedEntries
    #[inline]
    pub fn take_committed_entries(&mut self) -> Vec<Entry> {
        mem::take(&mut self.committed_entries)
    }

    /// Messages specifies outbound messages to be sent.
    #[inline]
    pub fn messages(&self) -> &[Message] {
        &self.messages
    }

    /// Take the messages
    pub fn take_message(&mut self) -> Vec<Message>{
        mem::take(&mut self.messages)
    }

}

/// RawNode is a thread-unsafe Node. The methods of this struct correspondto the method of Node and are described more fully there.
pub struct RawNode<T: Storage> {
    /// The internal raft state.
    pub raft: Raft<T>,
    prev_ss: SoftState,
    prev_hs: HardState,
    // Current max number of Record and ReadyRecord.
    max_number: u64,
    records: VecDeque<ReadyRecord>,
    // Index which the given committed entries should start from.
    commit_since_index: u64,
}

impl<T: Storage> RawNode<T> {

    /// 
    #[allow(clippy::new_ret_no_self)]
    pub fn new(config: &Config, store: T, logger: &Logger) -> Result<Self> {
        assert_ne!(config.id, 0, "config.id must not be zero");
        let r = Raft::new(config, store, logger)?;
        let mut rn = RawNode {
            raft: r,
            prev_ss: Default::default(),
            prev_hs: Default::default(),
            max_number: 0,
            records: VecDeque::new(),
            commit_since_index: config.applied,
        };
        rn.prev_hs = rn.raft.hard_state();
        rn.prev_ss = rn.raft.soft_state();
        info!(rn.raft.logger, "RawNode create with id {id}.", id = rn.raft.id);
        Ok(rn)
    }

    /// Create a new RowNode given some [``Config] and the defualt log. The default log is `slog` to `log` adapter.
    #[cfg(feature = "default-logger")]
    #[allow(clippy::new_ret_no_self)]
    pub fn with_default_logger(c: &Config, store: T) -> Result<Self> {
        Self::new(c, store, &crate::default_logger())
    }

    /// Sets priority of node.
    #[inline]
    pub fn set_priority(&mut self, priority: u64) {
        self.raft.set_priority(priority);
    }

    /// Tick advances the internal logical clock by a single tick. Returns true to indicate that there will probably be some readiness which need to be handled
    pub fn tick(&mut self) -> bool {
        self.raft.tick()
    }

    /// Campaign causes the RawNode to transaction to candidate state.
    pub fn campaign(&mut self) -> Result<()>{
        let mut m = Message::new();
        m.set_msg_type(MessageType::MsgHup);
        self.raft.step(m)
    }

    /// Propose(建议) progress data be appended to the raft log 
    pub fn propose(&mut self, context: Vec<u8>, data: Vec<u8>) -> Result<()> {
        let mut m = Message::default();
        m.set_msg_type(MessageType::MsgPropose);
        m.from = self.raft.id;
        let mut e = Entry::default();
        e.data = data.into();
        e.context = context.into();
        m.set_entries(vec![e].into());
        self.raft.step(m)
    }

    /// Broadcast heartbeat to all the followers, If it's not leader, northing will happen.
    pub fn ping(&mut self) {
        self.raft.ping();
    }

    /// ProposeConfChange proposes a config change. If the node entries joint state with `auto_leave` set to true, it's caller's responsebility to propose an empty 
    /// conf change again to force leaving joint state.
    #[cfg_attr(feature = "cargo-clippy", allow(clippy::needless_pass_by_value))]
    pub fn propose_conf_change(&mut self, context: Vec<u8>, cc: impl ConfChangeI) -> Result<()>{
        let (data, ty) = if let Some(cc) = cc.as_v1() {
            (cc.write_to_bytes()?, EntryType::EntryConfChange)
        } else {
            (cc.as_v2().write_to_bytes()?, EntryType::EntryConfChangeV2)
        };
        let mut m = Message::default();
        m.set_msg_type(MessageType::MsgPropose);
        let mut e = Entry::default();
        e.set_entry_type(ty);
        e.data = data.into();
        e.context = context.into();
        m.set_entries(vec![e].into());
        self.raft.step(m)
    }

    /// Applies a config change to the local node. The app must call this when it applies a configuration change, except when it decidees to reject the configuration
    /// change, in which case no call must take place.
    pub fn apply_conf_change(&mut self, cc: &impl ConfChangeI) -> Result<ConfState> {
        self.raft.apply_conf_change(&cc.as_v2())
    }

    /// Step advances(推进) the state machine using the given message.
    pub fn step(&mut self, m: Message) -> Result<()> {
        // Ingore unexpected loacal messages receiving over network.
        if is_local_message(m.get_msg_type()) {
            return Err(crate::Error::StepLocalMsg);
        }
        if self.raft.prs().get(m.from).is_some() || !is_response_msg(m.get_msg_type()){
            return self.raft.step(m);
        }

        Err(crate::Error::StepPeerNotFound)
    }

    /// A callback when entries are featched asynchronously. The context should provide the context passed from Storage.entries(). See more in the comment of 
    /// Storage.entries(). Panics if passed with the context of context.can_async() == false.
    pub fn on_entries_fetched(&mut self, context: GetEntriesContext) {
        match context.0 {
            GetEntriesFor::SendAppend { to, term, aggressively } => {
                if self.raft.term != term || self.raft.state != StateRole::Leader{
                    // term or leadership has changed
                    return;
                }
                if self.raft.prs().get(to).is_none() {
                    // the peer is removed, do nothing
                    return;
                }
                if aggressively {
                    self.raft.send_append_aggresively(to);
                }else {
                    self.raft.send_append(to);
                }
            },
            GetEntriesFor::Empty(can_async) if can_async => {},
            _ => panic!("shouldn't call callback on non-async context"),
        }
    }

    /// Generates a LightReady that has the committed entries and messages but no commit index.
    fn gen_light_ready(&mut self) -> LightReady {
        let mut rd = LightReady::default();
        let max_size = Some(self.raft.max_commited_size_per_ready);
        let raft = &mut self.raft;
        rd.committed_entries = raft.raft_log.next_entries_since(self.commit_since_index, max_size).unwrap_or_default();

        // Update raft committed entries size
        raft.reduce_uncommitted_size(&rd.committed_entries);
        if let Some(e) = rd.committed_entries.last() {
            assert!(self.commit_since_index < e.get_index());
            self.commit_since_index = e.get_index();
        }
        if !raft.msgs.is_empty() {
            rd.messages = mem::take(&mut raft.msgs);
        }

        rd
    }

    /// Returns the outstanding work that the application needs to handle. This includes appending and applying entries or a snapshot, updateing the Hardstate,
    /// and sending messages. The return `Ready` *MUST* be handled and subsquently passed back via `advance` or its families. Before that *DO NOT* call any cunction
    /// like `step`, `propose`, `campaign` to change internal state. [`Self::has_ready`] should be call first to check if it's necessary to handle the ready.
    pub fn ready(&mut self) -> Ready {
        let raft = &mut self.raft;

        self.max_number += 1;
        let mut rd = Ready{
            number: self.max_number,
            ..Default::default()
        };

        let mut rd_record = ReadyRecord{
            number: self.max_number,
            ..Default::default()
        };

        if self.prev_ss.raft_state != StateRole::Leader && raft.state != StateRole::Leader {
            // The vote msg which makes this peer become leader has been sent after persisting. So the remaining records must be generated during being candidate
            // which can not have last_entry and snapshot(if so, it should become follower).
            for record in self.records.drain(..) {
                assert_eq!(record.last_entry, None);
                assert_eq!(record.snapshot, None);
            }
        }

        let ss = raft.soft_state();
        if ss != self.prev_ss {
            rd.ss = Some(ss);
        }

        let hs = raft.hard_state();
        if hs != self.prev_hs {
            if hs.vote != self.prev_hs.vote || hs.term != self.prev_hs.term {
                rd.must_sync = true;
            }
            rd.hs = Some(hs);
        }

        if !raft.read_states.is_empty() {
            mem::swap(&mut rd.read_states, &mut raft.read_states);
        }

        if let Some(snapshot) = &raft.raft_log.unsatble_snap(){
            rd.snapshot = snapshot.clone();
            assert!(self.commit_since_index <= rd.snapshot.get_metadata().index);
            self.commit_since_index = rd.snapshot.get_metadata().index;
            //If there is a snapshot, the latter entries can not be persisted so there is no committed entries.
            assert!(!raft.raft_log.has_next_entries_since(self.commit_since_index), "has snapshot but also has committed entries since {}", self.commit_since_index);
            rd_record.snapshot = Some((rd.snapshot.get_metadata().index, rd.snapshot.get_metadata().term));
            rd.must_sync = true;

        }

        rd.entries =raft.raft_log.unstable_entries().to_vec();
        if let Some(e) = rd.entries.last() {
            // If the last entry exists, the entries must be empty, vice versa.
            rd.must_sync = true;
            rd_record.last_entry = Some((e.get_index(), e.get_term()));
        }

        // Leader can send messages immediately to make replication concurrently. Fo more details(更多细节), check raft thesis 10.2.1
        rd.is_persisted_msg = raft.state != StateRole::Leader;
        rd.light = self.gen_light_ready();
        self.records.push_back(rd_record);
        rd
    }

    /// HasReady called when RawNode user need to check if any Ready pending.
    pub fn has_ready(&self) -> bool {
        let raft = &self.raft;
        if !raft.msgs.is_empty() {
            return true;
        }
        if raft.soft_state() != self.prev_ss {
            return true;
        }
        if raft.hard_state() != self.prev_hs {
            return true;
        }
        if !raft.read_states.is_empty() {
            return true;
        }
        if !raft.raft_log.unstable_entries().is_empty() {
            return true;
        }
        if raft.snap().map_or(false, |s| !s.is_empty()) {
            return true;
        }
        if raft.raft_log.has_next_entries_since(self.commit_since_index) {
            return true;
        }

        false
    }

    fn commit_ready(&mut self, rd: Ready) {
        if let Some(ss) = rd.ss {
            self.prev_ss = ss;
        }
        if let Some(hs) = rd.hs {
            self.prev_hs = hs;
        }
        let rd_reaord = self.records.back().unwrap();
        assert!(rd_reaord.number == rd.number);
        let raft = &mut self.raft;
        if let Some((index, _)) = rd_reaord.snapshot {
            raft.raft_log.satble_snap(index);
        }

        if let Some((index, term)) = rd_reaord.last_entry {
            raft.raft_log.stable_entries(index, term)
        }

    }

    fn commit_apply(&mut self, applied: u64) {
        self.raft.commit_apply(applied);
    }

    /// Notifies that the ready of this number has been persisted. Since ready must be persisted in order, calling this function implicitly means all readies with 
    /// numbers smaller than this one have been persisted. [`Self::has_ready`] and [`Self:ready`] should be called later to handle further updates that become valid
    /// after ready being persisted.
    pub fn on_persist_ready(&mut self, number: u64) {
        let (mut index, mut term) = (0,0);
        let mut snap_index = 0;
        while let Some(record) = self.records.front() {
            if record.number > number {
                break;
            }
            let record = self.records.pop_front().unwrap();
            if let Some((i, _)) = record.snapshot {
                snap_index = i;
                index = 0;
                term = 0;
            }
        }
        if snap_index != 0 {
            self.raft.on_persist_snap(snap_index);
        }
        if index != 0 {
            self.raft.on_persist_entries(index, term);
        }
    }

    /// Advances the ready after fully processing it. Fully processing a ready requires to persist snapshot, entries and hard states , apply all committed entires,
    /// send all messages. Returns the LightReady the contains commit index, committed entries and messages. [`LightReady`] contains updates the only valid after
    /// persisting last ready, it should also be fully processed. Then [`Self::advanec_apply`] or [`Self::advance_apply_to`] should be used later to update applying
    /// progress.
    pub fn advance(&mut self, rd: Ready) -> LightReady {
        let applied = self.commit_since_index;
        let light_rd = self.advance_append(rd);
        self.advance_apply_to(applied);

        light_rd
    }

    /// Advances the ready without applying committed entries. [`Self::advance_apply`] or [`Self::advance_apply_to`] should be used later to update applying progress.
    /// Returns the LightReady that contains commit index, committed entries and messages. Since Ready must be persisted in order, calling this function implicitly 
    /// means all ready collected before have been persisted.
    #[inline]
    pub fn advance_append(&mut self, rd: Ready) -> LightReady {
        self.commit_ready(rd);
        self.on_persist_ready(self.max_number);
        let mut light_rd = self.gen_light_ready();
        if self.raft.state != StateRole::Leader && !light_rd.messages.is_empty() {
            fatal!(self.raft.logger, "not leader has new msg after davance");
        }
        // Set commit index is it's update.
        let hard_state = self.raft.hard_state();
        if hard_state.commit > self.prev_hs.commit {
            light_rd.commit_index = Some(hard_state.commit);
            self.prev_hs.commit = hard_state.commit;
        }else {
            assert!(hard_state.commit == self.prev_hs.commit);
            light_rd.commit_index = None;
        }
        assert_eq!(hard_state, self.prev_hs, "hard_state != prev_hs");
        light_rd
    }

    /// Advance apply to the passed index(传递索引)
    #[inline]
    pub fn advance_apply_to(&mut self, applied: u64) {
        self.commit_apply(applied);
    }

    /// Same as [`Self::advance_append`] except that it allows to only store the updates in cache. [`Self::on_persist_ready`] should be used later to update the 
    /// persisting progress. Raft works on an assumption persisted updates should not be lost, which usually requires expensive operations like `fsync`. 
    /// `advance_append_async` allows you to control the rate of such operations and get a reasonable batch size. However, it's still required that the updates 
    /// can be read by raft from the `Storage` trait before calling `advance_append_async`.
    #[inline]
    pub fn advance_append_async(&mut self, rd: Ready) {
        self.commit_ready(rd);
    }
    /// Advance apply to the index of the last committed entries given before.
    #[inline]
    pub fn advance_apply(&mut self) {
        self.commit_apply(self.commit_since_index);
    }

    /// Grabs(抓取) the snapshot from the raft if available.
    #[inline]
    pub fn snap(&self) -> Option<&Snapshot> {
        self.raft.snap()
    }

    /// Status returns the current status of the given group.
    #[inline]
    pub fn status(&self) -> Status {
        Status::new(&self.raft)
    }

    /// ReportUnreachable reports the given node is not reachable for the last send.
    pub fn report_unreachable(&mut self, id: u64) {
        let mut m = Message::default();
        m.set_msg_type(MessageType::MsgUnreachable);
        m.from = id;
        // we don't care if it is ok actually
        let _ = self.raft.step(m);
    }

    /// ReportSnapshot reports the status of the sent snapshot.
    pub fn report_snapshot(&mut self, id: u64, status: SnapshotStatus) {
        let rej = status == SnapshotStatus::Failure;
        let mut m = Message::default();
        m.set_msg_type(MessageType::MsgSnapStatus);
        m.from = id;
        // we don't care if it is ok actually
        let _ = self.raft.step(m);
    }

    /// Request a snapshot from a leader. The snapshot's index must be greater or equal to the request_index.
    pub fn request_snapshot(&mut self, request_index: u64) -> Result<()> {
        self.raft.request_snapshot(request_index)
    }

    /// TransferLeader tries to transfer leadership to the given transferee.
    pub fn transfer_leader(&mut self, transferee: u64) {
        let mut m = Message::default();
        m.set_msg_type(MessageType::MsgTransferLeader);
        m.from = transferee;
        let _ = self.raft.step(m);
    }

    /// ReadIndex requests a read state. The read state will be set in ready. Read State has a read index. Once the application advances(进度) further than the read
    /// index, any linearizable read requests issued before the read request can be processed safely. The read state will have the same rctx attached.
    pub fn read_index(&mut self, rctx: Vec<u8>) {
        let mut m = Message::default();
        m.set_msg_type(MessageType::MsgReadIndex);
        let mut e = Entry::default();
        e.data = rctx.into();
        m.set_entries(vec![e].into());
        let _ = self.raft.step(m);
    }

    /// Returns the store as an immutable reference
    #[inline]
    pub fn store(&self) -> &T {
        self.raft.store()
    }

    /// Returns the store as an mutable reference
    #[inline]
    pub fn mut_store(&mut self) -> &mut T {
        self.raft.mut_store()
    }

    /// Set whether skip broadcast empty commit messages at runtime.
    #[inline]
    pub fn skip_bcast_commit(&mut self, skip: bool) {
        self.raft.skip_bcast_commit(skip);
    }

    /// Set whether to batch append msg at runtime.
    #[inline]
    pub fn set_batch_append(&mut self, batch_append: bool) {
        self.raft.set_batch_append(batch_append);
    }

}
