use std::cmp;
use std::ops::{Deref, DerefMut};

use crate::config::{Config, NO_LIMIT};
use crate::eraftpb::{
    ConfChange, ConfChangeV2, ConfState, Entry, EntryType, HardState, Message, MessageType,Snapshot
};
use crate::quorum::VoteResult;
use crate::raft_log::RaftLog;
use crate::read_only::{ReadOnly, ReadState, ReadOnlyOption};
use crate::storage::GetEntriesFor;
use crate::{util, ProgressState};
use crate::{
    confchange, Error, GetEntriesContext, Progress, ProgressTracker, Storage, StorageError, Result
};
use crate::confchange::Changer;
use protobuf::Message as _;
use raft_proto::ConfChangeI;

use getset::Getters;
use rand::Rng;
use slog::{debug, info, o, trace, Logger, error, warn};

/// A constat represents invalid id of raft
pub const INVALID_ID: u64 = 0;

/// A constant represents invalid index of raft log
pub const INVALID_INDEX: u64 = 0;

// CAMPAIGN_PRE_ELECTION represents the first phase(第一阶段) of a normal election when Config.pre_vote is true.
#[doc(hidden)]
pub const CAMPAIGN_PRE_ELECTION: &[u8] = b"CampaignPreElection";

// CAMPAIGN_ELECTION represents a normal (time-based) election (the second phase of the election when Config.pre_vote is true).
#[doc(hidden)]
pub const CAMPAIGN_ELECTION: &[u8] = b"CampaignElection";

// CAMPAIGN_TRANSFER represents the type of leader transfer(迁移).
#[doc(hidden)]
pub const  CAMPAIGN_TRANSFER: &[u8] = b"CampaignTransfer";

/// The role of the node.
#[derive(Debug, PartialEq, Clone, Copy, Default)]
pub enum StateRole {
    /// The node is a follower of leader/
    #[default]
    Follower,
    /// The node could become leader.
    Candidate,
    /// The node is a leader.
    Leader,
    /// the node could become a candidate(候选), if `prevote` is enable.
    PreCandidate,
}

/// A struct the represents the raft consensus itself. Stores details concerning the current and possible state the system can take.
pub struct Raft<T: Storage> {
    prs: ProgressTracker,
    /// The list of messages.
    pub msgs: Vec<Message>,
    /// Internal raftCore.
    pub r: RaftCore<T>,
}

impl<T: Storage> Deref for Raft<T> {
    type Target = RaftCore<T>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.r
    }
}

impl<T: Storage> DerefMut for Raft<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.r
    }
}

trait AssertSend: Send {}
impl<T: Storage + Send> AssertSend for Raft<T> {}

/// SoftState provides state that is useful for logging and debugging. The state is volatile and does not to be persisted to the WAL.  
#[derive(Debug, Default, PartialEq)]
pub struct SoftState {
    /// The potential(潜在) leader of the cluster.
    pub leader_id: u64,
    /// The soft role this node may take.
    pub raft_state: StateRole,
}

/// The core struct of raft consensus. It's a helper struct to get around rust borrow checks.
#[derive(Getters)]
pub struct RaftCore<T: Storage> {
    /// The current election term.
    pub term: u64,
    /// Which peer this raft is voting for.
    pub vote: u64,
    /// The ID of this node,
    pub id: u64,
    /// The current read states.
    pub read_states: Vec<ReadState>,
    /// The peresistent log
    pub raft_log: RaftLog<T>,
    /// The maximum number of messages that can be inflight.
    pub max_inflight: usize,
    /// The maximum length (in bytes) of all the entries.
    pub max_msg_size: u64,
    /// The peer is requesting snapshot, it is the index that the follower needs it to be included in a snapshot.
    pub pengding_request_snapshot: u64,
    /// The current role of this node.
    pub state: StateRole,
    /// Indicates whether state machine can be promoted to leader, which is true when it's a voter and its own id is in progress list.
    promotable: bool,
    /// The leader id
    pub leader_id: u64,
    /// ID of leader transfer target when its value is not None. If this is Some(id) , we follow the procedure defined in raft thesis 3.10.
    pub leader_transferee: Option<u64>,
    /// Only one conf change my be pending (in the log, but not yet applied) at a time. This is enforced via `pengding_conf_index`, which is set to a value >= thelog index of the latest pengding
    /// configuration change (if any). Config changes are only allowed to be proposed if the leader's applied index is greater than this value.
    /// This value is conservatively set in case where there may be configuration change pending, but scaning the log is possibly expensive. This implies that the index stated here may not necessarily
    /// be a config change array, and it may not be a `BeginMembershipChange` entry, even if we set this to one.
    pub pending_conf_index: u64,
    /// The queue of read-only requests.
    pub read_only: ReadOnly,
    /// Ticks since it reached last electionTimeout when it is leader or candidate. Number of ticks since it reached last electionTimeout or received a vaild message from current leader when it is a
    /// follower.
    pub election_elapsed: usize,
    /// Number of ticks since it reached last heartbeatTimeout. only leader keeps heartbeatElapsed.
    heartbeat_elapsed: usize,
    /// Whether to check the quorum.
    pub check_quorum: bool,
    /// Enable the prevote algorithm. This enables a pre-election vote round on Candidates prior to disrupting the cluster. Enable this if greater cluster stability is preferred over fast elections.
    pub pre_vote: bool,
    skip_bcast_commit: bool,
    batch_append: bool,
    heartbeat_timeout: usize,
    election_timeout: usize,

    // randomized_election_timeout is a random number between [min_election_timeout, max_election_timeout - 1]. It gets reset when raft changes its state to follower or candidate.
    randomized_election_timeout: usize,
    min_election_timeout: usize,
    max_election_timeout: usize,
    /// The loggerfor the raft structure.
    pub(crate) logger: slog::Logger,
    /// The election priority of this node.
    pub priority: u64,
    /// Track(跟踪) uncommited log entry on this node.
    uncommited_state: UncommitedState,
    /// Max size per commited entries in a `Read`.
    pub(crate) max_commited_size_per_ready: u64,
}

/// UncommitedState is used to keep track of uncommited log entries on `leader` node.
struct UncommitedState {
    /// Specify maximum of uncomiited entry size. When this limited is reached, All proposals(提议， 建议) to append new log will be droped.
    max_uncommited_size: usize,
    /// Record current uncommited entries size.
    uncommited_size: usize,
    /// Record index of last log entry when node becomes leader from candidate.
    last_log_tail_index: u64,
}

impl UncommitedState {
    #[inline]
    pub fn is_no_limit(&self) -> bool {
        self.max_uncommited_size == NO_LIMIT as usize
    }

    pub fn maybe_increase_uncommitted_size(&mut self, ents: &[Entry]) -> bool {
        if self.is_no_limit() {
            return true;
        }

        let size: usize = ents.iter().map(|ent| ent.get_data().len()).sum();
        // 1. we should never drop an entry without any data(eg. leader election)
        // 2. we should allow at least one uncommitted entry
        // 3. add these entries will not cause size overlimit

        if size == 0
            || self.uncommited_size == 0
            || size + self.uncommited_size <= self.max_uncommited_size
        {
            self.uncommited_size += size;
            true
        } else {
            false
        }
    }

    pub fn maybe_reduce_uncommited_size(&mut self, ents: &[Entry]) -> bool {
        if self.is_no_limit() || ents.is_empty() {
            return true;
        }

        // user may advance a `Ready` which is generated before this node becomes leader.
        let size: usize = ents
            .iter()
            .skip_while(|ent| ent.index <= self.last_log_tail_index)
            .map(|ent| ent.get_data().len())
            .sum();
        if size > self.uncommited_size {
            self.uncommited_size = 0;
            false
        } else {
            self.uncommited_size -= size;
            true
        }
    }
}

fn new_message(to: u64, field_type: MessageType, from: Option<u64>) -> Message {
    let mut m = Message::default();
    m.to = to;
    if let Some(id) = from {
        m.from = id;
    }
    m.set_msg_type(field_type);
    m
}

/// Maps vote and pre_vote message type to their correspond responses.
pub fn vote_resp_msg_type(t: MessageType) -> MessageType {
    match t {
        MessageType::MsgRequestVote => MessageType::MsgRequestVoteResponse,
        MessageType::MsgRequestPreVote => MessageType::MsgRequestPreVoteResponse,
        _ => panic!("Not vote a message: {:?}", t),
    }
}

impl<T: Storage> Raft<T> {
    /// Create a new raft for use on the node.
    #[allow(clippy::new_ret_no_self)]
    pub fn new(c: &Config, store: T, logger: &Logger) -> Result<Self> {
        c.validate()?;
        let logger = logger.new(o!("raft_id" => c.id));
        let raft_state = store.initial_state()?;
        let conf_state = &raft_state.conf_state;
        let voters = &conf_state.voters;
        let learners = &conf_state.learners;
        let mut r = Raft {
            prs: ProgressTracker::with_capacity(voters.len(), learners.len(), c.max_inflight_msgs),
            msgs: Default::default(),
            r: RaftCore {
                term: Default::default(),
                vote: Default::default(),
                id: c.id,
                read_states: Default::default(),
                raft_log: RaftLog::new(store, logger.clone()),
                max_inflight: c.max_inflight_msgs,
                max_msg_size: c.max_size_per_msg,
                pengding_request_snapshot: INVALID_INDEX,
                state: StateRole::Follower,
                promotable: false,
                leader_id: Default::default(),
                leader_transferee: None,
                pending_conf_index: Default::default(),
                read_only: ReadOnly::new(c.read_only_option),
                election_elapsed: Default::default(),
                heartbeat_elapsed: Default::default(),
                check_quorum: c.check_quorum,
                pre_vote: c.pre_vote,
                skip_bcast_commit: c.skip_bcast_commit,
                election_timeout: c.election_tick,
                randomized_election_timeout: Default::default(),
                min_election_timeout: c.min_election_tick,
                max_election_timeout: c.max_election_tick,
                logger,
                priority: c.priority,
                uncommited_state: UncommitedState {
                    max_uncommited_size: c.max_uncommitted_size as usize,
                    uncommited_size: 0,
                    last_log_tail_index: 0,
                },
                max_commited_size_per_ready: c.max_committed_size_per_ready,
                batch_append: c.batch_append,
                heartbeat_timeout: c.heartbeat_tick,
            },
        };
        confchange::restore(&mut r.prs, r.r.raft_log.last_idx(), conf_state)?;
        let new_cs = r.post_conf_change();
        if !raft_proto::conf_state_eq(&new_cs, conf_state) {
            fatal!(
                r.logger,
                "invalid restore: {:?} != {:?}",
                conf_state,
                new_cs
            );
        }

        if raft_state.hard_state != HardState::default() {
            r.load_state(&raft_state.hard_state)
        }
        if c.applied > 0 {
            r.commit_apply(c.applied);
        }
        r.become_follower(r.term, INVALID_ID);

        info!(r.logger, "newRaft"; "term" => r.term, "commit" => r.raft_log.committed, "applied" => r.raft_log.applied, "last_index" => r.raft_log.last_idx(), "last_term" => r.raft_log.last_term(), "peers" => ?r.prs.conf().voters);
        Ok(r)
    }

    /// Sets priority of node.
    pub fn set_priority(&mut self, priority: u64) {
        self.priority = priority;
    }

    /// Creates new a raft for use on the node with the default logger. The default logger is an `slog` to `log`  adapter
    #[allow(clippy::new_ret_no_self)]
    #[cfg(feature = "default-logger")]
    pub fn with_default_logger(c: &Config, store: T) -> Result<Self> {
        Self::new(c, store, &crate::default_logger())
    }

    /// Grabs(获取) a inmutable reference to the store.
    #[inline]
    pub fn store(&self) -> &T {
        &self.raft_log.store
    }

    /// Grabs a mutable reference to the store.
    #[inline]
    pub fn mut_store(&mut self) -> &mut T {
        &mut self.raft_log.store
    }

    /// Grabs a reference to the snapshot
    #[inline]
    pub fn snap(&self) -> Option<&Snapshot> {
        self.raft_log.unstable.snapshot.as_ref()
    }

    /// Returns number of pending read-only message.
    #[inline]
    pub fn pending_read_count(&self) -> usize {
        self.read_only.pengding_read_count()
    }

    /// Returns how many read states exist
    #[inline]
    pub fn ready_read_count(&self) -> usize {
        self.read_states.len()
    }

    /// Returns a value representing the softstate at the time of calling.
    pub fn soft_state(&self) -> SoftState {
        SoftState { leader_id: self.leader_id, raft_state: self.state }
    }

    /// Returns a value representing the hardstate at the time of calling. 
    pub fn hard_state(&self) -> HardState {
        let mut hs = HardState::default();
        hs.term = self.term;
        hs.vote = self.vote;
        hs.commit = self.raft_log.committed;
        hs
    }

    /// Returns whteher the current raft is in lease.
    pub fn in_lease(&self) -> bool {
        self.state == StateRole::Leader && self.check_quorum
    }

    /// For testing leader lease
    #[doc(hidden)]
    pub fn set_randomized_election_timeout(&mut self, t: usize) {
        assert!(t >= self.min_election_timeout && t < self.max_election_timeout);
        self.randomized_election_timeout = t;
    }

    /// Fetch the length of the election timeout.
    pub fn election_timeout(&self) -> usize {
        self.election_timeout
    }

    /// Fetch the length of the heartbeat timeout.
    pub fn heartbeat_timeout(&self) -> usize {
        self.heartbeat_timeout
    }

    /// Fetch the number of ticks elapsed since last heartbeat(获取自上次心跳以来的滴答数)
    pub fn heartbeat_elapsed(&self) -> usize {
        self.heartbeat_elapsed
    }

    /// Return the length of the current randomized election timeout.
    pub fn randomized_election_timeout(&self) -> usize {
        self.randomized_election_timeout
    }

    /// Set whether skip broadcast empty commit messages at runtime.
    #[inline]
    pub fn skip_bcast_commit(&mut self, skip: bool) {
        self.skip_bcast_commit = skip;
    }

    /// Set whether batch append msg at runtime.
    #[inline]
    pub fn set_batch_append(&mut self, batch_append: bool) {
        self.batch_append = batch_append;
    }

    /// Configures group commit. If group commit is enabled, only logs replicated to at least two different groups are committed. You should use `assign_commit_group`
    /// to configure peer groups.
    pub fn enable_group_commit(&mut self, enable: bool) {
        self.mut_prs().enable_group_commit(enable);
        if StateRole::Leader == self.state && !enable && self.maybe_commit() {
            self.bcast_append();
        }
    }

    /// whether enable group commit
    pub fn group_commit(&self) -> bool {
        self.prs.group_commit()
    }

    /// Assigns groups to peers. The tuple is (`peer_id`, `group_is`). `group_id` should be large than 0. The group information is only stored in memeory. So you 
    /// need to configure it every time a raft state machine is initialized or a snapshot is applied.
    pub fn assign_commit_groups(&mut self, ids: &[(u64, u64)]) {
        let prs = self.mut_prs();
        for (peer_id, group_id) in ids {
            assert!(*group_id > 0);
            if let Some(pr) = prs.get_mut(*peer_id) {
                pr.commit_group_id = *group_id;
            } else {
                continue;
            }
        }
        if self.state == StateRole::Leader && self.group_commit() && self.maybe_commit() {
            self.bcast_append();
        }
    }

    /// Removes all commit group configurations.
    pub fn clear_commit_group(&mut self) {
        for (_, pr) in self.mut_prs().iter_mut() {
            pr.commit_group_id = 0;
        }
    }

    /// Checks whether the raft group is using group commit and consistent over group. If it can't get a correct answer, `None` is returned.
    pub fn check_group_commit_consistent(&mut self) -> Option<bool> {
        if self.state != StateRole::Leader {
            return None;
        }
        
        // Previous leader may have reach consistency already. check applied_index instead of committed_index to avoid pending conf change.
        if !self.apply_to_current_term(){
            return None;
        }
        let (index, use_group_commit) = self.mut_prs().maximal_commited_index();
        debug!(self.logger, "check group commit consistent"; "index" => index, "use_group_commit" => use_group_commit, "committed" => self.raft_log.committed);
        Some(use_group_commit && index == self.raft_log.committed)
    }

    /// Checks if logs are committed to its term. The check is useful usually when raft is leader.
    pub fn commit_to_current_term(&self) -> bool {
        self.raft_log.term(self.raft_log.applied).map_or(false, |t| t == self.term)
    }

    /// Checks if logs are applied the current term.
    pub fn apply_to_current_term(&self) -> bool {
        self.raft_log.term(self.raft_log.applied).map_or(false, |t| t == self.term)
    }

    /// Set `max_committed_size_per_ready` to `size`
    pub fn set_max_committed_size_per_ready(&mut self, size: u64) {
        self.max_commited_size_per_ready = size;
    }

    /// Set whether or not `check_quorum`.
    pub fn set_check_quorum(&mut self, check_quorum: bool) {
        self.check_quorum = check_quorum;
    }

}

impl<T: Storage> RaftCore<T> {
    fn send(&mut self, mut m: Message, msgs: &mut Vec<Message>) {
        debug!(self.logger, "Sending from {from} to {to}", from = self.id, to = m.to; "msg" => ?m);
        if m.from == INVALID_ID {
            m.from = self.id;
        }
        if m.get_msg_type() == MessageType::MsgRequestVote
            || m.get_msg_type() == MessageType::MsgRequestPreVote
            || m.get_msg_type() == MessageType::MsgRequestVoteResponse
            || m.get_msg_type() == MessageType::MsgRequestPreVoteResponse
        {
            if m.term == 0 {
                // All {pre-,}campaign messages need to have the term set when sending.
                // - MsgVote: m.Term is the term the node is campaigning for,non-zero as we increment the term when campaigning.
                // - MsgVoteResp: m.Term is the new r.Term if the MsgVote was granted, non-zero for the same reason MsgVote is
                // - MsgPreVote: m.Term is the term the node will campaign, non-zero as we use m.Term to indicate the next term we'll be campaigning for
                // - MsgPreVoteResp: m.Term is the term received in the original MsgPreVote if the pre-vote was granted, non-zero for the same reasons MsgPreVote is
                fatal!(
                    self.logger,
                    "term should be set when sending {:?}",
                    m.get_msg_type()
                );
            }
        } else {
            if m.term != 0 {
                fatal!(
                    self.logger,
                    "term should be set when sending {:?} (was {})",
                    m.get_msg_type(),
                    m.term
                );
            }
            // do not attach term to MsgPropose、 MsgReadIndex proposals are a way to forward to the leader and should be treated as local message. MsgReadIndex is
            // also forwarded to leader
            if m.get_msg_type() != MessageType::MsgPropose
                && m.get_msg_type() != MessageType::MsgReadIndex
            {
                m.term = self.term;
            }
        }
        if m.get_msg_type() == MessageType::MsgRequestVote
            || m.get_msg_type() == MessageType::MsgRequestPreVote
        {
            m.priority = self.priority;
        }
        msgs.push(m)
    }

    /// Sends an append RPC with new entries (if any) and the current commit index to the given peer.
    fn send_append(&mut self, to: u64, pr: &mut Progress, msgs: &mut Vec<Message>) {
        self.maybe_send_append(to, pr, true, msgs);
    }

    fn send_append_aggresively(&mut self, to: u64, pr: &mut Progress, msgs: &mut Vec<Message>) {
        // If we habe more entries to send, send as many messages as we can(without sending empty messages for the commit index).
        while self.maybe_send_append(to, pr, false, msgs) { }
    }
    fn maybe_send_append(
        &mut self,
        to: u64,
        pr: &mut Progress,
        allow_empty: bool,
        msgs: &mut Vec<Message>,
    ) -> bool {
        if pr.is_paused() {
            trace!(self.logger, "Skipping sending to {to}, it's paused", to = to; "progress" => ?pr);
            return false;
        }
        let mut m = Message::default();
        m.to = to;
        if pr.pending_request_snapshot != INVALID_INDEX {
            // Check pengding request snapshot first to avoid unnecessary loading entries.（首先检查挂起的请求快照，以避免加载不必要的条目）
            if !self.prepare_send_snapshot(&mut m, pr, to) {
                return false;
            }
        } else {
            let ents = self.raft_log.entries(
                pr.next_idx,
                self.max_msg_size,
                GetEntriesContext(GetEntriesFor::SendAppend {
                    to,
                    term: self.term,
                    aggressively: !allow_empty,
                }),
            );
            if !allow_empty && ents.as_ref().ok().map_or(true, |e| e.is_empty()) {
                return false;
            }
            let term = self.raft_log.term(pr.next_idx - 1);
            match (term, ents) {
                (Ok(term), Ok(mut ents)) => {
                    if self.batch_append && self.try_batching(to, msgs, pr, &mut ents) {
                        return true;
                    }
                    self.prepare_send_entries(&mut m, pr, term, ents)
                }
                (_, Err(Error::Store(StorageError::LogTemporarilyUnavailable))) => {
                    // wait for storage to fetch entries asychronously
                    return false;
                }
                _ => if !self.prepare_send_snapshot(&mut m, pr, to) {
                    return false;
                },
            }
        }
        self.send(m, msgs);
        true
    }

    fn try_batching(
        &mut self,
        to: u64,
        msgs: &mut [Message],
        pr: &mut Progress,
        ents: &mut Vec<Entry>,
    ) -> bool {
        // if MsgAppend for the receiver already exists, try_batching will append the entries to the entries to the existing MsgAppend
        let mut is_batched = false;
        for msg in msgs {
            if msg.get_msg_type() == MessageType::MsgAppend && msg.to == to {
                if !ents.is_empty() {
                    if !util::is_continuous_ents(msg, ents) {
                        return is_batched;
                    }
                    let mut batched_entries: Vec<_> = msg.take_entries().into();
                    batched_entries.append(ents);
                    msg.set_entries(batched_entries.into());
                    let last_idx = msg.entries.last().unwrap().index;
                    pr.update_state(last_idx);
                }
                msg.commit = self.raft_log.committed;
                is_batched = true;
                break;
            }
        }
        is_batched
    }

    fn prepare_send_entries(
        &mut self,
        m: &mut Message,
        pr: &mut Progress,
        term: u64,
        ents: Vec<Entry>,
    ) {
        m.set_msg_type(MessageType::MsgAppend);
        m.index = pr.next_idx - 1;
        m.log_term = term;
        m.set_entries(ents.into());
        m.commit = self.raft_log.committed;
        if !m.entries.is_empty() {
            let last = m.entries.last().unwrap().index;
            pr.update_state(last);
        }
    }

    fn prepare_send_snapshot(&mut self, m: &mut Message, pr: &mut Progress, to: u64) -> bool {
        if !pr.recent_active {
            debug!(self.logger, " ingore sending snapshot to {} since it is not recently active", to;);
            return false;
        }

        m.set_msg_type(MessageType::MsgSnapshot);
        let snapshot_r = self.raft_log.snapshot(pr.pending_request_snapshot, to);
        if let Err(e) = snapshot_r {
            if e == Error::Store(StorageError::SnapshotTemporarilyUnavailable) {
                debug!(self.logger, "failed to send snapshot to {} because snapshot is temporarily unavailable", to;);
                return false;
            }
            fatal!(self.logger, "unexpected error: {:?}", e);
        }
        let snapshot = snapshot_r.unwrap();
        if snapshot.get_metadata().index == 0 {
            fatal!(self.logger, "need non-empty snapshot");
        }
        let (sindex, sterm) = (snapshot.get_metadata().index, snapshot.get_metadata().term);
        m.set_snapshot(snapshot);
        debug!(self.logger, "[firstindex: {first_index}, commit:{committed}] sent snapshot[index: {snapshot_index}, term: {snapshot_term}] to {to}",first_index = self.raft_log.first_idx(),committed = self.raft_log.committed,snapshot_index = sindex,snapshot_term = sterm,to = to;"progress" => ?pr);
        pr.become_snapshot(sindex);
        debug!(self.logger, "paused sending replication messages to {}", to; "progress" => ?pr);
        true
    }

    /// send_heartbeat sends an empty MsgAppend
    fn send_heartbeat(&mut self, to: u64, pr: &Progress, ctx: Option<Vec<u8>>, msgs: &mut Vec<Message>) {
        // Attach the commit as min(to.matched, seld.raft_log_committed). When the leader sends out heartbeat message, the receiver(follower) might not be matched with
        // the leader or it might not have all the commited entries. The leader MUST NOT forward the follower's commit to an unmatched index.
        let mut m = Message::default();
        m.to = to;
        m.set_msg_type(MessageType::MsgHeartbeat);
        m.commit = cmp::min(pr.matched, self.raft_log.committed);
        if let Some(context) = ctx {
            m.context = context.into();
        }
        self.send(m, msgs)
    }
}

impl<T: Storage> Raft<T> {

    /// Get inflight buffer size.
    pub fn inflight_buffer_size(&self) -> usize {
        let mut total_size = 0;
        for (_, pr) in self.prs.iter() {
            total_size += pr.ins.buffer_capacity() * std::mem::size_of::<u64>();
        }
        total_size
    }

    pub(super) fn send_append_aggresively(&mut self, to: u64) {
        let pr = self.prs.get_mut(to).unwrap();
        self.r.send_append_aggresively(to, pr, &mut self.msgs);
    }

    /// Updates the in-memory state and, when necessary, carries out additional actions such as reacting to the removal of nodes or changed quorum requirement.
    pub fn post_conf_change(&mut self) -> ConfState {
        info!(self.logger, "switched to configuration"; "config" => ?self.prs.conf());
        // progress tracker is better.
        let cs = self.prs.conf().to_conf_state();
        let is_voter = self.prs.conf().voters.contains(self.id);
        self.promotable = is_voter;
        if !is_voter && self.state == StateRole::Leader {
            // This node is leader and was removed or demoted. We prevent demotions at the time writing but hypothetically we handle them the same way as removing
            // the leader: stepping down into the next Term. to TimeoutNow (to avoid interruption). This might still drop some proposals but it's better than nothing.
            return cs;
        }

        // The remaining steps only make sense if this node is the leader and there are other nodes.
        if self.state != StateRole::Leader || cs.voters.is_empty() {
            return cs;
        }

        if self.maybe_commit() {
            // If the configuration change means that more entries are committed now, broadcast/append to everyone in the update config.
            self.bcast_append();
        } else {
            // Otherwise, still probe the newly added replicas; there is no reason to let them wait out a heartbeat interval (or the next incoming proposal).
            let self_id = self.id;
            let core = &mut self.r;
            let msgs = &mut self.msgs;
            self.prs
                .iter_mut()
                .filter(|&(id, _)| *id != self_id)
                .for_each(|(id, pr)| {core.maybe_send_append(*id, pr, false, msgs);});
        }

        // The quorum size is now smaller, consider to response some read requests. If there is only one peer, all penging read requests must be responded.
        if let Some(ctx) = self.read_only.last_pending_request_ctx() {
            let prs = &self.prs;
            if self
                .r
                .read_only
                .recv_ack(self.id, &ctx)
                .map_or(false, |acks| prs.has_quorum(acks))
            {
                for rs in self.r.read_only.advance(&ctx, &self.r.logger) {
                    if let Some(m) = self.handle_ready_read_index(rs.req, rs.index) {
                        self.r.send(m, &mut self.msgs)
                    }
                }
            }
        }

        if self
            .leader_transferee
            .map_or(false, |e| !self.prs.conf().voters.contains(e))
        {
            self.abort_leader_transfer();
        }
        cs
    }

    /// Attempts to advancethe commit index. Returns true if the commit index changed (in which case the caller should call `r.bcast_append`).
    pub fn maybe_commit(&mut self) -> bool {
        let mci = self.mut_prs().maximal_commited_index().0;
        if self.r.raft_log.maybe_commit(mci, self.r.term) {
            let (self_id, commited) = (self.id, self.raft_log.committed);
            self.mut_prs()
                .get_mut(self_id)
                .unwrap()
                .update_commited(commited);
            return true;
        }
        false
    }

    /// Returns a mutable reference to the progress set.
    pub fn mut_prs(&mut self) -> &mut ProgressTracker {
        &mut self.prs
    }

    /// Sends RPC, with entries to all peers that are not up-to-date according to the progress recorded in r.prs().
    pub fn bcast_append(&mut self) {
        let self_id = self.id;
        let core = &mut self.r;
        let msgs = &mut self.msgs;
        self.prs
            .iter_mut()
            .filter(|&(id, _)| *id != self_id)
            .for_each(|(id, pr)| core.send_append(*id, pr, msgs))
    }

    /// Broadcast heratbeats to all the followers if it's leader.
    pub fn ping(&mut self) {
        if self.state == StateRole::Leader {
            self.bcast_heartbeat()
        }
    }

    /// Send RPC, without entries to all peers.
    pub fn bcast_heartbeat(&mut self) {
        let ctx = self.read_only.last_pending_request_ctx();
        self.bcast_heartbeat_with_ctx(ctx)
    }

    #[cfg_attr(feature= "cargo-clippy", allow(clippy::needless_pass_by_value))]
    fn bcast_heartbeat_with_ctx(&mut self, ctx: Option<Vec<u8>>) {
        let self_id = self.id;
        let core = &mut self.r;
        let msgs = &mut self.msgs;
        self.prs.iter_mut().filter(|&(id, _)| *id != self_id).for_each(|(id, pr)| core.send_heartbeat(*id, pr, ctx.clone(), msgs));
    }

    /// Sends an append RPC with new entries (if any) and the current commit index to the given peer.
    pub fn send_append(&mut self, to: u64) {
        let pr = self.prs.get_mut(to).unwrap();
        self.r.send_append(to, pr, &mut self.msgs)
    }

    fn handle_ready_read_index(&mut self, mut req: Message, index: u64) -> Option<Message> {
        if req.from == INVALID_ID || req.from == self.id {
            let rs = ReadState {
                index,
                request_ctx: req.take_entries()[0].take_data().to_vec(),
            };
            self.read_states.push(rs);
            return None;
        }
        let mut to_send = Message::default();
        to_send.set_msg_type(MessageType::MsgReadIndexResp);
        to_send.to = req.from;
        to_send.index = index;
        to_send.set_entries(req.take_entries());
        Some(to_send)
    }

    /// Stops the transfer of a leader
    pub fn abort_leader_transfer(&mut self) {
        self.leader_transferee = None;
    }

    /// For a given hardstate, load the state into self.
    pub fn load_state(&mut self, hs: &HardState) {
        if hs.commit < self.raft_log.committed || hs.commit > self.raft_log.last_idx() {
            fatal!(
                self.logger,
                "hs.commit {} is out of range [{}, {}]",
                hs.commit,
                self.raft_log.committed,
                self.raft_log.last_idx()
            );
        }
        self.raft_log.committed = hs.commit;
        self.term = hs.term;
        self.vote = hs.vote;
    }

    /// Commit that the Raft peer has applied up to the given index. Register the new applied index to the Raft log. Post: Checks to see if it's time to finalize a
    /// joint Consensus state.
    pub fn commit_apply(&mut self, applied: u64) {
        let old_applied = self.raft_log.applied;
        if self.prs.conf().auto_leave
            && old_applied <= self.pending_conf_index
            && applied >= self.pending_conf_index
            && self.state == StateRole::Leader
        {
            // If the current (and most recent, at least for this leader's term) configuration should be auto-left, initiate that now. We use a nil Data which
            // unmarshals into an empty ConfChangeV2 and has the benefit that appendEntry can never refuse it based on its size (which registers as zero).
            let mut entry = Entry::default();
            entry.set_entry_type(EntryType::EntryConfChangeV2);
            // append_entry will never refuse an empty
            if !self.append_entry(&mut [entry]) {
                panic!("appending an empty EntryConfChangev2 should never be droped")
            }
            self.pending_conf_index = self.raft_log.last_idx();
            info!(self.logger, "initiating automatic transition out of joint configuration"; "config" => ?self.prs.conf());
        }
    }

    /// Appends  a slice of entries to the log. The entries are updated to match the current index and term. Only called by leader currently.
    #[must_use]
    pub fn append_entry(&mut self, es: &mut [Entry]) -> bool {
        if !self.maybe_increase_uncommitted_size(es) {
            return false;
        }

        let li = self.raft_log.last_idx();
        for (i, e) in es.iter_mut().enumerate() {
            e.term = self.term;
            e.index = li + 1 + i as u64;
        }
        self.raft_log.append(es);

        // Not update self's pr.matched until on_persist_entries
        true
    }

    /// Increase size of 'ents' to uncommitted size. Return true when size limit is satisfied. Otherwise return false and uncommitted size remains unchanged.
    /// For raft with no limit(or non-leader raft), it always return true.
    #[inline]
    pub fn maybe_increase_uncommitted_size(&mut self, ents: &[Entry]) -> bool {
        self.uncommited_state.maybe_increase_uncommitted_size(ents)
    }

    /// Converts this node to a follower.
    pub fn become_follower(&mut self, term: u64, leader_id: u64) {
        let pending_request_snapshot = self.pengding_request_snapshot;
        self.reset(term);
        self.leader_id = leader_id;
        self.state = StateRole::Follower;
        self.pengding_request_snapshot = pending_request_snapshot;
        info!(self.logger, "became follower at term {term}", term = self.term;);
    }

    /// Resets the current node to a given term.
    pub fn reset(&mut self, term: u64) {
        if self.term != term {
            self.term = term;
            self.vote = INVALID_ID;
        }
        self.leader_id = INVALID_ID;
        self.reset_randomized_election_timeout();
        self.election_elapsed = 0;
        self.heartbeat_elapsed = 0;

        self.abort_leader_transfer();
        self.prs.reset_votes();

        self.pending_conf_index = 0;
        self.read_only = ReadOnly::new(self.read_only.option);
        self.pengding_request_snapshot = INVALID_INDEX;

        let last_index = self.raft_log.last_idx();
        let committed = self.raft_log.committed;
        let persisted = self.raft_log.persisted;
        let self_id = self.id;
        for (&id, mut pr) in self.mut_prs().iter_mut() {
            pr.reset(last_index + 1);
            if id == self_id {
                pr.matched = persisted;
                pr.commited_index = committed;
            }
        }
    }

    /// Regenerates and stores the election timeout
    pub fn reset_randomized_election_timeout(&mut self) {
        let prev_timeout = self.randomized_election_timeout;
        let timeout =
            rand::thread_rng().gen_range(self.min_election_timeout..self.max_election_timeout);
        debug!(self.logger, "reset election timeout {prev_timeout} -> {timeout} at {election_elapsed}", prev_timeout = prev_timeout, timeout = timeout, election_elapsed = self.election_elapsed;);
        self.randomized_election_timeout = timeout;
    }

    /// Notifies that these raft logs have been persisted.
    pub fn on_persist_entries(&mut self, index: u64, term: u64){
        let update = self.raft_log.maybe_persist(index, term);
        if update && self.state == StateRole::Leader {
            // Actually, If it is a leader and persisited index is updated, this term must be equal to self.term because the persisited index must be equal to the 
            // last index of entries from previous leader when it becomes leader (see the comments in become_leader), namely, the new persisited entries must come
            // from this leader.
            if term != self.term {
                error!(self.logger, "Leader's persisted index changed but the term {} is not the same as {}", term, self.term);
            }
            let self_id = self.id;
            let pr = self.mut_prs().get_mut(self_id).unwrap();
            if pr.maybe_update(index) && self.maybe_commit() && self.shuld_bcast_commit() {
                self.bcast_append();
            }
        }
    }

    /// Specifies if the commit should be broadcast.
    pub fn shuld_bcast_commit(&self) -> bool {
        !self.skip_bcast_commit || self.has_pending_conf()
    }


    /// Check if there is any pending confchange. This method can be false positive.
    #[inline]
    pub fn has_pending_conf(&self) -> bool {
        self.pending_conf_index > self.raft_log.applied
    }

    /// Notifies that the snapshot have been persisted.
    pub fn on_persist_snap(&mut self, index: u64) {
        self.raft_log.maybe_persist_snap(index);
    }

    /// Returns true to indicate that will probably be some readniess nedd to be handled.
    pub fn tick(&mut self) -> bool {
        match self.state {
            StateRole::Follower | StateRole::Candidate | StateRole::PreCandidate => self.tick_election(),
            StateRole::Leader => self.tick_heartbeat(),
        }
    }

    /// Run by followers and candidates after self.election_timeout. Returns true to indicate that there will probably be some readiness need to be handled.
    pub fn tick_election(&mut self) -> bool {
        self.election_elapsed += 1;
        if !self.pass_election_timeout() || !self.promotable{
            return false;
        }

        self.election_elapsed = 0;
        let m = new_message(INVALID_ID, MessageType::MsgHup, Some(self.id));
        let _ = self.step(m);
        true
    }

    /// tick_heartbeat is run by leaders to send a MsgBeat after self.heartbeat_timeout. Returns true to indicate that there will probably be soem readiness need
    /// to be handled.
    fn tick_heartbeat(&mut self) -> bool {
        self.heartbeat_elapsed += 1;
        self.election_elapsed += 1;

        let mut has_ready = false;
        if self.election_elapsed >= self.election_timeout {
            self.election_elapsed = 0;
            if self.check_quorum {
                let m = new_message(INVALID_ID, MessageType::MsgCheckQuorum, Some(self.id));
                has_ready = true;
                let _ = self.step(m);
            }
            if self.state == StateRole::Leader && self.leader_transferee.is_some() {
                self.abort_leader_transfer()
            }
        }

        if self.state != StateRole::Leader {
            return has_ready;
        }

        if self.heartbeat_elapsed >= self.heartbeat_timeout {
            self.heartbeat_elapsed = 0;
            has_ready = true;
            let m = new_message(INVALID_ID, MessageType::MsgBeat, Some(self.id));
            let _ = self.step(m);
        }

        has_ready
    }

    /// Converts this node to a candidate, Panics if a leader node already exists.
    pub fn become_candidate(&mut self) {
        assert_ne!(self.state, StateRole::Leader, "invalid transition [leader -> candidate]");
        let term = self.term + 1;
        self.reset(term);
        let id = self.id;
        self.vote = id;
        self.state = StateRole::Candidate;

        info!(self.logger, "became candidate at term {term}", term = self.term);
    }

    /// Converts this node to a pre-candidate. Panics if a leader node already exists. 
    pub fn become_per_candidate(&mut self) {
        assert_ne!(self.state, StateRole::Leader, "invalid transaction [leader -> pre-candidate]");
        // Becoming a pre-candidate changes our state, but doesn't change anything else, In particular it does not increase self.term or change self.vote
        self.state = StateRole::PreCandidate;
        self.prs.reset_votes();

        // If a network partition happens , and leader is in minotity partition, It will step down , and become follower without notifying others.
        self.leader_id = INVALID_ID;
        info!(self.logger, "become pre_candidate at term {term}", term = self.term);
    }
    
    /// Makes this raft the leader. Panics if this is a follower node.
    pub fn become_leader(&mut self) {
        trace!(self.logger, "enter become leader");
        assert_ne!(self.state, StateRole::Follower, "invalid transition [follower -> leader]");

        let term = self.term;
        self.reset(term);
        self.leader_id = self.id;
        self.state = StateRole::Leader;

        let last_index = self.raft_log.last_idx();
        // If there is only one peer, it becomes leader after compaigning so all logs must be persisted. If not , it becomes leader after sending RequestVote msg 
        // and logs can not be changed when it's (per)candidate. the last index is equal persisited index when it comes leader.
        assert_eq!(last_index, self.raft_log.persisted);

        // update uncommitted_state.
        self.uncommited_state.uncommited_size = 0;
        self.uncommited_state.last_log_tail_index = last_index;
        // Followers enter replicate mode when they've been successfully probed (perhaps after having received a snapshot as a result). The leader is trivially in
        // this state. Note that r.reset() has initialized this progress with the last index already.
        let id = self.id;
        self.mut_prs().get_mut(id).unwrap().become_replcate();
        // Conservatively set the pending_conf_index to the last index in the log. There may or may not be a pending config change, but it's safe to delay any future
        // proposals util we commit all our pending log entries, and scanning the entries tail of the log could be expensive.
        self.pending_conf_index = last_index;

        // No need to check result because append_entry never refuse entries which size is zero.
        if !self.append_entry(&mut [Entry::default()]) {
            panic!("appending an empty entry should never be dropped");
        }

        info!(self.logger, "become leader at term {term}", term = self.term);
        trace!(self.logger, "exit become_leader");
    }

    fn num_pending_conf(&self, ents: &[Entry]) -> usize {
        ents.iter().filter(|e| e.get_entry_type() == EntryType::EntryConfChange || e.get_entry_type() == EntryType::EntryConfChangeV2).count()
    }

    /// Campaign to attempt(视图) to become a leader. If prevote is enabled, this is handled as well.
    #[doc(hidden)]
    pub fn campaign(&mut self, campaign_type: &'static [u8]) {
        let (vote_msg, term) = if campaign_type == CAMPAIGN_PRE_ELECTION {
            self.become_per_candidate();
            // Pre-vote RPCs are sent for next term before we've incremented self.term.
            (MessageType::MsgRequestPreVote, self.term + 1)
        } else{
            self.become_candidate();
            (MessageType::MsgRequestVote, self.term)
        };
        let self_id = self.id;
        if VoteResult::Won == self.poll(self_id, vote_msg, true){
            // We won election after voting for ourselves (which must mean that this is a single-node cluster)
            return;
        }

        let (commit, commit_term) = self.raft_log.commit_info();
        let mut voters = [0; 7];
        let mut voter_cnt = 0;

        //Only send vote request to voters.
        for id in self.prs.conf().voters().ids().iter() {
            if id == self.id {
                continue;
            }

            if voter_cnt == voters.len() {
                self.log_broadcast_vote(vote_msg, &voters);
                voter_cnt = 0;
            }
            voters[voter_cnt] = id;
            voter_cnt += 1;
            let mut m = new_message(id, vote_msg, None);
            m.term = term;
            m.index = self.raft_log.last_idx();
            m.log_term = self.raft_log.last_term();
            m.commit = commit;
            m.commit_term = commit_term;
            if campaign_type == CAMPAIGN_TRANSFER {
                m.context = campaign_type.into();
            }
            self.r.send(m, &mut self.msgs);
        }

        if voter_cnt > 0 {
            self.log_broadcast_vote(vote_msg, &voters[..voter_cnt]);
        }
    }

    #[inline]
    fn log_broadcast_vote(&self, t: MessageType, ids: &[u64]) {
        info!(self.logger, "broadcasting vote request"; "type" => ?t, "term" => self.term, "log_term" => self.raft_log.last_term(), "log_index" => self.raft_log.last_idx(), "to" => ?ids);
    }

    /// `pass_election_timeout` returns true iff `election_elapsed` is greater than or equal to the randomized election timeout in [`election_timeout`, 2 * `election_timeout` - 1].
    pub fn pass_election_timeout(&self) -> bool {
        self.election_elapsed >= self.randomized_election_timeout
    }

    fn poll(&mut self, from: u64, t: MessageType, vote: bool) -> VoteResult {
        self.prs.record_vote(from, vote);
        let (gr, rj, res) = self.prs.tally_votes();
        // Unlike etcd, we log when necessary
        if from != self.id {
            info!(self.logger, "received votes response"; "vote" => vote, "from" => from, "rejections" => rj, "approvals" => gr, "type" => ?t, "term" => self.term);
        }
        match res {
            VoteResult::Lost => {
                // pb.MsgPreVoteResp contains feture term of pre-candidate m.term > self.term; reuse self.term
                let term = self.term;
                self.become_follower(term, INVALID_ID);
            },
            VoteResult::Won => {
                if self.state == StateRole::PreCandidate {
                    self.campaign(CAMPAIGN_ELECTION);
                } else {
                    self.become_leader();
                    self.bcast_append();
                }
            },
            VoteResult::Pending => (),
        }
        res
    }

    /// Steps the raft along via a message. This should be called everytime your raft receives a message from a peer.
    pub fn step(&mut self, m: Message) -> Result<()> {
        // Handle the message term, which may result in our steping down to a follower.
        if m.term == 0 {
            // local meaage
        } else if m.term > self.term {
            if m.get_msg_type()== MessageType::MsgRequestVote || m.get_msg_type() == MessageType::MsgRequestPreVote {
                let force = m.context == CAMPAIGN_TRANSFER;
                let in_lease = self.check_quorum && self.leader_id != INVALID_ID && self.election_elapsed < self.election_timeout;
                if !force && in_lease {
                    //
                    info!(self.logger, "[logterm: {log_term}, index: {log_index}, vote: {vote}] ignored vote from {from} [logterm: {msg_term}, index: {msg_index}]: lease is not expired", 
                        log_term = self.raft_log.last_term(), log_index = self.raft_log.last_idx(), vote = self.vote, from = m.from, msg_term = m.log_term, msg_index = m.index;
                        "term" => self.term, "remaining ticks" => self.election_timeout - self.election_elapsed, "msg type" => ?m.get_msg_type());
                    
                    return Ok(());
                }
            }

            if m.get_msg_type() == MessageType::MsgRequestPreVote || (m.get_msg_type() == MessageType::MsgRequestPreVoteResponse && !m.reject) {
                
            } else {
                info!(self.logger, "received a message with higher term from {from}", from = m.from; "term" => self.term, "message_term" => m.term, "msg type" => ?m.get_msg_type());
                if m.get_msg_type() == MessageType::MsgAppend || m.get_msg_type() == MessageType::MsgHeartbeat || m.get_msg_type() == MessageType::MsgSnapshot {
                    self.become_follower(m.term, m.from);
                } else {
                    self.become_follower(m.term, INVALID_ID);
                }
            }
        } else if m.term < self.term {
            if (self.check_quorum || self.pre_vote) && (m.get_msg_type() == MessageType::MsgHeartbeat || m.get_msg_type() == MessageType::MsgAppend) {
                let to_send = new_message(m.from, MessageType::MsgAppendResponse, None);
                self.r.send(to_send, &mut self.msgs);
            } else if m.get_msg_type() == MessageType::MsgRequestPreVote {
                info!(self.logger, "{} [log_term: {}, index: {}, vote: {}], rejected: {:?}, from: {} [log_term: {}, index: {} at term {}]", 
                self.id, self.raft_log.last_term(), self.raft_log.last_idx(), self.vote, m.get_msg_type(), m.from, m.log_term, m.index, self.term);

                let mut to_send = new_message(m.from, MessageType::MsgRequestPreVoteResponse, None);
                to_send.term = self.term;
                to_send.reject = true;
                self.r.send(to_send, &mut self.msgs);
            } else {
                // ingore other case
                info!(self.logger, "ingore a message with lower term from {from}", from = m.from; "term" => self.term, "msg type" => ?m.get_msg_type(), "mst term" => m.term);
            }
            return Ok(());
        }

        #[cfg(feature = "failpoints")]
        fail_point!("before_step");

        match m.get_msg_type() {
            MessageType::MsgHup => self.hup(false),
            MessageType::MsgRequestVote | MessageType::MsgRequestPreVote => {
                // We can vote if this is a repeat of a vote we've already cast...
                let can_vote = (self.vote == m.from) || (self.vote == INVALID_ID && self.leader_id == INVALID_ID) || (m.get_msg_type() == MessageType::MsgRequestPreVote && m.term > self.term);
                if can_vote && self.raft_log.is_up_to_date(m.index, m.log_term) && (m.index > self.raft_log.last_idx() || self.priority <= m.priority) {
                    //
                    self.log_vote_approve(&m);
                    let mut to_send = new_message(m.from, vote_resp_msg_type(m.get_msg_type()), None);
                    to_send.reject = false;
                    to_send.term = m.term;
                    self.r.send(to_send, &mut self.msgs);
                    if m.get_msg_type() == MessageType::MsgRequestVote {
                        // only recoard realy vote
                        self.election_elapsed = 0;
                        self.vote = m.from;
                    }
                }else {
                    self.log_vote_reject(&m);
                    let mut to_send = new_message(m.from, vote_resp_msg_type(m.get_msg_type()), None);
                    to_send.reject = true;
                    to_send.term = self.term;
                    let (commit, commit_term) = self.raft_log.commit_info();
                    to_send.commit = commit;
                    to_send.commit_term = commit_term;
                    self.r.send(to_send, &mut self.msgs);
                    self.maybe_commit_by_vote(&m);
                }
            },
            _ => match self.state {
                StateRole::Candidate | StateRole::PreCandidate => self.step_candidate(m)?,
                StateRole::Follower => self.step_follower(m)?,
                StateRole::Leader => self.step_leader(m)?,
            }
        }

        Ok(())
    }

    fn log_vote_approve(&self, m: &Message) {
        info!(self.logger, "[logterm: {log_term}, index: {log_index}, vote: {vote}], cast vote for {from} [logterm: {msg_term}, index: {msg_index}] at term {term}",
            log_term = self.raft_log.last_term(), log_index = self.raft_log.last_idx(), vote = self.vote, from = m.from, msg_term = m.log_term, msg_index = m.index,
            term = self.term; "msg type" => ?m.get_msg_type());
    }
    fn log_vote_reject(&self, m: &Message) {
        info!(self.logger, "[logterm: {log_term}, index: {log_index}, vote: {vote}], reject vote from {from} [logterm: {msg_term}, index: {msg_index}] at term {term}",
            log_term = self.raft_log.last_term(), log_index = self.raft_log.last_idx(), vote = self.vote, from = m.from, msg_term = m.log_term, msg_index = m.index,
            term = self.term; "msg type" => ?m.get_msg_type());
    }

    /// Commit the logs using commit info in vote messages.
    fn maybe_commit_by_vote(&mut self, m: &Message) {
        if m.commit == 0 || m.commit_term == 0 {
            return;
        }
        let last_commit = self.raft_log.committed;
        if m.commit <= last_commit || self.state == StateRole::Leader {
            return;
        }
        if !self.raft_log.maybe_commit(m.commit, m.commit_term) {
            return;
        }

        let log = &mut self.r.raft_log;
        info!(self.r.logger, "[commit: {}, lastindex: {}, lastterm: {}] fast-forwarded commit to vote request [index: {}, term: {}]",
            log.committed, log.last_idx(), log.last_term(), m.commit, m.commit_term);
        
        if self.state != StateRole::Candidate && self.state != StateRole::PreCandidate {
            return;
        }

        let ents = self.raft_log
            .slice(last_commit + 1, self.raft_log.committed + 1, None, GetEntriesContext(GetEntriesFor::CommitByVote))
            .unwrap_or_else(|e| {
                fatal!(self.logger, "unexpected error getting unapplied entries [{}, {}): {:?}", last_commit + 1, self.raft_log.committed + 1, e);
            });
        if self.num_pending_conf(&ents) != 0 {
            // The candidate doesn't have to step down in theory(在理论上), here just for best safety as we assume(假设) quorum won't change during election.
            let term = self.term;
            self.become_follower(term, INVALID_ID);
        }
    }

    fn hup(&mut self, transfer_leader: bool) {
        if self.state == StateRole::Leader {
            debug!(self.logger, "ignoring MsgHup because already leader");
            return;
        }

        //
        let first_index = match self.raft_log.unstable.maybe_first_index() {
            Some(idx) => idx,
            None => self.raft_log.applied + 1,
        };

        let ents = self.raft_log
            .slice(first_index, self.raft_log.committed + 1, None, GetEntriesContext(GetEntriesFor::TransferLeader))
            .unwrap_or_else(|e| {
                fatal!(self.logger, "unexpected error getting unapplied entries [{}, {}): {:?}", first_index, self.raft_log.committed + 1, e);
            });
        let n = self.num_pending_conf(&ents);
        if n != 0 {
            warn!(self.logger, "cannot campaign at term {term} since there are still {pending_changes} pending configuration changes to apply", term = self.term, pending_changes = n);
            return;
        }
        info!(self.logger, "starting a new election"; "term" => self.term);
        if transfer_leader {
            self.campaign(CAMPAIGN_TRANSFER);
        } else if self.pre_vote {
            self.campaign(CAMPAIGN_PRE_ELECTION);
        } else {
            self.campaign(CAMPAIGN_ELECTION);
        }
    }

    /// step_candidate is shared by state Candidate and PreCandidate; The difference is whether they respoond to MsgRequestVote or MsgRequestPreVote.
    fn step_candidate(&mut self, m: Message) -> Result<()>{
        match m.get_msg_type() {
            MessageType::MsgPropose => {
                info!(self.logger, "no leader at term {term}; dropping proposal", term = self.term);
                return Err(Error::ProposalDropped);
            },
            MessageType::MsgAppend => {
                debug_assert_eq!(self.term, m.term);
                self.become_follower(m.term, m.from);
                self.handle_append_entries(&m);
            },
            MessageType::MsgHeartbeat => {
                debug_assert_eq!(self.term, m.term);
                self.become_follower(m.term, m.from);
                self.handle_heartbeat(m);
            },
            MessageType::MsgSnapshot => {
                debug_assert_eq!(self.term, m.term);
                self.become_follower(m.term, m.from);
                self.handle_snapshot(m);
            },
            MessageType::MsgRequestPreVoteResponse | MessageType::MsgRequestVoteResponse => {
                // 
                if (self.state == StateRole::PreCandidate && m.get_msg_type() != MessageType::MsgRequestPreVoteResponse) 
                || (self.state == StateRole::Candidate && m.get_msg_type() != MessageType::MsgRequestVoteResponse) {
                    return Ok(());
                }
                self.poll(m.from, m.get_msg_type(), !m.reject);
                self.maybe_commit_by_vote(&m);
            },
            MessageType::MsgTimeoutNow => debug!(self.logger, "{term} ignored MsgTimeoutNow from {from}",term = self.term, from = m.from; "state" => ?self.state),
            _ =>{}
        }

        Ok(())
    }

    fn handle_snapshot(&mut self, mut m: Message) {
        let metadata = m.get_snapshot().get_metadata();
        let (sindex, sterm) = (metadata.index, metadata.term);
        if self.restore(m.take_snapshot()) {
            info!(self.logger, "[commit: {commit}, term: {term}] restored snapshot [index: {snapshot_index}, term: {snapshot_term}]",
                term = self.term, commit = self.raft_log.committed, snapshot_index = sindex, snapshot_term = sterm);
            let mut to_send = Message::default();
            to_send.set_msg_type(MessageType::MsgAppendResponse);
            to_send.to = m.from;
            to_send.index = self.raft_log.last_idx();
            self.r.send(to_send, &mut self.msgs);
        } else {
            info!(self.logger, "[commit: {commit}] ignored snapshot [index: {snapshot_index}, term: {snapshot_term}]",
                commit = self.raft_log.committed, snapshot_index = sindex, snapshot_term = sterm);
            let mut to_send = Message::default();
            to_send.set_msg_type(MessageType::MsgAppendResponse);
            to_send.to = m.from;
            to_send.index = self.raft_log.committed;
            self.r.send(to_send, &mut self.msgs);
        }
    }

    /// Recovers the state machine from a snapshot. It restore the log and the configuration of state machine.
    pub fn restore(&mut self, snap: Snapshot) -> bool {
        if snap.get_metadata().index < self.raft_log.committed {
            return false;
        }   
        if self.state != StateRole::Follower {
            // This is defense-in-depth: if the leader somehow ended up applying a snapshot, it could move into a new term without moving into a follower state .
            // This should never fire , but if it did we'd have prevented damage by returning early, so log only a long warning. At the time of writing, the instance
            // is guaranteed（保证） to be follower state when this method is called.
            warn!(self.logger, "non-follower attempted to restore snapshot"; "state" => ?self.state);
            self.become_follower(self.term + 1, INVALID_INDEX);
            return false;
        }

        // More defense-in-depth: throw away snapshot if recipient(收件人) is not in the config. This shouldn't ever happen(at the time of writing) but lots of code here
        // and there assumes（假定）that r.id is in the progress tracker.
        let meta = snap.get_metadata();
        let (snap_index, snap_term) = (meta.index, meta.term);
        let cs = meta.get_conf_state();
        if cs.get_voters().iter().chain(cs.get_learners()).chain(cs.get_voters_outgoing()).all(|id| *id != self.id) {
            warn!(self.logger, "attempted to restore snapshot but it is not in the ConfState"; "conf_state" => ?cs);
            return false;
        }

        // Now go ahead and actually restore.
        if self.pengding_request_snapshot == INVALID_INDEX && self.raft_log.match_term(meta.index, meta.term) {
            info!(self.logger, "fast-forwarded commit to snapshot"; "commit" => self.raft_log.committed, "last_index" => self.raft_log.last_idx(),
                "last_term" => self.raft_log.last_term(), "snapshot_index" => snap_index, "snapshot_term" => snap_term);
            self.raft_log.commit_to(meta.index);
            return false;
        }
        self.raft_log.restore(snap);
        let cs = self.r.raft_log.pending_snapshot().unwrap().get_metadata().get_conf_state();
        self.prs.clear();
        let last_index = self.raft_log.last_idx();
        if let Err(e) = confchange::restore(&mut self.prs, last_index, cs) {
            // This should never happen. Either there's a bug in our config change handling or the client corrupted(损坏) the config change.
            fatal!(self.logger, "unable to restore config {:?} {}", cs, e);
        }
        let new_cs = self.post_conf_change();
        let cs = self.r.raft_log.pending_snapshot().unwrap().get_metadata().get_conf_state();
        if !raft_proto::conf_state_eq(cs, &new_cs) {
            fatal!(self.logger, "invalid restore {:?} != {:?}", cs, new_cs);
        } 

        let pr = self.prs.get_mut(self.id).unwrap();
        pr.maybe_update(pr.next_idx -1);
        self.pengding_request_snapshot = INVALID_INDEX;

        info!(self.logger, "restored snapshot"; "commit" => self.raft_log.committed, "last_index" => self.raft_log.last_idx(), "last_term" => self.raft_log.last_term(),
            "snapshot_index" => snap_index, "snapshot_term" => snap_term);

        true
    }

    /// For a message, commit and send out heartbeat.
    pub fn handle_heartbeat(&mut self, mut m: Message) {
        self.raft_log.commit_to(m.commit);
        if self.pengding_request_snapshot != INVALID_INDEX {
            self.send_request_snapshot();
            return;
        }
        let mut to_send = Message::default();
        to_send.set_msg_type(MessageType::MsgHeartbeatResponse);
        to_send.to = m.from;
        to_send.context = m.take_context();
        to_send.commit = self.raft_log.committed;
        self.r.send(to_send, &mut self.msgs);
    }
    /// For a given message, append the entries to the log
    pub fn handle_append_entries(&mut self, m: &Message){
        if self.pengding_request_snapshot != INVALID_INDEX {
            self.send_request_snapshot();
            return;
        }
        if m.index < self.raft_log.committed {
            debug!(self.logger, "got message with lower index than committed.");
            let mut to_send = Message::default();
            to_send.set_msg_type(MessageType::MsgAppendResponse);
            to_send.to = m.from;
            to_send.index = self.raft_log.committed;
            to_send.commit = self.raft_log.committed;
            self.r.send(to_send, &mut self.msgs);
            return;
        }

        let mut to_send = Message::default();
        to_send.to = m.from;
        to_send.set_msg_type(MessageType::MsgAppendResponse);

        if let Some((_, last_idx)) = self.raft_log.maybe_append(m.index, m.log_term, m.commit, &m.entries){
            to_send.set_index(last_idx);
        } else {
            debug!(self.logger, "rejected msgApp [logterm: {msg_log_term}, index: {msg_index}] from {from}", msg_log_term = m.log_term, msg_index = m.index,
                from = m.from; "index" => m.index, "logterm" => ?self.raft_log.term(m.index));
            
            let hint_index = cmp::min(m.index, self.raft_log.last_idx());
            let (hint_index, hint_term) = self.raft_log.find_conflict_by_term(hint_index, m.log_term);
            if hint_term.is_none() {
                fatal!(self.logger, "term{index} must be valid", index = hint_index)
            }

            to_send.index = m.index;
            to_send.reject = true;
            to_send.reject_hint = hint_index;
            to_send.log_term = hint_term.unwrap();
        }        
        to_send.set_commit(self.raft_log.committed);
        self.r.send(to_send, &mut self.msgs);
    }

    fn send_request_snapshot(&mut self) {
        let mut m = Message::default();
        m.set_msg_type(MessageType::MsgAppendResponse);
        m.index = self.raft_log.committed;
        m.reject = true;
        m.reject_hint = self.raft_log.last_idx();
        m.to = self.leader_id;
        m.request_snapshot = self.pengding_request_snapshot;
        self.r.send(m, &mut self.msgs);
    }
    /// check_quorum_active returns true if the quorum is active from the view of the local raft state machine. Otherwise, it returns false. check_quorum_active also resets all recent_active to false,
    /// check_quorum_active can only called by leader.
    fn check_quorum_active(&mut self) -> bool {
        let self_id = self.id;
        self.mut_prs().quorum_recently_active(self_id)
    }

    fn step_follower(&mut self, mut m: Message) -> Result<()>{
        match m.get_msg_type() {
            MessageType::MsgPropose => {
                if self.leader_id == INVALID_ID {
                    info!(self.logger, "no leader at term {term}; dropping proposal", term = self.term);
                    return Err(Error::ProposalDropped);
                }
                m.to = self.leader_id;
                self.r.send(m, &mut self.msgs);
            },
            MessageType::MsgAppend => {
                self.election_elapsed = 0;
                self.leader_id = m.from;
                self.handle_append_entries(&m);
            },
            MessageType::MsgHeartbeat => {
                self.election_elapsed = 0;
                self.leader_id = m.from;
                self.handle_heartbeat(m);
            },
            MessageType::MsgSnapshot => {
                self.election_elapsed = 0;
                self.leader_id = m.from;
                self.handle_snapshot(m);
            },
            MessageType::MsgTransferLeader => {
                if self.leader_id == INVALID_ID {
                    info!(self.logger, "no leader at term {term}; dropping leader transfer msg", term = self.term);
                    return Ok(());
                }
                m.to = self.leader_id;
                self.r.send(m, &mut self.msgs);
            },
            MessageType::MsgTimeoutNow => {
                if self.promotable {
                    info!(self.logger,"[term {term}] received MsgTimeoutNow from {from} and starts an election to get leadership.", term = self.term, from = m.from);
                    // Leadership transfers never use pre-vote even if self.pre_vote is true; we know we are not recovering from a partition so there is no need for
                    // the extra round trip.
                    self.hup(true);
                } else {
                    info!(self.logger, "received MsgTimeoutNow from {} but is not promotable", m.from);
                }
            },
            MessageType::MsgReadIndex => {
                if self.leader_id == INVALID_ID {
                    info!(self.logger, "no leader at term {term}; dropping index reading msg", term = self.term);
                    return Ok(());
                }
                m.to = self.leader_id;
                self.r.send(m, &mut self.msgs);
            },
            MessageType::MsgReadIndexResp => {
                if m.entries.len() != 1 {
                    error!(self.logger, "invalid format of MsgReadIndexResp from {}", m.from; "entries.count" => m.entries.len());
                    return Ok(());
                }
                let rs = ReadState {
                    index: m.index,
                    request_ctx: m.take_entries()[0].take_data().to_vec(),
                };
                self.read_states.push(rs);
                // `index` and `term` in MsgReadIndexResp is the leader's commit index and its current term, the log entry in the leader's commit index will always
                // have the leader's current term, because the leader only handle MsgReadIndex after it has committed log entry in its term.
                self.raft_log.maybe_commit(m.index, m.term);
            },
            _ => {}

        }
        Ok(())
    }

    fn step_leader(&mut self, mut m: Message) -> Result<()> {
        // These message types do not require any progress for m.from
        match m.get_msg_type() {
            MessageType::MsgBeat => {
                self.bcast_heartbeat();
                return Ok(());
            },
            MessageType::MsgCheckQuorum => {
                if !self.check_quorum_active() {
                    warn!(self.logger, "stepped down to follower since quorum is not active");
                    let term = self.term;
                    self.become_follower(term, INVALID_ID);
                }
                return Ok(());
            },
            MessageType::MsgPropose => {
                if m.entries.is_empty() {
                    fatal!(self.logger, "stepped empty MsgProp");
                }
                if !self.prs.progress().contains_key(&self.id) {
                    // 
                    return Err(Error::ProposalDropped);
                }
                if self.leader_transferee.is_some() {
                    debug!(self.logger, "[term {term}] transfer leadership to {lead_transferee} is in progress; dropping proposal", term = self.term, lead_transferee = self.leader_transferee.unwrap());
                    return Err(Error::ProposalDropped);
                }
                
                for (i, e) in m.mut_entries().iter_mut().enumerate() {
                    let mut cc;
                    if e.get_entry_type() == EntryType::EntryConfChange {
                        let mut cc_v1 = ConfChange::default();
                        if let Err(e) = cc_v1.merge_from_bytes(e.get_data()) {
                            error!(self.logger, " invalid confchange"; "error" => ?e);
                            return Err(Error::ProposalDropped);
                        }
                        cc = cc_v1.into_v2();
                    } else if e.get_entry_type() == EntryType::EntryConfChangeV2 {
                        cc = ConfChangeV2::default();
                        if let Err(e) = cc.merge_from_bytes(e.get_data()) {
                            error!(self.logger, "invalid confchangev2"; "error" => ?e);
                            return Err(Error::ProposalDropped);
                        }
                    } else {
                        continue;
                    }
                    
                    let reason = if self.has_pending_conf() {
                        "possible unapplied conf change"
                    }else{
                        let already_joint = confchange::joint(self.prs.conf());
                        let want_leave = cc.changes.is_empty();
                        if already_joint && ! want_leave {
                            "must transition out of joint config first"
                        }else if !already_joint && want_leave{
                            "not in joint state; refusing empty conf change"
                        } else {
                            ""
                        }
                    };

                    if reason.is_empty() {
                        self.pending_conf_index = self.raft_log.last_idx() + i as u64 + 1;
                    } else {
                        info!(self.logger, "ingoring conf change"; "conf change" => ?cc, "reason" => reason, "config" => ?self.prs.conf(), 
                        "index" => self.pending_conf_index, "applied" => self.raft_log.applied);
                        *e = Entry::default();
                        e.set_entry_type(EntryType::EntryNormal);
                    }
                }
                if !self.append_entry(m.mut_entries()) {
                    //return ProposalDropped when uncommitted size limmit is reached. 
                    debug!(self.logger, "entries are dropped due overlimit of max uncommitted size, uncommitted_size: {}", self.uncommitted_size());
                    return Err(Error::ProposalDropped);
                }
                self.bcast_append();
                return Ok(());
            },
            MessageType::MsgReadIndex => {
                if !self.commit_to_current_term() {
                    // Reject(拒绝) read only request when this leader has not committed any log entry in its term.
                    return Ok(());
                }

                if self.prs().is_singleton() {
                    let read_index = self.raft_log.committed;
                    if let Some(m) = self.handle_ready_read_index(m, read_index) {
                        self.r.send(m, &mut self.msgs);
                    }
                    return Ok(());
                }

                //
                match self.read_only.option {
                    ReadOnlyOption::Safe => {
                        let ctx = m.entries[0].data.to_vec();
                        self.r.read_only.add_request(self.r.raft_log.committed, m, self.r.id);
                        self.bcast_heartbeat_with_ctx(Some(ctx));
                    },
                    ReadOnlyOption::LeaseBased => {
                        let read_index = self.raft_log.committed;
                        if let Some(m) = self.handle_ready_read_index(m, read_index) {
                            self.r.send(m, &mut self.msgs);
                        }
                    },
                }
                return Ok(());
            },
            _ => {},
        }

        match m.get_msg_type() {
            MessageType::MsgAppendResponse => {
                self.handle_append_response(&m);
            },
            MessageType::MsgHeartbeatResponse => {
                self.handle_heartbeat_response(&m);
            },
            MessageType::MsgSnapStatus => {
                self.handle_snap_status(&m);
            },
            MessageType::MsgUnreachable => {
                self.handle_unreachable(&m);
            },
            MessageType::MsgTransferLeader => {
                self.handle_transfer_leader(&m);
            },
            _ => {
                if self.prs.get(m.from).is_none() {
                    debug!(self.logger, "no progress avaiable for {}", m.from);
                }
            }
        }
        Ok(())
    }

    /// Return current uncommitted size recorded by uncommitted_state.
    #[inline]
    pub fn uncommitted_size(&self) -> usize {
        self.uncommited_state.uncommited_size
    }

    /// Returns a read-only reference to the progress set.
    pub fn prs(&self) -> &ProgressTracker {
        &self.prs
    }

    fn handle_append_response(&mut self, m: &Message) {
        let mut next_probe_index = m.reject_hint;
        // pull out find_conflict_by_term for imutable borrow.
        if m.reject && m.log_term > 0 {
            // 
            next_probe_index = self.raft_log.find_conflict_by_term(m.reject_hint, m.log_term).0;
        }

        let pr = match self.prs.get_mut(m.from) {
            Some(pr) => pr,
            None => {
                debug!(self.logger, "no progress available for {}", m.from);
                return;
            }
        };
        pr.recent_active = true;
        // update follower committed index via append response
        pr.update_commited(m.commit);

        if m.reject {
            // 
            debug!(self.r.logger, "received msgAppend rejection"; "reject_hint_index" => m.reject_hint, "retject_hint_term" => m.log_term, "from" => m.from, "index" => m.index);
            if pr.maybe_decr_to(m.index, next_probe_index, m.request_snapshot) {
                debug!(self.r.logger, "decreased progress of {}", m.from; "progress" => ?pr);

                if pr.state == ProgressState::Replicate {
                    pr.become_probe();
                }
                self.send_append(m.from);
            }
             return;
        }

        let old_paused = pr.is_paused();
        if !pr.maybe_update(m.index) {
            return;
        }

        match pr.state {
            ProgressState::Probe => pr.become_replcate(),
            ProgressState::Snapshot => {
                if pr.maybe_snapshot_abort() {
                    debug!(self.r.logger, "snapshot aborted, resumed sending replication message to {from}", from = m.from; "progerss" => ?pr);
                    pr.become_probe();
                }
            },
            ProgressState::Replicate => pr.ins.free_to(m.get_index()),
        }

        if self.maybe_commit() {
            if self.shuld_bcast_commit() {
                self.bcast_append();
            }
        }else if old_paused {
            self.send_append(m.from);
        }

        //
        self.send_append_aggresively(m.from);

        // Transfer leadership is in progress.
        if Some(m.from) == self.r.leader_transferee {
            let last_index = self.raft_log.last_idx();
            let pr = self.prs.get_mut(m.from).unwrap();
            if pr.matched == last_index {
                info!(self.logger, "sent MsgTimeoutNow tp {from} after received MsgAppResp", from = m.from);
                self.send_timeout_now(m.from);
            }
        }
       
    }

    /// Issues a message to timeout immediately(立即发出超时消息).
    pub fn send_timeout_now(&mut self, to: u64){
        let msg = new_message(to, MessageType::MsgTimeoutNow, None);
        self.r.send(msg, &mut self.msgs);
    }

    fn handle_heartbeat_response(&mut self, m: &Message) {
        // Update the node, Drop the value explicitly(显示) since we'll check quorum after.
        let pr = match self.prs.get_mut(m.from) {
            Some(pr) => pr,
            None => {
                debug!(self.logger, "no progress available for {}",m.from);
                return;
            }
        };

        // update followers committed index via heartbeat response.
        pr.update_commited(m.commit);
        pr.recent_active = true;
        pr.resume();

        //
        if pr.state == ProgressState::Replicate && pr.ins.full(){
            pr.ins.free_first_one();
        }
        // Does it request snapshot?
        if pr.matched < self.r.raft_log.last_idx() || pr.pending_request_snapshot != INVALID_INDEX{
            self.r.send_append(m.from, pr, &mut self.msgs);
        }
        if self.read_only.option != ReadOnlyOption::Safe || m.context.is_empty(){
            return;
        }

        match self.r.read_only.recv_ack(m.from, &m.context) {
            Some(acks) if self.prs.has_quorum(acks) => {},
            _ => return
        }
        for rs in self.r.read_only.advance(&m.context, &self.r.logger) {
            if let Some(m) = self.handle_ready_read_index(rs.req, rs.index) {
                self.r.send(m, &mut self.msgs);
            }
        }
    }

    fn handle_snap_status(&mut self, m: &Message) {
        let pr = match self.prs.get_mut(m.from) {
            Some(pr) => pr,
            None => {
                debug!(self.logger, "no progress available for {}",m.from);
                return;
            }
        };

        if pr.state != ProgressState::Snapshot {
            return;
        }
        if m.reject {
            pr.snapshot_failure();
            pr.become_probe();
            debug!(self.r.logger, "snapshot failed, resumed sending replication messages to {from}",from = m.from; "progerss" => ?pr);
        } else {
            pr.become_probe();
            debug!(self.r.logger, "snapshot succeeded, resumed sending replication messages to {from}",from = m.from; "progerss" => ?pr);
        }
        
        // If sanpshot finish, wait for the msgAppResp from the remote node before sending out the next msgAppend. If snapshot failure, wiat for a heartbeat interval
        // before next try.
        pr.pause();
        pr.pending_request_snapshot = INVALID_INDEX;
    }

    fn handle_unreachable(&mut self, m: &Message) {
        let pr = match self.prs.get_mut(m.from) {
            Some(pr) => pr,
            None => {
                debug!(self.logger, "no progress available for {}", m.from);
                return;
            }
        };

        // During optimistic(乐观) replication, if the remote becomes unreachable, There is huge probability that a MsgAppend is lost.
        if pr.state == ProgressState::Replicate {
            pr.become_probe();
        }
        debug!(self.r.logger, "failed to send message to {from} because it is unreachable", from = m.from; "progress" => ?pr);
    }

    fn handle_transfer_leader(&mut self, m: &Message) {
        if self.prs.get(m.from).is_none() {
            debug!(self.logger, "no progress available for {}", m.from);
            return;
        }

        let from = m.from;
        if self.prs.conf().learners.contains(&from) {
            debug!(self.logger, "ignored transferring leadership"; "to" => from);
            return;
        }
        let lead_transferee = from;
        if let Some(last_lead_transferee) = self.leader_transferee {
            if last_lead_transferee == lead_transferee {
                info!(self.logger, "[term {term}] transfer leadership to {lead_transferee} is in progress, ignores request to same node {lead_transferee}",
                    term = self.term, lead_transferee = lead_transferee);

                return;
            }
            self.abort_leader_transfer();
            info!(self.logger, "[term {term}] abort previous transferring leadership to {last_lead_transferee}", term = self.term, last_lead_transferee = last_lead_transferee);
        }
        if lead_transferee == self.id {
            debug!(self.logger, "already leader, ignored transferring leadership to self");
            return;
        }
        // Transfer leadership to third party.
        info!(self.logger, "[term {term}] starts to transfer leadership to {lead_transferee}", term = self.term, lead_transferee = lead_transferee);
        // Transfer leadership should be finished in one electionTimeout so reset r.electionElapsed.
        self.election_elapsed = 0;
        self.leader_transferee = Some(lead_transferee);
        let pr = self.prs.get_mut(from).unwrap();
        if pr.matched == self.r.raft_log.last_idx() {
            self.send_timeout_now(lead_transferee);
            info!(self.logger, "sends MsgTimeoutNow to {lead_transferee} immediately as {lead_transferee} already has up-to-date log", lead_transferee = lead_transferee);
        } else {
            self.r.send_append(lead_transferee, pr, &mut self.msgs);
        }

    }

    /// Reduce(减少) size of 'ents' from uncommitted size.
    pub fn reduce_uncommitted_size(&mut self, ents: &[Entry]) {
        // fast path for non-leader endpoint
        if self.state != StateRole::Leader {
            return;
        }

        if !self.uncommited_state.maybe_reduce_uncommited_size(ents) {
            // this will make self.uncommitted size not accurate(不准确). but in most situation, this behavior(这种行为) will not cause big problem.
            warn!(self.logger, "try to reduce uncommitted size less than 0, first index of pending ents is {}", ents[0].get_index());
        }
    }

    /// A raft leader allocates a vector with capacity `max_inflight_msgs` for every peer. It takes a lot of memory if there are too many Raft groups.
    /// `maybe_free_inflight_buffers` is used to free memory if necessary.
    pub fn maybe_free_inflight_buffers(&mut self) {
        for (_, pr) in self.mut_prs().iter_mut() {
            pr.ins.maby_free_buffer();
        }
    }

    /// To adjust `max_inflight_msgs` for the specified peer. Set to `0` will disable the progress.
    pub fn adjust_max_inflight_msgs(&mut self, target: u64, cap: usize) {
        if let Some(pr) = self.mut_prs().get_mut(target) {
            pr.ins.set_cap(cap);
        }
    }

    #[doc(hidden)]
    pub fn apply_conf_change(&mut self, cc: &ConfChangeV2) -> Result<ConfState>{
        let mut changer = Changer::new(&self.prs);
        let (cfg, changes) = if cc.leave_joint()  {
            changer.leave_joint()?
        }else if let Some(auto_leave) = cc.enter_joint() {
            changer.enter_joint(auto_leave, &cc.changes)?
        } else{
            changer.simple(&cc.changes)?
        };
        self.prs.apply_conf(cfg, changes, self.raft_log.last_idx());

        Ok(self.post_conf_change())
    }

    /// Request a snapshot from leader.
    pub fn request_snapshot(&mut self, request_index: u64) -> Result<()>{
        if self.state == StateRole::Leader {
            info!(self.logger, "can not request snapshot on leader; dropping request snapshot");
        } else if self.leader_id == INVALID_ID {
            info!(self.logger, "drop request snapshot because of no leader"; "term" => self.term);
        }else if self.snap().is_some() {
            info!(self.logger, "there is a pending snapshot; dropping request snapshot");
        } else if self.pengding_request_snapshot != INVALID_INDEX {
            info!(self.logger, "there is a pending snapshot; dropping request snapshot");
        } else {
            self.pengding_request_snapshot = request_index;
            self.send_request_snapshot();
            return Ok(());
        }

        Err(Error::RequestSnapshotDropped)
    }

}
