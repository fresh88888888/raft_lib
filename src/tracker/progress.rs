use std::cmp;

use crate::{Inflights, ProgressState, INVALID_INDEX};

/// The progress of catching up from a restart
#[derive(Debug, Clone, PartialEq)]
pub struct Progress {
    /// How much state is matched.
    pub matched: u64,
    /// The next index to apply.
    pub next_idx: u64,
    /// When in ProgressStateProbe, leader sends at most one replication message per heartbeat interval, It also probes actual progress of the follower.
    /// When in ProgressStateReplicate, leader optimistically increase next to the latest entry sent after sending replication message. This is an optimized state for fast replicating log entries
    /// to the follower.
    /// When in ProgressStateSnapshot, leader should have sent out snapshot before and stop sending any replication message.
    pub state: ProgressState,
    /// Paused is used in ProgressStateProbe. When Paused is true raft should pause sending replication message to this peer.
    pub paused: bool,
    /// This field is used in ProgressStateSnapshot. If there is a pending snapshot, the pendingsnapshot will be set to the index of snapshot, If penndingsnapshot is set, the replication process of
    /// this progress will be paused, raft will not resend snapshot until the pengding one is reported to be failed.
    pub pending_snapshot: u64,
    /// This field is used in request snapshot. If there is pending request snapshot, This will be set to the request index of the snapshot.
    pub pending_request_snapshot: u64,
    /// This is true if the progress is recently avtive. Receiving any messages from the corresponding follower indicatesthe progress is active. RecentAvtive can be reset to false after an election
    /// timeout.
    pub recent_active: bool,
    /// Inflights is a sliding window for the inflight messages. When inflights is full, no more message shuold be sent. When a leader sends out a message, index of the last entry should be added to
    /// inflights, The index must be added into inflights in order. When a leader receives a reply, the previous inflights should be freed by callings inflights.freeTo.
    pub ins: Inflights,
    /// Only log replicated to different group will be commited if any group is configured.
    pub commit_group_id: u64,
    /// Committed index in raft_log
    pub commited_index: u64,
}

impl Progress {
    /// Create new Progress with the given settings
    pub fn new(next_idx: u64, ins_size: usize) -> Self {
        Progress {
            matched: 0,
            next_idx,
            state: ProgressState::default(),
            paused: false,
            pending_snapshot: 0,
            pending_request_snapshot: 0,
            recent_active: false,
            ins: Inflights::new(ins_size),
            commit_group_id: 0,
            commited_index: 0,
        }
    }

    fn reset_state(&mut self, state: ProgressState) {
        self.paused = false;
        self.pending_snapshot = 0;
        self.state = state;
        self.ins.reset();
    }

    pub(crate) fn reset(&mut self, next_idx: u64) {
        self.matched = 0;
        self.next_idx = next_idx;
        self.state = ProgressState::default();
        self.paused = false;
        self.pending_request_snapshot = INVALID_INDEX;
        self.pending_snapshot = 0;
        self.recent_active = false;
        self.ins.reset();
    }

    /// Change the progress to a probe
    pub fn become_probe(&mut self) {
        // If the orignal state is ProgressStateSnapshot, progress knows that the pending snapshot has been to this peer successfully, Then probes(探测) from pendingSnapshot + 1.
        if self.state == ProgressState::Snapshot {
            let pending_snapshot = self.pending_snapshot;
            self.reset_state(ProgressState::Probe);
            self.next_idx = cmp::max(self.matched + 1, pending_snapshot + 1);
        } else {
            self.reset_state(ProgressState::Probe);
            self.next_idx = self.matched + 1;
        }
    }

    /// Change the progress to a replicate
    #[inline]
    pub fn become_replcate(&mut self) {
        self.reset_state(ProgressState::Replicate);
        self.next_idx = self.matched + 1;
    }

    /// Change the progress to a snapshot
    #[inline]
    pub fn become_snapshot(&mut self, snapshot_idx: u64) {
        self.reset_state(ProgressState::Snapshot);
        self.next_idx = snapshot_idx;
    }

    /// Sets the snapshot to failure.
    #[inline]
    pub fn snapshot_failure(&mut self) {
        self.pending_snapshot = 0;
    }

    /// Unsets pendingSnapshot if Match is equal or higher than the pengingSnapshot.
    #[inline]
    pub fn maybe_snapshot_abort(&self) -> bool {
        self.state == ProgressState::Snapshot && self.matched >= self.pending_snapshot
    }

    /// Returns false if the given n index comes from an outdated message. Otherwise it updates the progress and return true.
    pub fn maybe_update(&mut self, n: u64) -> bool {
        let need_update = self.matched < n;
        if need_update {
            self.matched = n;
            self.resume();
        }
        if self.next_idx < n + 1 {
            self.next_idx = n + 1;
        }

        need_update
    }

    /// Update commited_index
    pub fn update_commited(&mut self, commited_index: u64) {
        if commited_index > self.commited_index {
            self.commited_index = commited_index;
        }
    }

    /// Optimistically advance the index
    pub fn optimistic_update(&mut self, n: u64) {
        self.next_idx = n + 1;
    }

    /// Returns false if the given index comes from an out of order message, Otherwise it decreases the progress next index to min(reject, last) and return true.
    pub fn maybe_decr_to(&mut self, rejected: u64, match_hint: u64, request_snapshot: u64) -> bool {
        if self.state == ProgressState::Replicate {
            // the rejection must be stale if the progress has matched and "rejected" is smaller than "match". Or rejected equals to matched and request_snapshot is the INVALID_INDEX
            if rejected < self.matched || (rejected == self.matched && request_snapshot == INVALID_INDEX){
                return false;
            }
            if request_snapshot == INVALID_INDEX {
                self.next_idx = self.matched + 1;
            } else {
                self.pending_request_snapshot = request_snapshot;
            }

            return true;
        }

        // The rejection must be stale if "rejected" does not match next - 1. Do not consider it stale if it a request snapshot message.
        if (self.next_idx == 0 || self.next_idx - 1 != rejected) && request_snapshot == INVALID_INDEX{
            return false;
        }

        // Do not decrease next index if it's requesting snapshot.
        if request_snapshot == INVALID_INDEX {
            self.next_idx = cmp::min(rejected, match_hint + 1);
            if self.next_idx < 1 {
                self.next_idx = 1;
            }
        } else if self.pending_request_snapshot == INVALID_INDEX {
            // allow request snapshot even if it's not Replicate.
            self.pending_request_snapshot = request_snapshot;
        }
        self.resume();
        true
    }

    /// Determine whether progress is paused
    #[inline]
    pub fn is_paused(&self) -> bool {
        match self.state {
            ProgressState::Probe => self.paused,
            ProgressState::Replicate => self.ins.full(),
            ProgressState::Snapshot => true,
        }
    }

    /// Resume progress
    #[inline]
    pub fn resume(&mut self) {
        self.paused = false
    }

    /// Pause progress
    #[inline]
    pub fn pause(&mut self) {
        self.paused = true;
    }

    /// Update inflight msg and next_idx
    pub fn update_state(&mut self, last: u64) {
        match self.state {
            ProgressState::Replicate => {
                self.optimistic_update(last);
                self.ins.add(last);
            }
            ProgressState::Probe => self.pause(),
            ProgressState::Snapshot => {
                panic!("update progress state in unhandled state {:?}", self.state)
            }
        }
    }
}
