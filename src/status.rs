

use crate::{eraftpb::HardState, raft::SoftState, ProgressTracker, Storage, Raft, StateRole};


/// Represents the current status of the raft.
#[derive(Default)]
pub struct Status<'a>{
    /// The ID of current node.
    pub id: u64,
    /// The hardstate of raft, represents voted state.
    pub hs: HardState,
    /// The softstate of raft, represents proposed state.
    pub ss: SoftState,
    /// The index of the last entry to have been applied(已提交).
    pub applied: u64,
    /// The progress towards chatching up and applying log.
    pub progress: Option<&'a ProgressTracker>,
}

impl <'a> Status<'a> {
    /// Gets a copy of the current raft status.
    pub fn new<T: Storage>(raft: &'a Raft<T>) -> Status<'a>{
        let mut status = Status{ id: raft.id, ..Default::default() };
        status.hs = raft.hard_state();
        status.ss = raft.soft_state();
        status.applied = raft.raft_log.applied;
        if status.ss.raft_state == StateRole::Leader {
            status.progress = Some(raft.prs());
        }
        status
    }
}