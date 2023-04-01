use raft_proto::prelude::{ConfState, ConfChangeSingle, ConfChangeType};
use crate::{ProgressTracker, Result};
use super::changer::Changer;


/// Restore takes a Changer (which must represent an empty configuration), and runs a sequence of changes enacting the configuration described in the ConfState.
pub fn restore(tracker: &mut ProgressTracker, next_idx: u64, cs: &ConfState) -> Result<()>{
    let(outgoing, incoming) = to_conf_change_single(cs);
    if outgoing.is_empty() {
        for i in incoming  {
            let (cfg, changes) = Changer::new(tracker).simple(&[i])?;
            tracker.apply_conf(cfg, changes, next_idx);
        }
    }else {
        for cc in outgoing {
            let (cfg, changes) = Changer::new(tracker).simple(&[cc])?;
            tracker.apply_conf(cfg, changes, next_idx);
        }
        let (cfg, changes) = Changer::new(tracker).enter_joint(cs.auto_leave, &incoming)?;
        tracker.apply_conf(cfg, changes, next_idx);
    }

    Ok(())
}


fn to_conf_change_single(cs: &ConfState) -> (Vec<ConfChangeSingle>, Vec<ConfChangeSingle>) {
    let mut incoming = Vec::new();
    let mut outgoing = Vec::new();

    for id in cs.get_voters_outgoing() {
        // If there are outgoing voters, first and them one by one so that the (non-joint) config has them all.
        outgoing.push(raft_proto::new_conf_change_single(*id, ConfChangeType::AddNode));
    }

    // We're done constructing the outgoing slice. now on to the incoming one (which will apply on top of the config created by the outgoing slice).

    // First, we'll remove all of the outgoing voters.
    for id in cs.get_voters_outgoing() {
        incoming.push(raft_proto::new_conf_change_single(*id, ConfChangeType::RemoveNode));
    }
    for id in cs.get_voters() {
        incoming.push(raft_proto::new_conf_change_single(*id, ConfChangeType::AddNode));
    }
    for id in cs.get_learners() {
        incoming.push(raft_proto::new_conf_change_single(*id, ConfChangeType::AddLearnerNode));
    }

    // Same for LearnersNext; these are nodes we want to be learners but which are currently voters in the outgoing config.
    for id in cs.get_learners_next() {
        incoming.push(raft_proto::new_conf_change_single(*id, ConfChangeType::AddLearnerNode));
    }
    (outgoing, incoming)
}