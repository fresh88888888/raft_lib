
use crate::{Error, Result};
use crate::tracker::{Configuration ,ProgressMap, ProgressTracker};
use crate::eraftpb::{ConfChangeSingle, ConfChangeType};

/// Change log for progress map
pub enum MapChangeType {
    Add,
    Remove,
}

/// Changes made by `Changer`.
pub type MapChange = Vec<(u64, MapChangeType)>;

/// A map that stores updates instead of apply them directly.
pub struct IncrChangeMap<'a> {
    changes: MapChange,
    base: &'a ProgressMap,
}

impl IncrChangeMap<'_> {
    pub fn into_change(self) -> MapChange {
        self.changes
    }

    fn contains(&self, id: u64) -> bool {
        match self.changes.iter().rfind(|(i, _)| *i == id) {
            Some((_, MapChangeType::Add)) => true,
            Some((_, MapChangeType::Remove)) => false,
            None => self.base.contains_key(&id),
        }
    }
}

/// Changer facilitates(方便) configuration changes. It exposes(公布) methods to handle simple and joint consensus(联合共识) while performing the proper(适当的) 
/// vaildation that allows refusing invalid configuration changes bedfore they affect the active.
pub struct Changer<'a> {
    tracker: &'a ProgressTracker,
}

impl Changer<'_> {
    /// Creater a changer.
    pub fn new(tracker: &ProgressTracker) -> Changer {
        Changer { tracker }
    }

    /// Verifies that the outgoing (=right) majority config of the joint
    /// config is empty and initializes it with a copy of the incoming (=left)
    /// majority config. That is, it transitions from
    /// ```text
    ///     (1 2 3)&&()
    /// ```
    /// to
    /// ```text
    ///     (1 2 3)&&(1 2 3)
    /// ```.
    ///
    /// The supplied changes are then applied to the incoming majority config,
    /// resulting in a joint configuration that in terms of the Raft thesis[1]
    /// (Section 4.3) corresponds to `C_{new,old}`.
    ///
    pub fn enter_joint(&self, auto_leave: bool, ccs: &[ConfChangeSingle]) -> Result<(Configuration, MapChange)> {
        if super::joint(self.tracker.conf()) {
            return Err(Error::ConfigChangeError("config is already joint".to_owned()));
        }
        let (mut cfg, mut prs) = self.check_and_copy()?;
        if cfg.voters().incoming.is_empty() {
            // We allow adding nodes to an empty config for convenience (testing and bootstrap), but you can't enter joint state.
            return Err(Error::ConfigChangeError("can't make a zero-voter config joint".to_owned()));
        }
        cfg.voters.outgoing.extend(cfg.voters.incoming.iter().cloned());
        self.apply(&mut cfg, &mut prs, ccs);
        cfg.auto_leave = auto_leave;
        check_invariants(&cfg, &prs)?;
        Ok((cfg, prs.into_change()))

    }

    /// Transitions out of a joint configuration. It is an error to call this method if the configuration is not joint, i.e. if the outgoing majority config is empty
    /// The outgoing majority config of the joint configuration will be removed, that is, The incoming config is promoted as the sole decision maker. In the notation
    /// of the Raft thesis[1] (Section 4.3), this method transitions from `C_{new, old}` into `C_new`. At the same time, any staged learners (learnersNext) the 
    /// addition of which was held back by an overlapping voter in the former outgoing config will be inserted into learners.
    /// [1]: https://github.com/ongardie/dissertation/blob/master/online-trim.pdf
    pub fn leave_joint(&self) -> Result<(Configuration, MapChange)> {
        if !super::joint(self.tracker.conf()) {
            return Err(Error::ConfigChangeError("can't leave a non-joint config".to_owned()));
        }
        let (mut cfg, mut prs) = self.check_and_copy()?;
        if cfg.voters().outgoing.is_empty() {
            return Err(Error::ConfigChangeError(format!("configuration is not joint: {:?}", cfg)));
        }
        cfg.learners.extend(cfg.learners_next.drain());
        for id in &*cfg.voters.outgoing {
            if !cfg.voters.incoming.contains(id) && !cfg.learners.contains(id){
                prs.changes.push((*id, MapChangeType::Remove));
            }
        }

        cfg.voters.outgoing.clear();
        cfg.auto_leave = false;
        check_invariants(&cfg, &prs)?;
        Ok((cfg, prs.into_change()))
    }

    /// Carries out a series of configuration changes that (in aggregate) mutates the incoming majority config `Voters[0]` by at most one, This method will return
    /// an error if that if that is not the case, if the resulting quorum is zero, or if the configuration is in a joint state (i.e. if there is an outgoing 
    /// configuration).
    pub fn simple(&mut self, ccs: &[ConfChangeSingle]) -> Result<(Configuration, MapChange)> {
        if super::joint(self.tracker.conf()) {
            return Err(Error::ConfigChangeError("can't apply simple config change in joint config".to_owned()));
        }
        let (mut cfg, mut prs) = self.check_and_copy()?;
        self.apply(&mut cfg, &mut prs, ccs);
        if cfg.voters.incoming.symmetric_difference(&self.tracker.conf().voters.incoming).count() > 1 {
            return Err(Error::ConfigChangeError("more than one voter changed without entering joint config".to_owned()));
        }

        check_invariants(&cfg, &prs);
        Ok((cfg, prs.into_change()))
    }

    /// Applies a change to the configuration. By convention(根据惯例), changes to voters are always made to the incoming majority config. Outgoing is either 
    /// empty or preserves(保留的) the outgoing majority configuration while in a joint state.
    pub fn apply(&self, cfg: &mut Configuration, prs: &mut IncrChangeMap, ccs: &[ConfChangeSingle]) -> Result<()>{
        for cc in ccs {
            if cc.node_id == 0 {
                // Replaces the NodeID with zero if it decides (downstream of raft) to not apply a change, so we have to have explicit code here to ingore these
                continue;
            }
            match cc.get_change_type() {
                ConfChangeType::RemoveNode => self.remove(cfg, prs, cc.node_id),
                ConfChangeType::AddNode => self.make_voter(cfg, prs, cc.node_id),
                ConfChangeType::AddLearnerNode => self.make_learner(cfg, prs, cc.node_id),
            }
        }

        if cfg.voters().incoming.is_empty() {
            return Err(Error::ConfigChangeError("removed all voters".to_owned()));
        }

        Ok(())
    }

    /// Makes the given ID a learner or satge it to be a learner once an active joint configuration is exited. The former happens when the peer is not a part of 
    /// outgoing config, in which case we either add a new learner or demote a voter in the incoming config. The latter case occurs when the configuration is joint
    /// and the peer is a voter in the outgoing config. In that case, we do not want to add the peer as a learner because then we'd have to track a peer as a voter
    /// and learner simultaneously. Instead, we add the learner to LearnersNext, so that it will be added to learners the moment the outgoing config is removed by 
    /// LeaveJoint.
    fn make_learner(&self,  cfg: &mut Configuration, prs: &mut IncrChangeMap, id: u64) {
        if !prs.contains(id) {
            self.init_progress(cfg, prs, id, true);
            return;
        }
        if cfg.learners.contains(&id) {
            return;
        }

        cfg.voters.incoming.remove(&id);
        cfg.learners.remove(&id);
        cfg.learners_next.remove(&id);

        // Use LearnerNext if we can't add the learner to learners directly, i.e. if the peer is still tracked as a voter in the outgoing config. It will be turned
        // into a learner in LeaveJoint(). Otherwise, Add a regular learner right away.
        if cfg.voters().outgoing.contains(&id) {
            cfg.learners_next.insert(id);
        } else {
            cfg.learners.insert(id);
        }
    }

    fn make_voter(&self,  cfg: &mut Configuration, prs: &mut IncrChangeMap, id: u64){
        if !prs.contains(id) {
            self.init_progress(cfg, prs, id, false);
            return;
        }

        cfg.voters.incoming.insert(id);
        cfg.learners.remove(&id);
        cfg.learners_next.remove(&id);
    }

    fn init_progress(&self,  cfg: &mut Configuration, prs: &mut IncrChangeMap, id: u64, is_learner: bool) {
        if !is_learner {
            cfg.voters.incoming.insert(id);
        }else {
            cfg.learners.insert(id);
        }

        prs.changes.push((id, MapChangeType::Add));
    }

    fn remove(&self, cfg: &mut Configuration, prs: &mut IncrChangeMap, id: u64) {
        if !prs.contains(id) {
            return;
        }
         
        cfg.voters.incoming.remove(&id);
        cfg.learners.remove(&id);
        cfg.learners_next.remove(&id);

        if !cfg.voters.outgoing.contains(&id) {
            prs.changes.push((id, MapChangeType::Remove));
        }
    }

    /// Copies the tracker's config. It return an error if check_invariants does. Unlike etcd, we don't copy progress as we don't need to mutate the `is_learner` 
    /// flags. Additions and Removals should be done after everything is checked OK.
    fn check_and_copy(&self) -> Result<(Configuration, IncrChangeMap)>{
        let prs = IncrChangeMap{ changes: vec![], base: self.tracker.progress() };
        check_invariants(self.tracker.conf(), &prs)?;
        Ok((self.tracker.conf().clone(), prs))
    }

}

/// Make sure that the config and progress and compatible with each other. This is used to check both what the Changer is initialized with, as well as what is returns.
fn check_invariants(cfg: &Configuration, prs: &IncrChangeMap) -> Result<()> {
    // NB: intentionally allow the empty config. In production we'll never see a non-empty config (we prevent it from being created) but we will need to be able to 
    // *create* an initial config , for example during bootstrap (or during tests). instead of having to hand-code this, we allow transitioning from a empty config
    // into any other legal and non-empty config.
    for id in cfg.voters().ids().iter() {
        if !prs.contains(id) {
            return Err(Error::ConfigChangeError(format!("no progress for voter {}", id)));
        }
    }
    for id in &cfg.learners {
        if !prs.contains(*id) {
            return Err(Error::ConfigChangeError(format!("no progress for learner {}", id)));
        }
        // Conversely learners and Voters doesn't intersect at all
        if cfg.voters().outgoing.contains(id) {
            return Err(Error::ConfigChangeError(format!("{} is in learners and outgoing voters", id)));
        }
        if cfg.voters().incoming.contains(id) {
            return Err(Error::ConfigChangeError(format!("{} is in learners and incoming voters", id)));
        }
    }

    for id in &cfg.learners_next {
        if !prs.contains(*id) {
            return Err(Error::ConfigChangeError(format!("no progress for learner(next) {}", id)));
        }
        // Any staged learner was staged because it could not be directly added due to a conflicting voter in the outgoing config.
        if !cfg.voters().outgoing.contains(id) {
            return Err(Error::ConfigChangeError(format!("{} is in learner_next and outgoing voters", id)));
        }
    }

    if !super::joint(cfg) {
        // etcd enforces outgoing and learner_next to be nil amp. But there is no nil in rust. We just check empty for simplicity.
        if cfg.learners_next().is_empty() {
            return Err(Error::ConfigChangeError("learners_next must be euual when not joint".to_owned()));
        }
        if cfg.auto_leave {
            return Err(Error::ConfigChangeError("auto_leave must be false when not joint".to_owned()));
        }
    }
    Ok(())
}
