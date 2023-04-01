
use std::cmp;

use crate::{MajorityConfig, util::Union, HashSet};

use super::{AckedIndexer, VoteResult};


/// A configurationof two group of (possibly overlapping) majority configuration, Descisions(决策) require the support of both majorities.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Configuration{
    pub(crate) incoming: MajorityConfig,
    pub(crate) outgoing: MajorityConfig,
}

impl Configuration {
    /// Creates a new configuration using the given IDs.
    pub fn new(voters: HashSet<u64>) -> Configuration{
        Configuration { incoming: MajorityConfig::new(voters), outgoing: MajorityConfig::default() }
    }

    #[cfg(test)]
    pub(crate) fn new_joint_from_majorities(incoming: MajorityConfig, outgoing: MajorityConfig) -> Self {
        Self { incoming, outgoing }
    }

    /// Create a empty configuration with given capacity.
    pub fn with_capacity(cap: usize) -> Configuration{
        Configuration { incoming: MajorityConfig::with_capacity(cap), outgoing: MajorityConfig::default() }
    }

    /// Returns the largest commited index for the given joint quorum. An index is jointy commited if it is commited in both constituent majorities.
    /// The bool flag indicates whether the index is computed by group commit algorithm successfully. It's true only when both majorites use group commit.
    pub fn commited_index(&self, use_group_commit: bool, l: &impl AckedIndexer) -> (u64, bool) {
        let (i_idx, i_use_gc) = self.incoming.commited_index(use_group_commit, l);
        let (o_idx, o_use_gc) = self.outgoing.commited_index(use_group_commit, l);
        (cmp::min(i_idx, o_idx), i_use_gc && o_use_gc)
    }

    /// Takes a mapping of voters to yes/no (true/false) votes and returns a result indicating whether the vote is pending, lost, or won. A joint quorum requires
    /// both majority quorum to vote in favor.
    pub fn vote_result(&self, check: impl Fn(u64) -> Option<bool>) -> VoteResult{
        let i = self.incoming.vote_result(&check);
        let o = self.outgoing.vote_result(check);
        match (i, o) {
            // It won if won in both
            (VoteResult::Won, VoteResult::Won) => VoteResult::Won,
            // It lost if lost in either
            (VoteResult::Lost, _) | (_, VoteResult::Lost) => VoteResult::Lost,
            // It remains pending if pengding in both or just won in one side.
            _ => VoteResult::Pending,
        }
    }

    /// Clears all IDs.
    pub fn clear(&mut self){
        self.incoming.clear();
        self.outgoing.clear();
    }

    /// Returns true if (and only if ) there is only one voting member (i.e. the leader) in the current configuration
    pub fn is_singleton(&self) -> bool{
        self.outgoing.is_empty() && self.incoming.len() == 1
    }

    /// returns an interator over two hash set without cloning
    pub fn ids(&self)-> Union<'_>{
        Union::new(&self.incoming, &self.outgoing)
    }

    /// Check if an id is a voter.
    #[inline]
    pub fn contains(&self, id: u64) -> bool {
        self.incoming.contains(&id) || self.outgoing.contains(&id)
    }

    /// Describe returns a (multi-line) representation of the commit indexes for the given lookuper.
    #[cfg(test)]
    pub(crate) fn describe(&self, l: &impl AckedIndexer) -> String {
        MajorityConfig::new(self.ids().iter().collect()).describe(l)
    }

}