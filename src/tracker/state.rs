
use std::fmt::{Display, Formatter, Result};


/// The state of the Progress
#[derive(Debug, PartialEq, Clone, Copy, Default)]
pub enum ProgressState {
    /// Whether it's probing
    #[default]
    Probe,
    /// Whether it's replicating
    Replicate,
    /// Whether it's a snapshot
    Snapshot,
}

impl Display for ProgressState {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match self {
            ProgressState::Probe => write!(f, "StateProbe"),
            ProgressState::Replicate => write!(f, "StateReplicate"),
            ProgressState::Snapshot => write!(f, "StateSnapshot"),
        }
    }
}