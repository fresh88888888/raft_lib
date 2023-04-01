

mod changer;
mod restore;

use crate::tracker::Configuration;
pub use self::changer::{Changer,MapChange, MapChangeType};
pub use  self::restore::restore;

pub(crate)fn joint(conf: &Configuration) -> bool{
    !conf.voters().outgoing.is_empty()
}