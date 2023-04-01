


pub use super::util::NO_LIMIT;
pub use super::read_only::ReadOnlyOption;

/// Config contains the parameters to start to raft.

use super::{
    INVALID_ID,
    errors::{Error, Result}
};

/// Config contains the parameters to start a raft.
#[derive(Clone)]
pub struct Config{
    /// The identity of the local raft, it can not be 0 and must be unique in the group.
    pub id: u64,

    /// The number of node.tick invocations that must pass between elections. that is. if follower does not receive any messages from the leader of current term.
    /// before ElectionTick has elapsed(结束), It will become candidate(候选人) and start an election(选举). election_tick must be greater than HeartbeatTick. we suggest 
    /// election_tick= 10 * HeartbeatTick to avoid(避免) unnecessary(不必要) leader switching.
    pub election_tick: usize,

    /// HeartbeatTick is the number of node.tick invocations that must be passed between heartbeat. That is, The leader sends heartbeat messages to maintain(保持) its leadership hearbeat ticks.
    pub heartbeat_tick: usize,

    /// Applied is the last applied index. It should be only be set when restarting raft. raft will not returnentries to the application smaller or equals to applied.
    /// if applied is unset when restarting. raft might return previous applied entries. This is very applictaion dependent(依赖) configuration.
    pub applied: u64,

    /// Limited the max size of each append message. Smaller value lowers the raft recovery cost(回收成本)(initial probingand meassage lost during noramal operation). On the other side, It might affect
    /// the throughput(吞吐量) during normal replicaton(复制). Note math.Uusize64 for unlimited. 0 for at most one entry per message.
    pub max_size_per_msg: u64,

    /// Limited the max number of in-flight append messages during optimistic repliaction phase. The application transportation(传输层) layer usually has its own sending buffer over TCP/UDP. 
    /// Set to avoid(避免) overflowing that sending buffer(发送的缓冲区溢出).
    pub max_inflight_msgs: usize,

    /// Specify if the leader should check quorum activity(仲裁活动). Leader steps down(下线) when quorum(仲裁人数) is not active for an electionTimeout.
    pub check_quorum: bool,

    /// Enables the Pre-vote algorithm described in raft thesis section 9.6 This prevents disruption(中断) when a node that has been partitioned(已经分区的) away rejoins the cluster.
    pub pre_vote: bool,

    /// The range of election(选举) timeout, In some cases, we hope some nodes has less possibility(可能性较小) to become leader. This configuration ensurs(确保) that the randomized election_timeout.
    /// Will always be suit in [min_election_tick, max_election_tick) if it is 0, then election_tick will be chosen.
    pub min_election_tick: usize,

    /// If it is 0, then 2 * election_tick will be chosen.
    pub max_election_tick: usize,

    /// Choosen the linearizability(线性化) mode or the lease mode(租约模式) to read data. If you don't care about the read consistency(读一致性) and want a higher read performance(读性能), 
    /// you can use lease mode. Setting this to `LeaseBased` requires `check_quorum=true`.
    pub read_only_option: ReadOnlyOption,

    /// Don't broadcast an empty raft entry to notify follower to commit an entry. This may make follower wait a long time to apply(申请) an entry. This configrution 
    /// May affect proposal forwarding(提案转发) and follower read. 
    pub skip_bcast_commit: bool,

    /// Battches every append msg if any msg already exists.
    pub batch_append: bool,

    /// The election priority(优先级) of this node.
    pub priority: u64,

    /// Sepecify maxinum of uncommited entry size. When this limit is reached, All propsals(建议) to append new log will be droped.
    pub max_uncommitted_size: u64,

    /// Max size for committed entries in a `Ready`
    pub max_committed_size_per_ready: u64,
}

impl Default for Config{
    fn default() -> Self{
        const HEARTBEAT_TICK: usize = 2;
        Self{
            id: 0,
            election_tick: HEARTBEAT_TICK * 10,
            heartbeat_tick: HEARTBEAT_TICK,
            applied: 0,
            max_size_per_msg: 0,
            max_inflight_msgs: 256,
            check_quorum: false,
            pre_vote: false,
            min_election_tick: 0,
            max_election_tick: 0,
            read_only_option: ReadOnlyOption::Safe,
            skip_bcast_commit: false,
            batch_append: false,
            priority: 0,
            max_uncommitted_size: NO_LIMIT,
            max_committed_size_per_ready: NO_LIMIT,
        }
    }
}

impl Config{
    /// Creates a new config
    pub fn new(id: u64) -> Self{
        Self{
            id,
            ..Self::default()
        }
    }

    /// The minimum number of ticks before an election.
    #[inline]
    pub fn min_election_tick(&self) -> usize {
        if self.min_election_tick == 0 {
            self.election_tick
        } else {
            self.min_election_tick
        }
    }

    /// The maximum number of ticks before an election.
    #[inline]
    pub fn max_election_tick(&self) -> usize {
        if self.max_election_tick == 0 {
            2 * self.election_tick
        } else {
            self.max_election_tick
        }
    }

    /// Runs validations against the config
    pub fn validate(&self) -> Result<()>{
        if self.id == INVALID_ID {
            return Err(Error::ConfigInvalid("invalid node id".to_owned()));
        }
        
        if self.heartbeat_tick == 0 {
            return Err(Error::ConfigInvalid("heratbeat tick must greater than 0".to_owned()));
        }

        if self.election_tick <= self.heartbeat_tick {
            return Err(Error::ConfigInvalid("election tick must be greater than heartbeat_tick".to_owned()));
        }

        let min_timeout: usize = self.min_election_tick;
        let max_timeout: usize = self.max_election_tick;
        if min_timeout < self.election_tick {
            return Err(Error::ConfigInvalid(format!("min election tick {} must be less than election tick {}", min_timeout, self.election_tick)));
        }

        if min_timeout >= max_timeout {
            return Err(Error::ConfigInvalid(format!("min election tick {} should be less than max election tick {}",min_timeout, max_timeout)));
        }

        if self.max_inflight_msgs == 0 {
            return Err(Error::ConfigInvalid("max inflight messages must be greater than 0".to_owned()));
        }

        if self.read_only_option == ReadOnlyOption::LeaseBased && !self.check_quorum {
            return Err(Error::ConfigInvalid("read_only_option == LeaseBased requires check_quorum == true".into()));
        }

        if self.max_uncommitted_size < self.max_size_per_msg {
            return Err(Error::ConfigInvalid("max uncommited size should greater than max_size_per_msg".to_owned()));
        }

        Ok(())
    }

}