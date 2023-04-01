
use thiserror::Error;

/// The base error type for raft
#[derive(Debug, Error)]
pub enum Error {
    
    /// An IO error occurred
    #[error("{0}")]
    Io(#[from] std::io::Error),

    /// A storage error occurred.
    #[error("{0}")]
    Store(#[from] StorageError),

    /// raft can not step the local message
    #[error("raft: can not step raft local message")]
    StepLocalMsg,

    /// The raft peer not found and thus cannot step
    #[error("raft: can not step as peer not found")]
    StepPeerNotFound, 

    /// The proposal of the changes was deopped.(修改的建议被否决了)
    #[error("raft: proposal dropped")]
    ProposalDropped,

    /// The configuration is invalid
    #[error("{0}")]
    ConfigInvalid(String),

    /// A Protobuf message codec failed in same manner.
    #[error("protobuf codec error {0:?}")]
    CodecError(#[from] protobuf::ProtobufError),

    /// The node exists, but should not
    #[error("The node {id} already exists in the {set} set.")]
    Exist {
        /// The node id
        id: u64,
        /// The node set
        set: &'static str,
    },

    /// The node not exists, but should.
    #[error("The node {id} is not in the {set} set.")]
    NotExists{
        /// The node id 
        id: u64,
        /// The node set.
        set: &'static str,
    },

    /// ConfChange proposal is invalid.(ConfChange 提议无效)
    #[error("{0}")]
    ConfigChangeError(String),

    /// The request snapshot is dropped.(请求快照被删除)
    #[error("raft: request snapshot dropped.")]
    RequestSnapshotDropped,
}

impl PartialEq for Error {
    #[cfg_attr(feature = "cargo-clippy", allow(clippy::match_same_arms))]
    fn eq(&self, other: &Error) -> bool {
        match (self, other) {
            (Error::StepPeerNotFound, Error::StepPeerNotFound) => true,
            (Error::ProposalDropped, Error::ProposalDropped) => true,
            (Error::Store(ref e1), Error::Store(ref e2)) => e1 == e2,
            (Error::Io(ref e1), Error::Io(ref e2)) => e1.kind() == e2.kind(),
            (Error::StepLocalMsg, Error::StepLocalMsg) => true,
            (Error::ConfigInvalid(ref e1), Error::ConfigInvalid(ref e2)) => e1 == e2,
            (Error::RequestSnapshotDropped, Error::RequestSnapshotDropped) => true,
            (Error::ConfigChangeError(e1), Error::ConfigChangeError(e2)) => e1 == e2,
            _ => false,
        }
    }
}

/// An error with the storage
#[derive(Debug, Error)]
pub enum StorageError {
    /// The storage was compacted and not accessible
    #[error("log compacted")]
    Compacted,

    /// The log is not unavailable(日志不可用)
    #[error("log unavailable")]
    Unavailable,

    /// The log is being fetched.(正在获取日志)
    #[error("log is temporarily unavailable")]
    LogTemporarilyUnavailable,

    /// The snapshot out of date(快照过期)
    #[error("snapshot out of date")]
    SnapshotOutOfDate,

    /// The snapshot is being created
    #[error("snapshot is temporarily unavailable")]
    SnapshotTemporarilyUnavailable,

    /// Some other error occurred.
    #[error("unknown error {0}")]
    Other(#[from] Box<dyn std::error::Error + Sync + Send>),

 }

 impl PartialEq for StorageError {
    #[cfg_attr(feature="cargo-clippy", allow(clippy::match_same_arms))]
    fn eq(&self, other: &StorageError) -> bool {
        matches!(
            (self, other),
            (StorageError::Compacted, StorageError::Compacted) | (StorageError::Unavailable, StorageError::Unavailable) |
            (StorageError::LogTemporarilyUnavailable, StorageError::LogTemporarilyUnavailable) |
            (StorageError::SnapshotOutOfDate, StorageError::SnapshotOutOfDate) |
            (StorageError::SnapshotTemporarilyUnavailable, StorageError::SnapshotTemporarilyUnavailable)
        )
    }
}

/// A result type that wraps up the raft errors
pub type Result<T> = std::result::Result<T, Error>;