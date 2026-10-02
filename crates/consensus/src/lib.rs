//! Raft-based replication for the quorum-ledger state machine.
//!
//! * [`replica`]       – sans-IO Raft replica (election, log replication,
//!   commit, log repair, client sessions, batching)
//! * [`state_machine`] – ledger + client session table (at-most-once apply)
//! * [`storage`]       – checksummed write-ahead log behind a device trait
//! * [`message`]       – wire types and binary codec
//! * [`prng`]          – deterministic PRNG used by replicas and the simulator

pub mod message;
pub mod prng;
pub mod replica;
pub mod state_machine;
pub mod storage;

pub use message::{
    Entry, Frame, Message, Operation, Payload, Reply, ReplyBody, ReplyStatus, Request,
};
pub use replica::{Config, InjectedBugs, Outgoing, Replica, Role};
pub use state_machine::StateMachine;
pub use storage::{BlockDevice, FileDevice, HardState, MemDevice, Storage, Wal};
