//! Networking for quorum-ledger: a blocking-I/O TCP server that drives the
//! sans-IO [`consensus::Replica`], and a small Rust client used by the
//! benchmarks and integration tests.

pub mod client;
pub mod framing;
pub mod node;
