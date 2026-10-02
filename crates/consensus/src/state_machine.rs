//! The replicated state machine: the ledger plus the client session table.
//!
//! Sessions give at-most-once execution of client requests. Because a retried
//! request can be appended to the log more than once (e.g. by two different
//! leaders), deduplication has to happen here, at apply time, where every
//! replica makes the same decision.

use rustc_hash::FxHashMap;

use ledger::codec::Fnv64;
use ledger::Ledger;

use crate::message::{Entry, Operation, Payload, Reply, ReplyBody, ReplyStatus, Request};

#[derive(Debug, Clone)]
pub struct Session {
    pub last_request_number: u64,
    pub last_reply: ReplyBody,
}

/// Result of applying one request from the log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Applied {
    /// First execution of this request.
    Executed(Reply),
    /// The same request was already executed; this is its cached reply.
    Duplicate(Reply),
    /// An older request number than the session's latest; ignored.
    Stale,
}

#[derive(Debug, Clone, Default)]
pub struct StateMachine {
    pub ledger: Ledger,
    sessions: FxHashMap<u128, Session>,
    last_timestamp: u64,
}

impl StateMachine {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn session(&self, client_id: u128) -> Option<&Session> {
        self.sessions.get(&client_id)
    }

    pub fn session_count(&self) -> usize {
        self.sessions.len()
    }

    pub fn last_timestamp(&self) -> u64 {
        self.last_timestamp
    }

    /// Applies one committed log entry and returns the outcome of each request.
    pub fn apply(&mut self, entry: &Entry) -> Vec<Applied> {
        assert!(
            entry.timestamp > self.last_timestamp,
            "log timestamps must be strictly increasing ({} after {})",
            entry.timestamp,
            self.last_timestamp
        );
        self.last_timestamp = entry.timestamp;
        let ts = entry.timestamp;
        self.ledger.expire_pending(ts);
        match &entry.payload {
            Payload::Noop => Vec::new(),
            Payload::Batch(requests) => {
                requests.iter().map(|r| self.apply_request(ts, r)).collect()
            }
        }
    }

    fn apply_request(&mut self, ts: u64, req: &Request) -> Applied {
        if let Some(s) = self.sessions.get(&req.client_id) {
            if req.request_number < s.last_request_number {
                return Applied::Stale;
            }
            if req.request_number == s.last_request_number {
                return Applied::Duplicate(Reply {
                    client_id: req.client_id,
                    request_number: req.request_number,
                    status: ReplyStatus::Ok(s.last_reply.clone()),
                });
            }
        }
        let body = match &req.operation {
            Operation::CreateAccounts(v) => ReplyBody::Results(self.ledger.create_accounts(ts, v)),
            Operation::CreateTransfers(v) => {
                ReplyBody::Results(self.ledger.create_transfers(ts, v))
            }
            Operation::LookupAccounts(ids) => ReplyBody::Accounts(self.ledger.lookup_accounts(ids)),
            Operation::LookupTransfers(ids) => {
                ReplyBody::Transfers(self.ledger.lookup_transfers(ids))
            }
        };
        self.sessions.insert(
            req.client_id,
            Session {
                last_request_number: req.request_number,
                last_reply: body.clone(),
            },
        );
        Applied::Executed(Reply {
            client_id: req.client_id,
            request_number: req.request_number,
            status: ReplyStatus::Ok(body),
        })
    }

    /// Digest of the ledger and the session table.
    pub fn digest(&self) -> u64 {
        let mut h = Fnv64::default();
        h.write(&self.ledger.digest().to_le_bytes());
        let mut sessions: Vec<_> = self.sessions.iter().collect();
        sessions.sort_by_key(|(id, _)| **id);
        for (id, s) in sessions {
            h.write(&id.to_le_bytes());
            h.write(&s.last_request_number.to_le_bytes());
        }
        h.write(&self.last_timestamp.to_le_bytes());
        h.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ledger::{NewAccount, ResultCode};

    fn req(client: u128, n: u64, id: u128) -> Request {
        Request {
            client_id: client,
            request_number: n,
            operation: Operation::CreateAccounts(vec![NewAccount {
                id,
                ledger: 1,
                code: 1,
                flags: 0,
            }]),
        }
    }

    #[test]
    fn duplicate_requests_are_not_reexecuted() {
        let mut sm = StateMachine::new();
        let e1 = Entry {
            term: 1,
            index: 1,
            timestamp: 10,
            payload: Payload::Batch(vec![req(7, 1, 1)]),
        };
        let out = sm.apply(&e1);
        assert!(
            matches!(&out[0], Applied::Executed(r) if r.status == ReplyStatus::Ok(ReplyBody::Results(vec![ResultCode::Ok])))
        );
        // The same request appended again by a later leader: cached reply, no re-execution
        // (re-execution would have returned `Exists`).
        let e2 = Entry {
            term: 2,
            index: 2,
            timestamp: 20,
            payload: Payload::Batch(vec![req(7, 1, 1), req(7, 0, 9)]),
        };
        let out = sm.apply(&e2);
        assert!(
            matches!(&out[0], Applied::Duplicate(r) if r.status == ReplyStatus::Ok(ReplyBody::Results(vec![ResultCode::Ok])))
        );
        assert_eq!(out[1], Applied::Stale);
        assert_eq!(sm.ledger.account_count(), 1);
    }

    #[test]
    #[should_panic(expected = "strictly increasing")]
    fn rejects_non_monotonic_timestamps() {
        let mut sm = StateMachine::new();
        sm.apply(&Entry {
            term: 1,
            index: 1,
            timestamp: 10,
            payload: Payload::Noop,
        });
        sm.apply(&Entry {
            term: 1,
            index: 2,
            timestamp: 10,
            payload: Payload::Noop,
        });
    }
}
