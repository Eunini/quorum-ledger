//! Client requests/replies, log entries and replica-to-replica messages, with
//! their binary encodings. The byte layout is documented in `docs/protocol.md`
//! and mirrored by the Java client.

use ledger::codec::{DecodeError, DecodeResult, Put, Reader};
use ledger::{Account, NewAccount, NewTransfer, ResultCode, Transfer};

/// Upper bound on events (accounts, transfers or ids) in one client request.
pub const MAX_EVENTS_PER_REQUEST: usize = 8_190;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Operation {
    CreateAccounts(Vec<NewAccount>),
    CreateTransfers(Vec<NewTransfer>),
    LookupAccounts(Vec<u128>),
    LookupTransfers(Vec<u128>),
}

impl Operation {
    pub fn code(&self) -> u8 {
        match self {
            Operation::CreateAccounts(_) => 1,
            Operation::CreateTransfers(_) => 2,
            Operation::LookupAccounts(_) => 3,
            Operation::LookupTransfers(_) => 4,
        }
    }

    pub fn event_count(&self) -> usize {
        match self {
            Operation::CreateAccounts(v) => v.len(),
            Operation::CreateTransfers(v) => v.len(),
            Operation::LookupAccounts(v) | Operation::LookupTransfers(v) => v.len(),
        }
    }

    pub fn encode(&self, out: &mut Vec<u8>) {
        out.put_u8(self.code());
        out.put_u32(self.event_count() as u32);
        match self {
            Operation::CreateAccounts(v) => v.iter().for_each(|a| a.encode(out)),
            Operation::CreateTransfers(v) => v.iter().for_each(|t| t.encode(out)),
            Operation::LookupAccounts(v) | Operation::LookupTransfers(v) => {
                v.iter().for_each(|id| out.put_u128(*id))
            }
        }
    }

    pub fn decode(r: &mut Reader<'_>) -> DecodeResult<Self> {
        let code = r.u8()?;
        let op = match code {
            1 => {
                let n = r.count(NewAccount::ENCODED_LEN)?;
                Operation::CreateAccounts(
                    (0..n)
                        .map(|_| NewAccount::decode(r))
                        .collect::<Result<_, _>>()?,
                )
            }
            2 => {
                let n = r.count(NewTransfer::ENCODED_LEN)?;
                Operation::CreateTransfers(
                    (0..n)
                        .map(|_| NewTransfer::decode(r))
                        .collect::<Result<_, _>>()?,
                )
            }
            3 | 4 => {
                let n = r.count(16)?;
                let ids = (0..n).map(|_| r.u128()).collect::<Result<_, _>>()?;
                if code == 3 {
                    Operation::LookupAccounts(ids)
                } else {
                    Operation::LookupTransfers(ids)
                }
            }
            _ => return Err(DecodeError("unknown operation")),
        };
        Ok(op)
    }
}

/// A client request. `(client_id, request_number)` identifies it for
/// at-most-once execution; clients have one request in flight and number
/// them 1, 2, 3, ...
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Request {
    pub client_id: u128,
    pub request_number: u64,
    pub operation: Operation,
}

impl Request {
    pub fn encode(&self, out: &mut Vec<u8>) {
        out.put_u128(self.client_id);
        out.put_u64(self.request_number);
        self.operation.encode(out);
    }

    pub fn decode(r: &mut Reader<'_>) -> DecodeResult<Self> {
        Ok(Request {
            client_id: r.u128()?,
            request_number: r.u64()?,
            operation: Operation::decode(r)?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplyBody {
    Results(Vec<ResultCode>),
    Accounts(Vec<Account>),
    Transfers(Vec<Transfer>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplyStatus {
    Ok(ReplyBody),
    /// This replica is not the leader. `leader_hint` is its best guess.
    NotLeader {
        leader_hint: Option<u8>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reply {
    pub client_id: u128,
    pub request_number: u64,
    pub status: ReplyStatus,
}

impl Reply {
    pub fn encode(&self, out: &mut Vec<u8>) {
        out.put_u128(self.client_id);
        out.put_u64(self.request_number);
        match &self.status {
            ReplyStatus::Ok(body) => {
                out.put_u8(0);
                match body {
                    ReplyBody::Results(v) => {
                        out.put_u8(1);
                        out.put_u32(v.len() as u32);
                        v.iter().for_each(|c| out.put_u32(*c as u32));
                    }
                    ReplyBody::Accounts(v) => {
                        out.put_u8(2);
                        out.put_u32(v.len() as u32);
                        v.iter().for_each(|a| a.encode(out));
                    }
                    ReplyBody::Transfers(v) => {
                        out.put_u8(3);
                        out.put_u32(v.len() as u32);
                        v.iter().for_each(|t| t.encode(out));
                    }
                }
            }
            ReplyStatus::NotLeader { leader_hint } => {
                out.put_u8(1);
                out.put_u8(leader_hint.unwrap_or(u8::MAX));
            }
        }
    }

    pub fn decode(r: &mut Reader<'_>) -> DecodeResult<Self> {
        let client_id = r.u128()?;
        let request_number = r.u64()?;
        let status = match r.u8()? {
            0 => {
                let body = match r.u8()? {
                    1 => {
                        let n = r.count(4)?;
                        ReplyBody::Results(
                            (0..n)
                                .map(|_| r.u32().and_then(ResultCode::from_u32))
                                .collect::<Result<_, _>>()?,
                        )
                    }
                    2 => {
                        let n = r.count(Account::ENCODED_LEN)?;
                        ReplyBody::Accounts(
                            (0..n)
                                .map(|_| Account::decode(r))
                                .collect::<Result<_, _>>()?,
                        )
                    }
                    3 => {
                        let n = r.count(Transfer::ENCODED_LEN)?;
                        ReplyBody::Transfers(
                            (0..n)
                                .map(|_| Transfer::decode(r))
                                .collect::<Result<_, _>>()?,
                        )
                    }
                    _ => return Err(DecodeError("unknown reply body")),
                };
                ReplyStatus::Ok(body)
            }
            1 => {
                let h = r.u8()?;
                ReplyStatus::NotLeader {
                    leader_hint: (h != u8::MAX).then_some(h),
                }
            }
            _ => return Err(DecodeError("unknown reply status")),
        };
        Ok(Reply {
            client_id,
            request_number,
            status,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Payload {
    /// Appended by every new leader so it can commit entries of earlier terms.
    Noop,
    /// A batch of client requests committed and applied together.
    Batch(Vec<Request>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    pub term: u64,
    pub index: u64,
    /// Cluster time in ns assigned by the leader; strictly increasing along
    /// the log. The state machine uses it for timestamps and hold expiry.
    pub timestamp: u64,
    pub payload: Payload,
}

impl Entry {
    pub fn encode(&self, out: &mut Vec<u8>) {
        out.put_u64(self.term);
        out.put_u64(self.index);
        out.put_u64(self.timestamp);
        match &self.payload {
            Payload::Noop => out.put_u8(0),
            Payload::Batch(reqs) => {
                out.put_u8(1);
                out.put_u32(reqs.len() as u32);
                reqs.iter().for_each(|r| r.encode(out));
            }
        }
    }

    pub fn decode(r: &mut Reader<'_>) -> DecodeResult<Self> {
        let term = r.u64()?;
        let index = r.u64()?;
        let timestamp = r.u64()?;
        let payload = match r.u8()? {
            0 => Payload::Noop,
            1 => {
                let n = r.count(16 + 8 + 1 + 4)?;
                Payload::Batch(
                    (0..n)
                        .map(|_| Request::decode(r))
                        .collect::<Result<_, _>>()?,
                )
            }
            _ => return Err(DecodeError("unknown payload")),
        };
        Ok(Entry {
            term,
            index,
            timestamp,
            payload,
        })
    }

    pub fn request_count(&self) -> usize {
        match &self.payload {
            Payload::Noop => 0,
            Payload::Batch(r) => r.len(),
        }
    }
}

/// Raft messages between replicas.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
    RequestVote {
        term: u64,
        candidate: u8,
        last_log_index: u64,
        last_log_term: u64,
    },
    RequestVoteResponse {
        term: u64,
        granted: bool,
    },
    AppendEntries {
        term: u64,
        leader: u8,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<Entry>,
        leader_commit: u64,
    },
    AppendEntriesResponse {
        term: u64,
        success: bool,
        /// On success: index of the last entry known to match the leader.
        match_index: u64,
        /// On failure: where the leader should retry from (fast backtracking).
        conflict_index: u64,
        /// On failure: term of the conflicting entry, 0 if the follower's log
        /// was simply too short.
        conflict_term: u64,
    },
}

impl Message {
    pub fn term(&self) -> u64 {
        match self {
            Message::RequestVote { term, .. }
            | Message::RequestVoteResponse { term, .. }
            | Message::AppendEntries { term, .. }
            | Message::AppendEntriesResponse { term, .. } => *term,
        }
    }

    pub fn kind(&self) -> &'static str {
        match self {
            Message::RequestVote { .. } => "RequestVote",
            Message::RequestVoteResponse { .. } => "RequestVoteResponse",
            Message::AppendEntries { .. } => "AppendEntries",
            Message::AppendEntriesResponse { .. } => "AppendEntriesResponse",
        }
    }
}

/// Every frame on a TCP connection: `u32 little-endian length || tag || body`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Frame {
    /// First frame on every connection. `replica_id` is meaningful only for
    /// peers.
    Hello {
        is_peer: bool,
        replica_id: u8,
    },
    Peer(Message),
    ClientRequest(Request),
    ClientReply(Reply),
}

pub mod tag {
    pub const HELLO: u8 = 0x01;
    pub const REQUEST_VOTE: u8 = 0x10;
    pub const REQUEST_VOTE_RESPONSE: u8 = 0x11;
    pub const APPEND_ENTRIES: u8 = 0x12;
    pub const APPEND_ENTRIES_RESPONSE: u8 = 0x13;
    pub const CLIENT_REQUEST: u8 = 0x20;
    pub const CLIENT_REPLY: u8 = 0x21;
}

impl Frame {
    /// Encodes the frame body (without the length prefix).
    pub fn encode(&self, out: &mut Vec<u8>) {
        match self {
            Frame::Hello {
                is_peer,
                replica_id,
            } => {
                out.put_u8(tag::HELLO);
                out.put_u8(u8::from(!*is_peer));
                out.put_u8(*replica_id);
            }
            Frame::Peer(m) => match m {
                Message::RequestVote {
                    term,
                    candidate,
                    last_log_index,
                    last_log_term,
                } => {
                    out.put_u8(tag::REQUEST_VOTE);
                    out.put_u64(*term);
                    out.put_u8(*candidate);
                    out.put_u64(*last_log_index);
                    out.put_u64(*last_log_term);
                }
                Message::RequestVoteResponse { term, granted } => {
                    out.put_u8(tag::REQUEST_VOTE_RESPONSE);
                    out.put_u64(*term);
                    out.put_u8(u8::from(*granted));
                }
                Message::AppendEntries {
                    term,
                    leader,
                    prev_log_index,
                    prev_log_term,
                    entries,
                    leader_commit,
                } => {
                    out.put_u8(tag::APPEND_ENTRIES);
                    out.put_u64(*term);
                    out.put_u8(*leader);
                    out.put_u64(*prev_log_index);
                    out.put_u64(*prev_log_term);
                    out.put_u64(*leader_commit);
                    out.put_u32(entries.len() as u32);
                    entries.iter().for_each(|e| e.encode(out));
                }
                Message::AppendEntriesResponse {
                    term,
                    success,
                    match_index,
                    conflict_index,
                    conflict_term,
                } => {
                    out.put_u8(tag::APPEND_ENTRIES_RESPONSE);
                    out.put_u64(*term);
                    out.put_u8(u8::from(*success));
                    out.put_u64(*match_index);
                    out.put_u64(*conflict_index);
                    out.put_u64(*conflict_term);
                }
            },
            Frame::ClientRequest(r) => {
                out.put_u8(tag::CLIENT_REQUEST);
                r.encode(out);
            }
            Frame::ClientReply(r) => {
                out.put_u8(tag::CLIENT_REPLY);
                r.encode(out);
            }
        }
    }

    pub fn decode(buf: &[u8]) -> DecodeResult<Self> {
        let mut r = Reader::new(buf);
        let bool_byte = |r: &mut Reader<'_>| -> DecodeResult<bool> {
            match r.u8()? {
                0 => Ok(false),
                1 => Ok(true),
                _ => Err(DecodeError("invalid bool")),
            }
        };
        let frame = match r.u8()? {
            tag::HELLO => {
                let kind = r.u8()?;
                if kind > 1 {
                    return Err(DecodeError("invalid hello kind"));
                }
                Frame::Hello {
                    is_peer: kind == 0,
                    replica_id: r.u8()?,
                }
            }
            tag::REQUEST_VOTE => Frame::Peer(Message::RequestVote {
                term: r.u64()?,
                candidate: r.u8()?,
                last_log_index: r.u64()?,
                last_log_term: r.u64()?,
            }),
            tag::REQUEST_VOTE_RESPONSE => Frame::Peer(Message::RequestVoteResponse {
                term: r.u64()?,
                granted: bool_byte(&mut r)?,
            }),
            tag::APPEND_ENTRIES => {
                let term = r.u64()?;
                let leader = r.u8()?;
                let prev_log_index = r.u64()?;
                let prev_log_term = r.u64()?;
                let leader_commit = r.u64()?;
                let n = r.count(25)?;
                let entries = (0..n)
                    .map(|_| Entry::decode(&mut r))
                    .collect::<Result<_, _>>()?;
                Frame::Peer(Message::AppendEntries {
                    term,
                    leader,
                    prev_log_index,
                    prev_log_term,
                    entries,
                    leader_commit,
                })
            }
            tag::APPEND_ENTRIES_RESPONSE => Frame::Peer(Message::AppendEntriesResponse {
                term: r.u64()?,
                success: bool_byte(&mut r)?,
                match_index: r.u64()?,
                conflict_index: r.u64()?,
                conflict_term: r.u64()?,
            }),
            tag::CLIENT_REQUEST => Frame::ClientRequest(Request::decode(&mut r)?),
            tag::CLIENT_REPLY => Frame::ClientReply(Reply::decode(&mut r)?),
            _ => return Err(DecodeError("unknown frame tag")),
        };
        r.finish()?;
        Ok(frame)
    }

    /// Encodes with the 4-byte little-endian length prefix used on TCP.
    pub fn to_wire(&self) -> Vec<u8> {
        let mut out = vec![0u8; 4];
        self.encode(&mut out);
        let len = (out.len() - 4) as u32;
        out[..4].copy_from_slice(&len.to_le_bytes());
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    pub(crate) fn sample_request() -> Request {
        Request {
            client_id: 0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10,
            request_number: 7,
            operation: Operation::CreateTransfers(vec![NewTransfer {
                id: 1,
                debit_account_id: 2,
                credit_account_id: 3,
                amount: 1_000,
                pending_id: 0,
                ledger: 840,
                code: 1,
                flags: ledger::transfer_flags::PENDING,
                timeout: 30,
            }]),
        }
    }

    fn all_frames() -> Vec<Frame> {
        let entry = Entry {
            term: 3,
            index: 9,
            timestamp: 123,
            payload: Payload::Batch(vec![sample_request()]),
        };
        vec![
            Frame::Hello {
                is_peer: true,
                replica_id: 2,
            },
            Frame::Hello {
                is_peer: false,
                replica_id: 0,
            },
            Frame::Peer(Message::RequestVote {
                term: 4,
                candidate: 1,
                last_log_index: 10,
                last_log_term: 3,
            }),
            Frame::Peer(Message::RequestVoteResponse {
                term: 4,
                granted: true,
            }),
            Frame::Peer(Message::AppendEntries {
                term: 4,
                leader: 1,
                prev_log_index: 8,
                prev_log_term: 3,
                entries: vec![
                    entry.clone(),
                    Entry {
                        payload: Payload::Noop,
                        ..entry
                    },
                ],
                leader_commit: 7,
            }),
            Frame::Peer(Message::AppendEntriesResponse {
                term: 4,
                success: false,
                match_index: 0,
                conflict_index: 5,
                conflict_term: 2,
            }),
            Frame::ClientRequest(sample_request()),
            Frame::ClientRequest(Request {
                client_id: 1,
                request_number: 1,
                operation: Operation::LookupAccounts(vec![1, 2, u128::MAX]),
            }),
            Frame::ClientReply(Reply {
                client_id: 1,
                request_number: 2,
                status: ReplyStatus::Ok(ReplyBody::Results(vec![
                    ResultCode::Ok,
                    ResultCode::ExceedsCredits,
                ])),
            }),
            Frame::ClientReply(Reply {
                client_id: 1,
                request_number: 2,
                status: ReplyStatus::Ok(ReplyBody::Accounts(vec![Account {
                    id: 5,
                    credits_posted: 9,
                    ..Default::default()
                }])),
            }),
            Frame::ClientReply(Reply {
                client_id: 1,
                request_number: 2,
                status: ReplyStatus::Ok(ReplyBody::Transfers(vec![Transfer {
                    id: 5,
                    amount: 9,
                    ..Default::default()
                }])),
            }),
            Frame::ClientReply(Reply {
                client_id: 1,
                request_number: 2,
                status: ReplyStatus::NotLeader {
                    leader_hint: Some(2),
                },
            }),
            Frame::ClientReply(Reply {
                client_id: 1,
                request_number: 2,
                status: ReplyStatus::NotLeader { leader_hint: None },
            }),
        ]
    }

    #[test]
    fn frames_roundtrip() {
        for f in all_frames() {
            let wire = f.to_wire();
            let len = u32::from_le_bytes(wire[..4].try_into().unwrap()) as usize;
            assert_eq!(len, wire.len() - 4);
            assert_eq!(Frame::decode(&wire[4..]).unwrap(), f);
        }
    }

    #[test]
    fn truncated_frames_are_rejected() {
        for f in all_frames() {
            let wire = f.to_wire();
            let body = &wire[4..];
            for cut in 0..body.len() {
                assert!(Frame::decode(&body[..cut]).is_err(), "{f:?} cut at {cut}");
            }
            let mut extra = body.to_vec();
            extra.push(0);
            assert!(Frame::decode(&extra).is_err());
        }
    }

    /// Golden bytes shared with the Java client's codec test
    /// (`java-client/src/test/java/.../CodecTest.java`). If this changes, the
    /// wire protocol changed.
    #[test]
    fn golden_client_request_bytes() {
        let hex: String = Frame::ClientRequest(sample_request())
            .to_wire()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        assert_eq!(hex, GOLDEN_REQUEST_HEX);
    }

    pub(crate) const GOLDEN_REQUEST_HEX: &str = concat!(
        "7a000000",                         // length = 122
        "20",                               // tag CLIENT_REQUEST
        "100f0e0d0c0b0a090807060504030201", // client_id
        "0700000000000000",                 // request_number
        "02",                               // op CreateTransfers
        "01000000",                         // count
        "01000000000000000000000000000000", // id
        "02000000000000000000000000000000", // debit_account_id
        "03000000000000000000000000000000", // credit_account_id
        "e8030000000000000000000000000000", // amount
        "00000000000000000000000000000000", // pending_id
        "48030000",                         // ledger 840
        "0100",                             // code
        "0100",                             // flags PENDING
        "1e000000",                         // timeout 30
    );
}
