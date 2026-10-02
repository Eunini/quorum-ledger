//! Blocking Rust client.
//!
//! One `Client` is one session: it has a random 128-bit client id and keeps
//! exactly one request in flight, numbered 1, 2, 3, ... A request that times
//! out or hits a dead/non-leader replica is resent *with the same request
//! number* to another replica, so the cluster's session table guarantees it
//! executes at most once.

use std::hash::{BuildHasher, Hasher};
use std::io::{self, BufReader, BufWriter, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::{Duration, Instant};

use consensus::{Frame, Operation, ReplyBody, ReplyStatus, Request};
use ledger::{Account, NewAccount, NewTransfer, ResultCode, Transfer};

use crate::framing::read_frame;

pub struct Client {
    addrs: Vec<SocketAddr>,
    client_id: u128,
    request_number: u64,
    leader_guess: usize,
    conn: Option<(usize, BufReader<TcpStream>, BufWriter<TcpStream>)>,
    /// Per-attempt timeout before trying another replica.
    pub attempt_timeout: Duration,
    /// Give up on a request after this long.
    pub request_timeout: Duration,
}

pub fn random_u128() -> u128 {
    let a = std::collections::hash_map::RandomState::new()
        .build_hasher()
        .finish();
    let mut h = std::collections::hash_map::RandomState::new().build_hasher();
    h.write_u64(a);
    h.write_u128(Instant::now().elapsed().as_nanos());
    (u128::from(a) << 64) | u128::from(h.finish())
}

impl Client {
    pub fn new(addrs: Vec<SocketAddr>) -> Self {
        Client {
            addrs,
            client_id: random_u128(),
            request_number: 0,
            leader_guess: 0,
            conn: None,
            attempt_timeout: Duration::from_millis(1000),
            request_timeout: Duration::from_secs(30),
        }
    }

    pub fn client_id(&self) -> u128 {
        self.client_id
    }

    /// Index of the replica this client currently believes is the leader.
    pub fn leader_guess(&self) -> usize {
        self.leader_guess
    }

    fn connect(&mut self) -> io::Result<()> {
        if let Some((idx, _, _)) = &self.conn {
            if *idx == self.leader_guess {
                return Ok(());
            }
        }
        self.conn = None;
        let addr = self.addrs[self.leader_guess];
        let stream = TcpStream::connect_timeout(&addr, Duration::from_millis(500))?;
        stream.set_nodelay(true)?;
        let mut w = BufWriter::new(stream.try_clone()?);
        w.write_all(
            &Frame::Hello {
                is_peer: false,
                replica_id: 0,
            }
            .to_wire(),
        )?;
        w.flush()?;
        self.conn = Some((self.leader_guess, BufReader::new(stream), w));
        Ok(())
    }

    fn next_replica(&mut self, hint: Option<u8>) {
        self.conn = None;
        self.leader_guess = match hint {
            Some(h) if (h as usize) < self.addrs.len() && h as usize != self.leader_guess => {
                h as usize
            }
            _ => (self.leader_guess + 1) % self.addrs.len(),
        };
    }

    /// Sends one request and waits for its reply, retrying across replicas.
    pub fn request(&mut self, operation: Operation) -> io::Result<ReplyBody> {
        self.request_number += 1;
        let req = Request {
            client_id: self.client_id,
            request_number: self.request_number,
            operation,
        };
        let wire = Frame::ClientRequest(req).to_wire();
        let deadline = Instant::now() + self.request_timeout;
        let mut not_leader_streak = 0u32;
        while Instant::now() < deadline {
            match self.attempt(&wire) {
                Ok(Some(body)) => return Ok(body),
                Ok(None) => {
                    // NotLeader: hint already applied. Back off a little if
                    // the cluster has no leader yet.
                    not_leader_streak += 1;
                    if not_leader_streak >= self.addrs.len() as u32 {
                        std::thread::sleep(Duration::from_millis(50));
                    }
                }
                Err(_) => {
                    self.next_replica(None);
                    std::thread::sleep(Duration::from_millis(20));
                }
            }
        }
        Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "request timed out on all replicas",
        ))
    }

    fn attempt(&mut self, wire: &[u8]) -> io::Result<Option<ReplyBody>> {
        self.connect()?;
        let (_, reader, writer) = self.conn.as_mut().expect("connected");
        writer.write_all(wire)?;
        writer.flush()?;
        reader
            .get_ref()
            .set_read_timeout(Some(self.attempt_timeout))?;
        loop {
            let frame = read_frame(reader)?;
            let Frame::ClientReply(reply) = frame else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "unexpected frame",
                ));
            };
            if reply.client_id != self.client_id || reply.request_number != self.request_number {
                continue; // stale reply to an earlier attempt
            }
            return match reply.status {
                ReplyStatus::Ok(body) => Ok(Some(body)),
                ReplyStatus::NotLeader { leader_hint } => {
                    self.next_replica(leader_hint);
                    Ok(None)
                }
            };
        }
    }

    pub fn create_accounts(&mut self, accounts: Vec<NewAccount>) -> io::Result<Vec<ResultCode>> {
        match self.request(Operation::CreateAccounts(accounts))? {
            ReplyBody::Results(r) => Ok(r),
            other => Err(unexpected(other)),
        }
    }

    pub fn create_transfers(&mut self, transfers: Vec<NewTransfer>) -> io::Result<Vec<ResultCode>> {
        match self.request(Operation::CreateTransfers(transfers))? {
            ReplyBody::Results(r) => Ok(r),
            other => Err(unexpected(other)),
        }
    }

    pub fn lookup_accounts(&mut self, ids: Vec<u128>) -> io::Result<Vec<Account>> {
        match self.request(Operation::LookupAccounts(ids))? {
            ReplyBody::Accounts(a) => Ok(a),
            other => Err(unexpected(other)),
        }
    }

    pub fn lookup_transfers(&mut self, ids: Vec<u128>) -> io::Result<Vec<Transfer>> {
        match self.request(Operation::LookupTransfers(ids))? {
            ReplyBody::Transfers(t) => Ok(t),
            other => Err(unexpected(other)),
        }
    }
}

fn unexpected(body: ReplyBody) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("unexpected reply body {body:?}"),
    )
}
