//! Random ledger workload: account creation, single-phase transfers, holds
//! with and without timeouts, captures, voids, lookups, and deliberate reuse
//! of transfer ids (exact retries and conflicting bodies) to exercise
//! idempotency.

use consensus::prng::Prng;
use consensus::Operation;
use ledger::{account_flags, transfer_flags, NewAccount, NewTransfer};

pub struct Workload {
    rng: Prng,
    account_space: u128,
    next_transfer_id: u128,
    history: Vec<NewTransfer>,
    holds: Vec<(u128, u128)>,
    max_events: u64,
}

impl Workload {
    pub fn new(rng: Prng, account_space: u128, max_events: u64) -> Self {
        Workload {
            rng,
            account_space,
            next_transfer_id: 1,
            history: Vec::new(),
            holds: Vec::new(),
            max_events,
        }
    }

    fn account_id(&mut self) -> u128 {
        1 + u128::from(self.rng.below(self.account_space as u64))
    }

    fn ledger_of(&mut self, id: u128) -> u32 {
        if self.rng.chance(0.03) {
            1 + self.rng.below(2) as u32
        } else {
            (id % 2) as u32 + 1
        }
    }

    fn amount(&mut self) -> u128 {
        if self.rng.chance(0.01) {
            u128::MAX - u128::from(self.rng.below(10))
        } else {
            1 + u128::from(self.rng.below(1000))
        }
    }

    fn new_account(&mut self) -> NewAccount {
        let id = self.account_id();
        let flags = match self.rng.below(10) {
            0..=2 => account_flags::DEBITS_MUST_NOT_EXCEED_CREDITS,
            3 => account_flags::CREDITS_MUST_NOT_EXCEED_DEBITS,
            _ => 0,
        };
        NewAccount {
            id,
            ledger: self.ledger_of(id),
            code: 1,
            flags,
        }
    }

    fn new_transfer(&mut self) -> NewTransfer {
        let roll = self.rng.below(100);
        if roll < 8 && !self.history.is_empty() {
            // Exact resubmission of an earlier transfer.
            let i = self.rng.below(self.history.len() as u64) as usize;
            return self.history[i];
        }
        if roll < 11 && !self.history.is_empty() {
            // Same id, different body.
            let i = self.rng.below(self.history.len() as u64) as usize;
            let mut t = self.history[i];
            t.amount = t.amount.wrapping_add(1);
            return t;
        }
        let id = self.next_transfer_id;
        self.next_transfer_id += 1;
        let t = if roll < 35 && !self.holds.is_empty() {
            let i = self.rng.below(self.holds.len() as u64) as usize;
            let (pending_id, amount) = self.holds[i];
            if self.rng.chance(0.3) {
                self.holds.swap_remove(i);
            }
            let post = self.rng.chance(0.6);
            NewTransfer {
                id,
                pending_id,
                amount: if post && self.rng.chance(0.5) {
                    u128::from(self.rng.below(amount.min(2000) as u64 + 1))
                } else {
                    0
                },
                flags: if post {
                    transfer_flags::POST_PENDING
                } else {
                    transfer_flags::VOID_PENDING
                },
                ..Default::default()
            }
        } else {
            let dr = self.account_id();
            let cr = self.account_id();
            let pending = self.rng.chance(0.3);
            let ledger = self.ledger_of(dr);
            let amount = self.amount();
            let t = NewTransfer {
                id,
                debit_account_id: dr,
                credit_account_id: cr,
                amount,
                pending_id: 0,
                ledger,
                code: 1,
                flags: if pending { transfer_flags::PENDING } else { 0 },
                timeout: if pending && self.rng.chance(0.5) {
                    1 + self.rng.below(3) as u32
                } else {
                    0
                },
            };
            if pending {
                self.holds.push((id, amount));
            }
            t
        };
        self.history.push(t);
        t
    }

    pub fn next_operation(&mut self) -> Operation {
        let n = 1 + self.rng.below(self.max_events) as usize;
        match self.rng.below(100) {
            0..=14 => Operation::CreateAccounts((0..n).map(|_| self.new_account()).collect()),
            15..=89 => Operation::CreateTransfers((0..n).map(|_| self.new_transfer()).collect()),
            90..=95 => Operation::LookupAccounts((0..n).map(|_| self.account_id()).collect()),
            _ => {
                let max = self.next_transfer_id as u64;
                Operation::LookupTransfers(
                    (0..n)
                        .map(|_| u128::from(1 + self.rng.below(max)))
                        .collect(),
                )
            }
        }
    }
}
