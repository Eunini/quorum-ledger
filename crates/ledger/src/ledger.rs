//! The double-entry ledger state machine.
//!
//! It is fully deterministic: given the same sequence of
//! `(timestamp, operation)` inputs every replica reaches the same state. Time
//! is never read from the host clock; it comes from the timestamp the leader
//! assigned to the log entry being applied.

use std::collections::BTreeSet;

use rustc_hash::FxHashMap;

use crate::codec::Fnv64;
use crate::sharded::ShardedMap;
use crate::types::{
    account_flags, transfer_flags, Account, NewAccount, NewTransfer, ResultCode, Transfer,
};

const NANOS_PER_SECOND: u64 = 1_000_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PendingStatus {
    Pending,
    Posted,
    Voided,
    Expired,
}

impl PendingStatus {
    fn as_u8(self) -> u8 {
        match self {
            PendingStatus::Pending => 0,
            PendingStatus::Posted => 1,
            PendingStatus::Voided => 2,
            PendingStatus::Expired => 3,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct PendingInfo {
    status: PendingStatus,
    /// Absolute expiry in cluster nanoseconds; 0 means "never expires".
    expires_at: u64,
}

/// Bits of `StoredTransfer::zero_fields`: which optional post/void fields the
/// request left as zero (and were therefore inherited from the hold).
mod zero {
    pub const DEBIT: u8 = 1;
    pub const CREDIT: u8 = 2;
    pub const LEDGER: u8 = 4;
    pub const CODE: u8 = 8;
    pub const AMOUNT: u8 = 16;
}

#[derive(Debug, Clone)]
struct StoredTransfer {
    transfer: Transfer,
    /// Together with `transfer` this reconstructs the exact request, which
    /// idempotency compares against, without storing a second copy of it.
    zero_fields: u8,
}

impl StoredTransfer {
    fn new(transfer: Transfer, request: &NewTransfer) -> Self {
        let mut zero_fields = 0;
        if request.debit_account_id == 0 {
            zero_fields |= zero::DEBIT;
        }
        if request.credit_account_id == 0 {
            zero_fields |= zero::CREDIT;
        }
        if request.ledger == 0 {
            zero_fields |= zero::LEDGER;
        }
        if request.code == 0 {
            zero_fields |= zero::CODE;
        }
        if request.amount == 0 {
            zero_fields |= zero::AMOUNT;
        }
        let s = StoredTransfer {
            transfer,
            zero_fields,
        };
        debug_assert_eq!(s.request(), *request);
        s
    }

    /// The request exactly as it was submitted.
    fn request(&self) -> NewTransfer {
        let t = &self.transfer;
        let pick = |bit: u8, v: u128| if self.zero_fields & bit != 0 { 0 } else { v };
        NewTransfer {
            id: t.id,
            debit_account_id: pick(zero::DEBIT, t.debit_account_id),
            credit_account_id: pick(zero::CREDIT, t.credit_account_id),
            amount: pick(zero::AMOUNT, t.amount),
            pending_id: t.pending_id,
            ledger: pick(zero::LEDGER, u128::from(t.ledger)) as u32,
            code: pick(zero::CODE, u128::from(t.code)) as u16,
            flags: t.flags,
            timeout: t.timeout,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Single,
    Pending,
    Post,
    Void,
}

#[derive(Debug, Clone, Default)]
pub struct Ledger {
    accounts: ShardedMap<Account>,
    transfers: ShardedMap<StoredTransfer>,
    pending: ShardedMap<PendingInfo>,
    /// Pending transfers with a timeout, ordered by expiry time.
    expiry: BTreeSet<(u64, u128)>,
}

impl Ledger {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn account_count(&self) -> usize {
        self.accounts.len()
    }

    pub fn transfer_count(&self) -> usize {
        self.transfers.len()
    }

    pub fn account(&self, id: u128) -> Option<&Account> {
        self.accounts.get(&id)
    }

    pub fn transfer(&self, id: u128) -> Option<&Transfer> {
        self.transfers.get(&id).map(|s| &s.transfer)
    }

    pub fn pending_status(&self, id: u128) -> Option<PendingStatus> {
        self.pending.get(&id).map(|p| p.status)
    }

    pub fn create_accounts(&mut self, timestamp: u64, batch: &[NewAccount]) -> Vec<ResultCode> {
        batch
            .iter()
            .map(|a| self.create_account(timestamp, a))
            .collect()
    }

    pub fn create_transfers(&mut self, timestamp: u64, batch: &[NewTransfer]) -> Vec<ResultCode> {
        batch
            .iter()
            .map(|t| self.create_transfer(timestamp, t))
            .collect()
    }

    pub fn lookup_accounts(&self, ids: &[u128]) -> Vec<Account> {
        ids.iter()
            .filter_map(|id| self.accounts.get(id).copied())
            .collect()
    }

    pub fn lookup_transfers(&self, ids: &[u128]) -> Vec<Transfer> {
        ids.iter()
            .filter_map(|id| self.transfers.get(id).map(|s| s.transfer))
            .collect()
    }

    pub fn create_account(&mut self, timestamp: u64, a: &NewAccount) -> ResultCode {
        if a.id == 0 {
            return ResultCode::IdMustNotBeZero;
        }
        if a.flags & !account_flags::ALL != 0 || a.flags == account_flags::ALL {
            return ResultCode::FlagsInvalid;
        }
        if a.ledger == 0 {
            return ResultCode::LedgerMustNotBeZero;
        }
        if let Some(existing) = self.accounts.get(&a.id) {
            return if existing.ledger == a.ledger
                && existing.code == a.code
                && existing.flags == a.flags
            {
                ResultCode::Exists
            } else {
                ResultCode::ExistsWithDifferentFields
            };
        }
        self.accounts.insert(
            a.id,
            Account {
                id: a.id,
                ledger: a.ledger,
                code: a.code,
                flags: a.flags,
                timestamp,
                ..Account::default()
            },
        );
        ResultCode::Ok
    }

    pub fn create_transfer(&mut self, timestamp: u64, t: &NewTransfer) -> ResultCode {
        if t.id == 0 {
            return ResultCode::IdMustNotBeZero;
        }
        if t.flags & !transfer_flags::ALL != 0 {
            return ResultCode::FlagsInvalid;
        }
        let kind = match t.flags {
            0 => Kind::Single,
            transfer_flags::PENDING => Kind::Pending,
            transfer_flags::POST_PENDING => Kind::Post,
            transfer_flags::VOID_PENDING => Kind::Void,
            _ => return ResultCode::FlagsInvalid,
        };
        // Idempotency: the same id with the same body is a no-op success; the
        // same id with a different body is an error. Checked before any other
        // validation so a retry always gets a stable answer.
        if let Some(existing) = self.transfers.get(&t.id) {
            return if existing.request() == *t {
                ResultCode::Exists
            } else {
                ResultCode::ExistsWithDifferentFields
            };
        }
        match kind {
            Kind::Single | Kind::Pending => self.create_single_or_pending(timestamp, t, kind),
            Kind::Post | Kind::Void => self.resolve_pending(timestamp, t, kind),
        }
    }

    fn create_single_or_pending(
        &mut self,
        timestamp: u64,
        t: &NewTransfer,
        kind: Kind,
    ) -> ResultCode {
        if t.pending_id != 0 {
            return ResultCode::PendingIdMustBeZero;
        }
        if kind == Kind::Single && t.timeout != 0 {
            return ResultCode::TimeoutReservedForPendingTransfer;
        }
        if t.debit_account_id == t.credit_account_id {
            return ResultCode::AccountsMustBeDifferent;
        }
        if t.ledger == 0 {
            return ResultCode::LedgerMustNotBeZero;
        }
        if t.amount == 0 {
            return ResultCode::AmountMustNotBeZero;
        }
        let Some(dr) = self.accounts.get(&t.debit_account_id).copied() else {
            return ResultCode::DebitAccountNotFound;
        };
        let Some(cr) = self.accounts.get(&t.credit_account_id).copied() else {
            return ResultCode::CreditAccountNotFound;
        };
        if dr.ledger != cr.ledger {
            return ResultCode::AccountsMustHaveSameLedger;
        }
        if t.ledger != dr.ledger {
            return ResultCode::TransferMustHaveSameLedgerAsAccounts;
        }

        // Compute every new balance with overflow checks before mutating
        // anything, so a failed event leaves no partial effects.
        let amount = t.amount;
        let (dr_pending, dr_posted, cr_pending, cr_posted) = match kind {
            Kind::Pending => (
                dr.debits_pending.checked_add(amount),
                Some(dr.debits_posted),
                cr.credits_pending.checked_add(amount),
                Some(cr.credits_posted),
            ),
            _ => (
                Some(dr.debits_pending),
                dr.debits_posted.checked_add(amount),
                Some(cr.credits_pending),
                cr.credits_posted.checked_add(amount),
            ),
        };
        let (Some(dr_pending), Some(dr_posted), Some(cr_pending), Some(cr_posted)) =
            (dr_pending, dr_posted, cr_pending, cr_posted)
        else {
            return ResultCode::Overflow;
        };
        let Some(dr_total) = dr_pending.checked_add(dr_posted) else {
            return ResultCode::Overflow;
        };
        let Some(cr_total) = cr_pending.checked_add(cr_posted) else {
            return ResultCode::Overflow;
        };
        if dr.flags & account_flags::DEBITS_MUST_NOT_EXCEED_CREDITS != 0
            && dr_total > dr.credits_posted
        {
            return ResultCode::ExceedsCredits;
        }
        if cr.flags & account_flags::CREDITS_MUST_NOT_EXCEED_DEBITS != 0
            && cr_total > cr.debits_posted
        {
            return ResultCode::ExceedsDebits;
        }
        let expires_at = if kind == Kind::Pending && t.timeout > 0 {
            timestamp.saturating_add(u64::from(t.timeout) * NANOS_PER_SECOND)
        } else {
            0
        };

        // Commit.
        {
            let d = self
                .accounts
                .get_mut(&t.debit_account_id)
                .expect("checked above");
            d.debits_pending = dr_pending;
            d.debits_posted = dr_posted;
        }
        {
            let c = self
                .accounts
                .get_mut(&t.credit_account_id)
                .expect("checked above");
            c.credits_pending = cr_pending;
            c.credits_posted = cr_posted;
        }
        if kind == Kind::Pending {
            self.pending.insert(
                t.id,
                PendingInfo {
                    status: PendingStatus::Pending,
                    expires_at,
                },
            );
            if expires_at != 0 {
                self.expiry.insert((expires_at, t.id));
            }
        }
        self.transfers.insert(
            t.id,
            StoredTransfer::new(
                Transfer {
                    id: t.id,
                    debit_account_id: t.debit_account_id,
                    credit_account_id: t.credit_account_id,
                    amount,
                    pending_id: 0,
                    ledger: t.ledger,
                    code: t.code,
                    flags: t.flags,
                    timeout: t.timeout,
                    timestamp,
                },
                t,
            ),
        );
        ResultCode::Ok
    }

    fn resolve_pending(&mut self, timestamp: u64, t: &NewTransfer, kind: Kind) -> ResultCode {
        if t.pending_id == 0 {
            return ResultCode::PendingIdMustNotBeZero;
        }
        if t.pending_id == t.id {
            return ResultCode::PendingIdMustBeDifferent;
        }
        if t.timeout != 0 {
            return ResultCode::TimeoutReservedForPendingTransfer;
        }
        let Some(p) = self.transfers.get(&t.pending_id).map(|s| s.transfer) else {
            return ResultCode::PendingTransferNotFound;
        };
        if p.flags & transfer_flags::PENDING == 0 {
            return ResultCode::PendingTransferNotPending;
        }
        // Optional fields must match the pending transfer when provided.
        if (t.debit_account_id != 0 && t.debit_account_id != p.debit_account_id)
            || (t.credit_account_id != 0 && t.credit_account_id != p.credit_account_id)
            || (t.ledger != 0 && t.ledger != p.ledger)
            || (t.code != 0 && t.code != p.code)
        {
            return ResultCode::PendingTransferFieldMismatch;
        }
        let info = *self
            .pending
            .get(&t.pending_id)
            .expect("every pending transfer has a status entry");
        match info.status {
            PendingStatus::Pending => {}
            PendingStatus::Posted => return ResultCode::PendingTransferAlreadyPosted,
            PendingStatus::Voided => return ResultCode::PendingTransferAlreadyVoided,
            PendingStatus::Expired => return ResultCode::PendingTransferExpired,
        }
        // `expire_pending` runs before every log entry, so this can only be hit
        // if a caller forgot to call it; treat the transfer as expired anyway.
        if info.expires_at != 0 && info.expires_at <= timestamp {
            return ResultCode::PendingTransferExpired;
        }
        let amount = match kind {
            Kind::Post => {
                let a = if t.amount == 0 { p.amount } else { t.amount };
                if a > p.amount {
                    return ResultCode::PostAmountExceedsPendingAmount;
                }
                a
            }
            _ => {
                if t.amount != 0 && t.amount != p.amount {
                    return ResultCode::VoidAmountMustMatchPendingAmount;
                }
                p.amount
            }
        };
        let dr = self.accounts[&p.debit_account_id];
        let cr = self.accounts[&p.credit_account_id];
        let (dr_posted, cr_posted) = if kind == Kind::Post {
            match (
                dr.debits_posted.checked_add(amount),
                cr.credits_posted.checked_add(amount),
            ) {
                (Some(d), Some(c)) => (d, c),
                _ => return ResultCode::Overflow,
            }
        } else {
            (dr.debits_posted, cr.credits_posted)
        };
        // Releasing the reservation and posting at most the reserved amount can
        // only reduce `pending + posted`, so the balance flags cannot be broken.
        {
            let d = self.accounts.get_mut(&p.debit_account_id).expect("exists");
            d.debits_pending -= p.amount;
            d.debits_posted = dr_posted;
        }
        {
            let c = self.accounts.get_mut(&p.credit_account_id).expect("exists");
            c.credits_pending -= p.amount;
            c.credits_posted = cr_posted;
        }
        let status = if kind == Kind::Post {
            PendingStatus::Posted
        } else {
            PendingStatus::Voided
        };
        self.pending.get_mut(&t.pending_id).expect("exists").status = status;
        if info.expires_at != 0 {
            self.expiry.remove(&(info.expires_at, t.pending_id));
        }
        self.transfers.insert(
            t.id,
            StoredTransfer::new(
                Transfer {
                    id: t.id,
                    debit_account_id: p.debit_account_id,
                    credit_account_id: p.credit_account_id,
                    amount,
                    pending_id: t.pending_id,
                    ledger: p.ledger,
                    code: p.code,
                    flags: t.flags,
                    timeout: 0,
                    timestamp,
                },
                t,
            ),
        );
        ResultCode::Ok
    }

    /// Expires every pending transfer whose deadline is `<= now`, releasing
    /// its reserved amounts. Must be called with the timestamp of each log
    /// entry before that entry's operations are applied.
    pub fn expire_pending(&mut self, now: u64) -> usize {
        let mut expired = 0;
        while let Some(&(expires_at, id)) = self.expiry.first() {
            if expires_at > now {
                break;
            }
            self.expiry.pop_first();
            let p = self.transfers[&id].transfer;
            let info = self.pending.get_mut(&id).expect("pending status exists");
            debug_assert_eq!(info.status, PendingStatus::Pending);
            info.status = PendingStatus::Expired;
            self.accounts
                .get_mut(&p.debit_account_id)
                .expect("exists")
                .debits_pending -= p.amount;
            self.accounts
                .get_mut(&p.credit_account_id)
                .expect("exists")
                .credits_pending -= p.amount;
            expired += 1;
        }
        expired
    }

    /// Checks every ledger invariant from first principles. Intended for tests
    /// and the simulator; cost is linear in the number of accounts+transfers.
    ///
    /// 1. Every account balance equals the sum of the stored transfers that
    ///    touch it (so no transfer was applied twice or lost).
    /// 2. Per ledger: total debits == total credits (posted and pending).
    /// 3. Account balance flags hold.
    /// 4. The expiry index matches the set of live pending transfers.
    pub fn check_invariants(&self) -> Result<(), String> {
        #[derive(Default, Clone, Copy)]
        struct Bal {
            dp: u128,
            dpo: u128,
            cp: u128,
            cpo: u128,
        }
        let mut expect: FxHashMap<u128, Bal> = FxHashMap::default();
        let mut live_expiring = 0usize;
        for s in self.transfers.values() {
            let t = &s.transfer;
            let (pending, posted) = if t.flags & transfer_flags::PENDING != 0 {
                let info = self
                    .pending
                    .get(&t.id)
                    .ok_or_else(|| format!("pending transfer {} has no status", t.id))?;
                if info.status == PendingStatus::Pending {
                    if info.expires_at != 0 {
                        live_expiring += 1;
                        if !self.expiry.contains(&(info.expires_at, t.id)) {
                            return Err(format!(
                                "pending transfer {} missing from expiry index",
                                t.id
                            ));
                        }
                    }
                    (t.amount, 0)
                } else {
                    (0, 0)
                }
            } else if t.flags & transfer_flags::VOID_PENDING != 0 {
                (0, 0)
            } else {
                (0, t.amount)
            };
            let d = expect.entry(t.debit_account_id).or_default();
            d.dp = d.dp.wrapping_add(pending);
            d.dpo = d.dpo.wrapping_add(posted);
            let c = expect.entry(t.credit_account_id).or_default();
            c.cp = c.cp.wrapping_add(pending);
            c.cpo = c.cpo.wrapping_add(posted);
        }
        if live_expiring != self.expiry.len() {
            return Err(format!(
                "expiry index has {} entries but {} live expiring pending transfers",
                self.expiry.len(),
                live_expiring
            ));
        }
        let mut per_ledger: FxHashMap<u32, Bal> = FxHashMap::default();
        for a in self.accounts.values() {
            let e = expect.remove(&a.id).unwrap_or_default();
            if (e.dp, e.dpo, e.cp, e.cpo)
                != (
                    a.debits_pending,
                    a.debits_posted,
                    a.credits_pending,
                    a.credits_posted,
                )
            {
                return Err(format!(
                    "account {} balances {:?} do not match transfers {:?}",
                    a.id,
                    (
                        a.debits_pending,
                        a.debits_posted,
                        a.credits_pending,
                        a.credits_posted
                    ),
                    (e.dp, e.dpo, e.cp, e.cpo)
                ));
            }
            if a.flags & account_flags::DEBITS_MUST_NOT_EXCEED_CREDITS != 0
                && a.debits_pending
                    .checked_add(a.debits_posted)
                    .is_none_or(|v| v > a.credits_posted)
            {
                return Err(format!(
                    "account {} violates debits_must_not_exceed_credits",
                    a.id
                ));
            }
            if a.flags & account_flags::CREDITS_MUST_NOT_EXCEED_DEBITS != 0
                && a.credits_pending
                    .checked_add(a.credits_posted)
                    .is_none_or(|v| v > a.debits_posted)
            {
                return Err(format!(
                    "account {} violates credits_must_not_exceed_debits",
                    a.id
                ));
            }
            let l = per_ledger.entry(a.ledger).or_default();
            // Totals are compared modulo 2^128: each transfer adds the same
            // amount to both sides, so equality must hold even on wrap-around.
            l.dp = l.dp.wrapping_add(a.debits_pending);
            l.dpo = l.dpo.wrapping_add(a.debits_posted);
            l.cp = l.cp.wrapping_add(a.credits_pending);
            l.cpo = l.cpo.wrapping_add(a.credits_posted);
        }
        if let Some((id, _)) = expect.into_iter().next() {
            return Err(format!("transfers reference missing account {id}"));
        }
        for (ledger, b) in per_ledger {
            if b.dpo != b.cpo || b.dp != b.cp {
                return Err(format!(
                    "ledger {ledger} unbalanced: debits posted {} credits posted {} debits pending {} credits pending {}",
                    b.dpo, b.cpo, b.dp, b.cp
                ));
            }
        }
        Ok(())
    }

    /// Order-independent digest of the full ledger state, used to compare
    /// replicas.
    pub fn digest(&self) -> u64 {
        let mut accounts: Vec<&Account> = self.accounts.values().collect();
        accounts.sort_by_key(|a| a.id);
        let mut transfers: Vec<&StoredTransfer> = self.transfers.values().collect();
        transfers.sort_by_key(|t| t.transfer.id);
        let mut h = Fnv64::default();
        let mut buf = Vec::with_capacity(128);
        for a in accounts {
            buf.clear();
            a.encode(&mut buf);
            h.write(&buf);
        }
        for t in transfers {
            buf.clear();
            t.transfer.encode(&mut buf);
            t.request().encode(&mut buf);
            if let Some(p) = self.pending.get(&t.transfer.id) {
                buf.push(p.status.as_u8());
                buf.extend_from_slice(&p.expires_at.to_le_bytes());
            }
            h.write(&buf);
        }
        h.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::account_flags::*;
    use crate::types::transfer_flags::*;

    fn acct(id: u128, flags: u16) -> NewAccount {
        NewAccount {
            id,
            ledger: 1,
            code: 10,
            flags,
        }
    }

    fn xfer(id: u128, dr: u128, cr: u128, amount: u128) -> NewTransfer {
        NewTransfer {
            id,
            debit_account_id: dr,
            credit_account_id: cr,
            amount,
            ledger: 1,
            code: 1,
            ..Default::default()
        }
    }

    fn setup() -> Ledger {
        let mut l = Ledger::new();
        let r = l.create_accounts(
            1,
            &[
                acct(1, 0),
                acct(2, DEBITS_MUST_NOT_EXCEED_CREDITS),
                acct(3, CREDITS_MUST_NOT_EXCEED_DEBITS),
            ],
        );
        assert!(r.iter().all(|c| *c == ResultCode::Ok));
        l
    }

    #[test]
    fn account_idempotency() {
        let mut l = setup();
        assert_eq!(l.create_account(2, &acct(1, 0)), ResultCode::Exists);
        assert_eq!(
            l.create_account(2, &acct(1, DEBITS_MUST_NOT_EXCEED_CREDITS)),
            ResultCode::ExistsWithDifferentFields
        );
        assert_eq!(
            l.create_account(2, &acct(0, 0)),
            ResultCode::IdMustNotBeZero
        );
        assert_eq!(
            l.create_account(
                2,
                &acct(
                    9,
                    DEBITS_MUST_NOT_EXCEED_CREDITS | CREDITS_MUST_NOT_EXCEED_DEBITS
                )
            ),
            ResultCode::FlagsInvalid
        );
        assert_eq!(
            l.create_account(
                2,
                &NewAccount {
                    id: 9,
                    ledger: 0,
                    code: 0,
                    flags: 0
                }
            ),
            ResultCode::LedgerMustNotBeZero
        );
    }

    #[test]
    fn single_phase_transfer_and_idempotency() {
        let mut l = setup();
        assert_eq!(l.create_transfer(5, &xfer(100, 1, 2, 50)), ResultCode::Ok);
        assert_eq!(
            l.create_transfer(6, &xfer(100, 1, 2, 50)),
            ResultCode::Exists
        );
        assert_eq!(
            l.create_transfer(6, &xfer(100, 1, 2, 51)),
            ResultCode::ExistsWithDifferentFields
        );
        assert_eq!(l.account(1).unwrap().debits_posted, 50);
        assert_eq!(l.account(2).unwrap().credits_posted, 50);
        assert_eq!(l.transfer(100).unwrap().timestamp, 5);
        l.check_invariants().unwrap();
    }

    #[test]
    fn validation_errors() {
        let mut l = setup();
        assert_eq!(
            l.create_transfer(5, &xfer(0, 1, 2, 1)),
            ResultCode::IdMustNotBeZero
        );
        assert_eq!(
            l.create_transfer(5, &xfer(7, 1, 1, 1)),
            ResultCode::AccountsMustBeDifferent
        );
        assert_eq!(
            l.create_transfer(5, &xfer(7, 1, 2, 0)),
            ResultCode::AmountMustNotBeZero
        );
        assert_eq!(
            l.create_transfer(5, &xfer(7, 99, 2, 1)),
            ResultCode::DebitAccountNotFound
        );
        assert_eq!(
            l.create_transfer(5, &xfer(7, 1, 99, 1)),
            ResultCode::CreditAccountNotFound
        );
        let mut t = xfer(7, 1, 2, 1);
        t.flags = PENDING | POST_PENDING;
        assert_eq!(l.create_transfer(5, &t), ResultCode::FlagsInvalid);
        let mut t = xfer(7, 1, 2, 1);
        t.timeout = 3;
        assert_eq!(
            l.create_transfer(5, &t),
            ResultCode::TimeoutReservedForPendingTransfer
        );
        let mut t = xfer(7, 1, 2, 1);
        t.ledger = 2;
        assert_eq!(
            l.create_transfer(5, &t),
            ResultCode::TransferMustHaveSameLedgerAsAccounts
        );
        l.create_account(
            1,
            &NewAccount {
                id: 50,
                ledger: 2,
                code: 1,
                flags: 0,
            },
        );
        assert_eq!(
            l.create_transfer(5, &xfer(7, 1, 50, 1)),
            ResultCode::AccountsMustHaveSameLedger
        );
        // Failed transfers leave no trace.
        assert_eq!(l.transfer_count(), 0);
        l.check_invariants().unwrap();
    }

    #[test]
    fn balance_flags_enforced() {
        let mut l = setup();
        // Account 2 cannot be debited beyond its credits.
        assert_eq!(
            l.create_transfer(5, &xfer(10, 2, 1, 1)),
            ResultCode::ExceedsCredits
        );
        assert_eq!(l.create_transfer(5, &xfer(11, 1, 2, 100)), ResultCode::Ok);
        assert_eq!(l.create_transfer(5, &xfer(12, 2, 1, 60)), ResultCode::Ok);
        assert_eq!(
            l.create_transfer(5, &xfer(13, 2, 1, 41)),
            ResultCode::ExceedsCredits
        );
        assert_eq!(l.create_transfer(5, &xfer(13, 2, 1, 40)), ResultCode::Ok);
        // Account 3 cannot be credited beyond its debits.
        assert_eq!(
            l.create_transfer(5, &xfer(14, 1, 3, 1)),
            ResultCode::ExceedsDebits
        );
        l.check_invariants().unwrap();
    }

    #[test]
    fn two_phase_post_partial_and_void() {
        let mut l = setup();
        l.create_transfer(1, &xfer(1, 1, 2, 100));
        let mut hold = xfer(2, 2, 1, 70);
        hold.flags = PENDING;
        assert_eq!(l.create_transfer(2, &hold), ResultCode::Ok);
        assert_eq!(l.account(2).unwrap().debits_pending, 70);
        // A second hold beyond the remaining balance fails.
        let mut hold2 = xfer(3, 2, 1, 31);
        hold2.flags = PENDING;
        assert_eq!(l.create_transfer(2, &hold2), ResultCode::ExceedsCredits);
        // Partial capture of 50: 20 released.
        let post = NewTransfer {
            id: 4,
            pending_id: 2,
            amount: 50,
            flags: POST_PENDING,
            ..Default::default()
        };
        assert_eq!(l.create_transfer(3, &post), ResultCode::Ok);
        let a = l.account(2).unwrap();
        assert_eq!((a.debits_pending, a.debits_posted), (0, 50));
        assert_eq!(l.transfer(4).unwrap().debit_account_id, 2);
        // Cannot void or post again.
        let void = NewTransfer {
            id: 5,
            pending_id: 2,
            flags: VOID_PENDING,
            ..Default::default()
        };
        assert_eq!(
            l.create_transfer(4, &void),
            ResultCode::PendingTransferAlreadyPosted
        );
        assert_eq!(l.create_transfer(4, &post), ResultCode::Exists);
        // Void a fresh hold.
        let mut hold3 = xfer(6, 2, 1, 10);
        hold3.flags = PENDING;
        assert_eq!(l.create_transfer(5, &hold3), ResultCode::Ok);
        let void3 = NewTransfer {
            id: 7,
            pending_id: 6,
            flags: VOID_PENDING,
            ..Default::default()
        };
        assert_eq!(l.create_transfer(6, &void3), ResultCode::Ok);
        assert_eq!(l.pending_status(6), Some(PendingStatus::Voided));
        let post3 = NewTransfer {
            id: 8,
            pending_id: 6,
            flags: POST_PENDING,
            ..Default::default()
        };
        assert_eq!(
            l.create_transfer(7, &post3),
            ResultCode::PendingTransferAlreadyVoided
        );
        // Post amount above the hold, mismatched fields, non-pending target.
        let mut hold4 = xfer(9, 1, 2, 5);
        hold4.flags = PENDING;
        assert_eq!(l.create_transfer(8, &hold4), ResultCode::Ok);
        let over = NewTransfer {
            id: 10,
            pending_id: 9,
            amount: 6,
            flags: POST_PENDING,
            ..Default::default()
        };
        assert_eq!(
            l.create_transfer(9, &over),
            ResultCode::PostAmountExceedsPendingAmount
        );
        let mism = NewTransfer {
            id: 10,
            pending_id: 9,
            debit_account_id: 3,
            flags: POST_PENDING,
            ..Default::default()
        };
        assert_eq!(
            l.create_transfer(9, &mism),
            ResultCode::PendingTransferFieldMismatch
        );
        let notp = NewTransfer {
            id: 10,
            pending_id: 1,
            flags: POST_PENDING,
            ..Default::default()
        };
        assert_eq!(
            l.create_transfer(9, &notp),
            ResultCode::PendingTransferNotPending
        );
        let badvoid = NewTransfer {
            id: 10,
            pending_id: 9,
            amount: 4,
            flags: VOID_PENDING,
            ..Default::default()
        };
        assert_eq!(
            l.create_transfer(9, &badvoid),
            ResultCode::VoidAmountMustMatchPendingAmount
        );
        l.check_invariants().unwrap();
    }

    #[test]
    fn pending_transfers_expire() {
        let mut l = setup();
        let mut hold = xfer(1, 1, 2, 10);
        hold.flags = PENDING;
        hold.timeout = 2;
        let t0 = 1_000;
        assert_eq!(l.create_transfer(t0, &hold), ResultCode::Ok);
        assert_eq!(l.expire_pending(t0 + 2 * NANOS_PER_SECOND - 1), 0);
        assert_eq!(l.account(1).unwrap().debits_pending, 10);
        assert_eq!(l.expire_pending(t0 + 2 * NANOS_PER_SECOND), 1);
        assert_eq!(l.account(1).unwrap().debits_pending, 0);
        let post = NewTransfer {
            id: 2,
            pending_id: 1,
            flags: POST_PENDING,
            ..Default::default()
        };
        assert_eq!(
            l.create_transfer(t0 + 3 * NANOS_PER_SECOND, &post),
            ResultCode::PendingTransferExpired
        );
        l.check_invariants().unwrap();
    }

    #[test]
    fn overflow_is_rejected_without_side_effects() {
        let mut l = setup();
        assert_eq!(
            l.create_transfer(1, &xfer(1, 1, 3, u128::MAX)),
            ResultCode::ExceedsDebits
        );
        l.create_account(1, &acct(4, 0));
        assert_eq!(
            l.create_transfer(1, &xfer(1, 1, 4, u128::MAX)),
            ResultCode::Ok
        );
        assert_eq!(
            l.create_transfer(1, &xfer(2, 1, 4, 1)),
            ResultCode::Overflow
        );
        let a = l.account(1).unwrap();
        assert_eq!(a.debits_posted, u128::MAX);
        l.check_invariants().unwrap();
    }
}
