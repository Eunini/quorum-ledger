//! Ledger data types and their binary encodings.

use crate::codec::{DecodeError, DecodeResult, Put, Reader};

/// Account flags.
pub mod account_flags {
    /// `debits_pending + debits_posted <= credits_posted` must always hold
    /// (an asset-style account that cannot go overdrawn).
    pub const DEBITS_MUST_NOT_EXCEED_CREDITS: u16 = 1 << 0;
    /// `credits_pending + credits_posted <= debits_posted` must always hold.
    pub const CREDITS_MUST_NOT_EXCEED_DEBITS: u16 = 1 << 1;
    pub const ALL: u16 = DEBITS_MUST_NOT_EXCEED_CREDITS | CREDITS_MUST_NOT_EXCEED_DEBITS;
}

/// Transfer flags. At most one may be set.
pub mod transfer_flags {
    /// Phase one of a two-phase transfer: reserve funds as pending.
    pub const PENDING: u16 = 1 << 0;
    /// Phase two: post (capture) a pending transfer, fully or partially.
    pub const POST_PENDING: u16 = 1 << 1;
    /// Phase two: void (release) a pending transfer.
    pub const VOID_PENDING: u16 = 1 << 2;
    pub const ALL: u16 = PENDING | POST_PENDING | VOID_PENDING;
}

/// Request to create an account. Balances always start at zero.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct NewAccount {
    pub id: u128,
    /// Ledger / currency partition. Transfers never cross ledgers.
    pub ledger: u32,
    /// Opaque user-defined account type (chart-of-accounts code).
    pub code: u16,
    pub flags: u16,
}

impl NewAccount {
    pub const ENCODED_LEN: usize = 16 + 4 + 2 + 2;

    pub fn encode(&self, out: &mut Vec<u8>) {
        out.put_u128(self.id);
        out.put_u32(self.ledger);
        out.put_u16(self.code);
        out.put_u16(self.flags);
    }

    pub fn decode(r: &mut Reader<'_>) -> DecodeResult<Self> {
        Ok(NewAccount {
            id: r.u128()?,
            ledger: r.u32()?,
            code: r.u16()?,
            flags: r.u16()?,
        })
    }
}

/// Stored account with balances.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Account {
    pub id: u128,
    pub debits_pending: u128,
    pub debits_posted: u128,
    pub credits_pending: u128,
    pub credits_posted: u128,
    pub ledger: u32,
    pub code: u16,
    pub flags: u16,
    /// Cluster timestamp (ns) of the log entry that created the account.
    pub timestamp: u64,
}

impl Account {
    pub const ENCODED_LEN: usize = 16 * 5 + 4 + 2 + 2 + 8;

    pub fn encode(&self, out: &mut Vec<u8>) {
        out.put_u128(self.id);
        out.put_u128(self.debits_pending);
        out.put_u128(self.debits_posted);
        out.put_u128(self.credits_pending);
        out.put_u128(self.credits_posted);
        out.put_u32(self.ledger);
        out.put_u16(self.code);
        out.put_u16(self.flags);
        out.put_u64(self.timestamp);
    }

    pub fn decode(r: &mut Reader<'_>) -> DecodeResult<Self> {
        Ok(Account {
            id: r.u128()?,
            debits_pending: r.u128()?,
            debits_posted: r.u128()?,
            credits_pending: r.u128()?,
            credits_posted: r.u128()?,
            ledger: r.u32()?,
            code: r.u16()?,
            flags: r.u16()?,
            timestamp: r.u64()?,
        })
    }
}

/// Request to create a transfer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct NewTransfer {
    pub id: u128,
    pub debit_account_id: u128,
    pub credit_account_id: u128,
    pub amount: u128,
    /// For POST_PENDING / VOID_PENDING: id of the pending transfer.
    pub pending_id: u128,
    pub ledger: u32,
    pub code: u16,
    pub flags: u16,
    /// For PENDING: seconds until the reservation expires (0 = never).
    pub timeout: u32,
}

impl NewTransfer {
    pub const ENCODED_LEN: usize = 16 * 5 + 4 + 2 + 2 + 4;

    pub fn encode(&self, out: &mut Vec<u8>) {
        out.put_u128(self.id);
        out.put_u128(self.debit_account_id);
        out.put_u128(self.credit_account_id);
        out.put_u128(self.amount);
        out.put_u128(self.pending_id);
        out.put_u32(self.ledger);
        out.put_u16(self.code);
        out.put_u16(self.flags);
        out.put_u32(self.timeout);
    }

    pub fn decode(r: &mut Reader<'_>) -> DecodeResult<Self> {
        Ok(NewTransfer {
            id: r.u128()?,
            debit_account_id: r.u128()?,
            credit_account_id: r.u128()?,
            amount: r.u128()?,
            pending_id: r.u128()?,
            ledger: r.u32()?,
            code: r.u16()?,
            flags: r.u16()?,
            timeout: r.u32()?,
        })
    }
}

/// Stored transfer. For post/void transfers the account ids, ledger, code and
/// amount are the *resolved* values (inherited from the pending transfer when
/// the request left them zero).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Transfer {
    pub id: u128,
    pub debit_account_id: u128,
    pub credit_account_id: u128,
    pub amount: u128,
    pub pending_id: u128,
    pub ledger: u32,
    pub code: u16,
    pub flags: u16,
    pub timeout: u32,
    pub timestamp: u64,
}

impl Transfer {
    pub const ENCODED_LEN: usize = NewTransfer::ENCODED_LEN + 8;

    pub fn encode(&self, out: &mut Vec<u8>) {
        out.put_u128(self.id);
        out.put_u128(self.debit_account_id);
        out.put_u128(self.credit_account_id);
        out.put_u128(self.amount);
        out.put_u128(self.pending_id);
        out.put_u32(self.ledger);
        out.put_u16(self.code);
        out.put_u16(self.flags);
        out.put_u32(self.timeout);
        out.put_u64(self.timestamp);
    }

    pub fn decode(r: &mut Reader<'_>) -> DecodeResult<Self> {
        Ok(Transfer {
            id: r.u128()?,
            debit_account_id: r.u128()?,
            credit_account_id: r.u128()?,
            amount: r.u128()?,
            pending_id: r.u128()?,
            ledger: r.u32()?,
            code: r.u16()?,
            flags: r.u16()?,
            timeout: r.u32()?,
            timestamp: r.u64()?,
        })
    }
}

macro_rules! result_codes {
    ($( $name:ident = $val:expr ),* $(,)?) => {
        /// Per-event result of `create_accounts` / `create_transfers`.
        /// The numeric values are part of the wire protocol.
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
        #[repr(u32)]
        pub enum ResultCode { $( $name = $val ),* }

        impl ResultCode {
            pub fn from_u32(v: u32) -> Result<Self, DecodeError> {
                match v {
                    $( $val => Ok(ResultCode::$name), )*
                    _ => Err(DecodeError("unknown result code")),
                }
            }
            pub const ALL: &'static [ResultCode] = &[ $( ResultCode::$name ),* ];
        }
    };
}

result_codes! {
    Ok = 0,
    Exists = 1,
    ExistsWithDifferentFields = 2,
    IdMustNotBeZero = 3,
    LedgerMustNotBeZero = 4,
    FlagsInvalid = 5,
    AccountsMustBeDifferent = 6,
    DebitAccountNotFound = 7,
    CreditAccountNotFound = 8,
    AccountsMustHaveSameLedger = 9,
    TransferMustHaveSameLedgerAsAccounts = 10,
    AmountMustNotBeZero = 11,
    ExceedsCredits = 12,
    ExceedsDebits = 13,
    Overflow = 14,
    PendingIdMustBeZero = 15,
    PendingIdMustNotBeZero = 16,
    PendingIdMustBeDifferent = 17,
    PendingTransferNotFound = 18,
    PendingTransferNotPending = 19,
    PendingTransferAlreadyPosted = 20,
    PendingTransferAlreadyVoided = 21,
    PendingTransferExpired = 22,
    PendingTransferFieldMismatch = 23,
    PostAmountExceedsPendingAmount = 24,
    VoidAmountMustMatchPendingAmount = 25,
    TimeoutReservedForPendingTransfer = 26,
}

impl ResultCode {
    /// `Ok` and `Exists` both mean "the object is in the ledger exactly as
    /// requested" and are safe to treat as success by an idempotent client.
    pub fn is_success(self) -> bool {
        matches!(self, ResultCode::Ok | ResultCode::Exists)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encoded_lengths_match_constants() {
        let mut v = Vec::new();
        NewAccount::default().encode(&mut v);
        assert_eq!(v.len(), NewAccount::ENCODED_LEN);
        v.clear();
        Account::default().encode(&mut v);
        assert_eq!(v.len(), Account::ENCODED_LEN);
        v.clear();
        NewTransfer::default().encode(&mut v);
        assert_eq!(v.len(), NewTransfer::ENCODED_LEN);
        v.clear();
        Transfer::default().encode(&mut v);
        assert_eq!(v.len(), Transfer::ENCODED_LEN);
    }

    #[test]
    fn result_codes_roundtrip() {
        for code in ResultCode::ALL {
            assert_eq!(ResultCode::from_u32(*code as u32).unwrap(), *code);
        }
        assert!(ResultCode::from_u32(9999).is_err());
    }
}
