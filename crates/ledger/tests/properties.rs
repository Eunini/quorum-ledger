//! Property tests: arbitrary sequences of ledger operations never break the
//! ledger invariants, replay is deterministic, and idempotent retries are
//! side-effect free.

use ledger::{account_flags, transfer_flags, Ledger, NewAccount, NewTransfer, ResultCode};
use proptest::prelude::*;

#[derive(Debug, Clone)]
enum Op {
    Account(NewAccount),
    Transfer(NewTransfer),
    /// Advance the cluster clock by this many milliseconds and expire holds.
    Advance(u32),
}

fn amount() -> impl Strategy<Value = u128> {
    prop_oneof![
        8 => 0u128..200,
        1 => Just(u128::MAX),
        1 => (u128::MAX - 1000)..=u128::MAX,
    ]
}

fn op() -> impl Strategy<Value = Op> {
    let account = (0u128..10, 1u32..3, 0u16..3, 0u16..4).prop_map(|(id, ledger, code, flags)| {
        Op::Account(NewAccount {
            id,
            ledger,
            code,
            flags,
        })
    });
    let flags = prop_oneof![
        3 => Just(0u16),
        3 => Just(transfer_flags::PENDING),
        2 => Just(transfer_flags::POST_PENDING),
        2 => Just(transfer_flags::VOID_PENDING),
        1 => 0u16..8,
    ];
    let transfer = (
        0u128..40,
        prop_oneof![Just(0u128), 0u128..10],
        prop_oneof![Just(0u128), 0u128..10],
        amount(),
        prop_oneof![3 => Just(0u128), 2 => 0u128..40],
        0u32..3,
        flags,
        prop_oneof![3 => Just(0u32), 1 => 0u32..4],
    )
        .prop_map(|(id, dr, cr, amount, pending_id, ledger, flags, timeout)| {
            Op::Transfer(NewTransfer {
                id,
                debit_account_id: dr,
                credit_account_id: cr,
                amount,
                pending_id,
                ledger,
                code: 1,
                flags,
                timeout,
            })
        });
    prop_oneof![
        2 => account,
        6 => transfer,
        1 => (0u32..3000).prop_map(Op::Advance),
    ]
}

fn run(ops: &[Op]) -> (Ledger, Vec<ResultCode>) {
    let mut l = Ledger::new();
    let mut now: u64 = 1_000_000_000;
    let mut results = Vec::new();
    for op in ops {
        match op {
            Op::Account(a) => results.push(l.create_account(now, a)),
            Op::Transfer(t) => results.push(l.create_transfer(now, t)),
            Op::Advance(ms) => {
                now += u64::from(*ms) * 1_000_000;
                l.expire_pending(now);
            }
        }
        now += 1;
    }
    (l, results)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    #[test]
    fn invariants_hold_after_every_op(ops in prop::collection::vec(op(), 1..120)) {
        let mut l = Ledger::new();
        let mut now: u64 = 1_000_000_000;
        for op in &ops {
            match op {
                Op::Account(a) => { l.create_account(now, a); }
                Op::Transfer(t) => { l.create_transfer(now, t); }
                Op::Advance(ms) => {
                    now += u64::from(*ms) * 1_000_000;
                    l.expire_pending(now);
                }
            }
            now += 1;
            if let Err(e) = l.check_invariants() {
                prop_assert!(false, "invariant violated after {:?}: {}", op, e);
            }
        }
    }

    #[test]
    fn replay_is_deterministic(ops in prop::collection::vec(op(), 1..120)) {
        let (a, ra) = run(&ops);
        let (b, rb) = run(&ops);
        prop_assert_eq!(ra, rb);
        prop_assert_eq!(a.digest(), b.digest());
    }

    #[test]
    fn retrying_successful_creates_is_a_noop(ops in prop::collection::vec(op(), 1..120)) {
        let (mut l, results) = run(&ops);
        let before = l.digest();
        let now = u64::MAX / 2;
        for (op, r) in ops.iter().filter(|o| !matches!(o, Op::Advance(_))).zip(results) {
            if r != ResultCode::Ok {
                continue;
            }
            let again = match op {
                Op::Account(a) => l.create_account(now, a),
                Op::Transfer(t) => l.create_transfer(now, t),
                Op::Advance(_) => unreachable!(),
            };
            prop_assert_eq!(again, ResultCode::Exists);
        }
        prop_assert_eq!(l.digest(), before);
    }

    #[test]
    fn guarded_accounts_never_go_negative(
        amounts in prop::collection::vec((1u128..500, any::<bool>(), any::<bool>()), 1..200)
    ) {
        // A funded customer account that must not be overdrawn, debited by a
        // stream of holds, captures, voids and direct payments.
        let mut l = Ledger::new();
        l.create_account(1, &NewAccount { id: 1, ledger: 1, code: 1, flags: 0 });
        l.create_account(1, &NewAccount { id: 2, ledger: 1, code: 1, flags: account_flags::DEBITS_MUST_NOT_EXCEED_CREDITS });
        l.create_account(1, &NewAccount { id: 3, ledger: 1, code: 1, flags: 0 });
        l.create_transfer(1, &NewTransfer { id: 1, debit_account_id: 1, credit_account_id: 2, amount: 10_000, ledger: 1, code: 1, ..Default::default() });
        let mut next_id = 2u128;
        for (amount, hold, capture) in amounts {
            let id = next_id;
            next_id += 2;
            let flags = if hold { transfer_flags::PENDING } else { 0 };
            let r = l.create_transfer(2, &NewTransfer { id, debit_account_id: 2, credit_account_id: 3, amount, ledger: 1, code: 1, flags, ..Default::default() });
            if hold && r == ResultCode::Ok {
                let flags = if capture { transfer_flags::POST_PENDING } else { transfer_flags::VOID_PENDING };
                let r2 = l.create_transfer(3, &NewTransfer { id: id + 1, pending_id: id, flags, ..Default::default() });
                prop_assert_eq!(r2, ResultCode::Ok);
            }
            let a = l.account(2).unwrap();
            prop_assert!(a.debits_pending + a.debits_posted <= a.credits_posted);
        }
        l.check_invariants().unwrap();
    }
}
