//! End-to-end test against a real 3-process cluster over localhost TCP:
//! replication, idempotency, two-phase transfers, leader crash (SIGKILL),
//! WAL recovery on restart.

use std::net::{SocketAddr, TcpListener};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use ledger::{account_flags, transfer_flags, NewAccount, NewTransfer, ResultCode};
use server::client::Client;

struct Cluster {
    addrs: Vec<SocketAddr>,
    dir: PathBuf,
    children: Vec<Option<Child>>,
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

impl Cluster {
    fn start(n: usize) -> Self {
        let dir =
            std::env::temp_dir().join(format!("ql-it-{}-{}", std::process::id(), free_port()));
        std::fs::create_dir_all(&dir).unwrap();
        let addrs = (0..n)
            .map(|_| format!("127.0.0.1:{}", free_port()).parse().unwrap())
            .collect();
        let mut c = Cluster {
            addrs,
            dir,
            children: (0..n).map(|_| None).collect(),
        };
        for i in 0..n {
            c.spawn(i);
        }
        c
    }

    fn spawn(&mut self, i: usize) {
        let cluster: Vec<String> = self.addrs.iter().map(|a| a.to_string()).collect();
        let child = Command::new(env!("CARGO_BIN_EXE_quorum-ledger-server"))
            .args([
                "--id",
                &i.to_string(),
                "--cluster",
                &cluster.join(","),
                "--data",
            ])
            .arg(&self.dir)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        self.children[i] = Some(child);
    }

    fn kill(&mut self, i: usize) {
        if let Some(mut c) = self.children[i].take() {
            c.kill().unwrap();
            c.wait().unwrap();
        }
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for i in 0..self.children.len() {
            self.kill(i);
        }
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn transfer(id: u128, dr: u128, cr: u128, amount: u128) -> NewTransfer {
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

#[test]
fn replicated_ledger_survives_leader_crash_and_restart() {
    let mut cluster = Cluster::start(3);
    let mut client = Client::new(cluster.addrs.clone());
    client.attempt_timeout = Duration::from_millis(500);

    // Accounts: 1 = bank (unrestricted), 2 = customer (cannot overdraw), 3 = merchant.
    let accounts = vec![
        NewAccount {
            id: 1,
            ledger: 1,
            code: 1,
            flags: 0,
        },
        NewAccount {
            id: 2,
            ledger: 1,
            code: 2,
            flags: account_flags::DEBITS_MUST_NOT_EXCEED_CREDITS,
        },
        NewAccount {
            id: 3,
            ledger: 1,
            code: 3,
            flags: 0,
        },
    ];
    assert_eq!(
        client.create_accounts(accounts.clone()).unwrap(),
        vec![ResultCode::Ok; 3]
    );
    assert_eq!(
        client.create_accounts(accounts).unwrap(),
        vec![ResultCode::Exists; 3]
    );

    // Fund the customer, then exercise idempotency and the overdraft guard.
    assert_eq!(
        client
            .create_transfers(vec![transfer(10, 1, 2, 1_000)])
            .unwrap(),
        vec![ResultCode::Ok]
    );
    assert_eq!(
        client
            .create_transfers(vec![transfer(10, 1, 2, 1_000)])
            .unwrap(),
        vec![ResultCode::Exists]
    );
    assert_eq!(
        client
            .create_transfers(vec![transfer(10, 1, 2, 999)])
            .unwrap(),
        vec![ResultCode::ExistsWithDifferentFields]
    );
    assert_eq!(
        client
            .create_transfers(vec![transfer(11, 2, 3, 1_001)])
            .unwrap(),
        vec![ResultCode::ExceedsCredits]
    );

    // Two-phase: hold 300, capture 200.
    let mut hold = transfer(12, 2, 3, 300);
    hold.flags = transfer_flags::PENDING;
    hold.timeout = 60;
    let capture = NewTransfer {
        id: 13,
        pending_id: 12,
        amount: 200,
        flags: transfer_flags::POST_PENDING,
        ..Default::default()
    };
    assert_eq!(
        client.create_transfers(vec![hold, capture]).unwrap(),
        vec![ResultCode::Ok, ResultCode::Ok]
    );

    // Crash the leader with SIGKILL and keep going on the remaining two.
    let leader = client.leader_guess();
    cluster.kill(leader);
    let mut next_id = 100u128;
    for _ in 0..20 {
        assert_eq!(
            client
                .create_transfers(vec![transfer(next_id, 1, 3, 5)])
                .unwrap(),
            vec![ResultCode::Ok]
        );
        next_id += 1;
    }
    assert_ne!(client.leader_guess(), leader);

    // Restart the old leader from its WAL, then kill another replica: the
    // cluster can only make progress if the restarted replica recovered and
    // caught up.
    cluster.spawn(leader);
    std::thread::sleep(Duration::from_millis(500));
    let other = (0..3)
        .find(|i| *i != leader && *i != client.leader_guess())
        .unwrap();
    cluster.kill(other);
    for _ in 0..20 {
        assert_eq!(
            client
                .create_transfers(vec![transfer(next_id, 1, 3, 5)])
                .unwrap(),
            vec![ResultCode::Ok]
        );
        next_id += 1;
    }

    let balances = client.lookup_accounts(vec![1, 2, 3]).unwrap();
    assert_eq!(balances.len(), 3);
    let (bank, customer, merchant) = (&balances[0], &balances[1], &balances[2]);
    assert_eq!(bank.debits_posted, 1_000 + 40 * 5);
    assert_eq!(customer.credits_posted, 1_000);
    assert_eq!(customer.debits_posted, 200);
    assert_eq!(
        customer.debits_pending, 0,
        "partial capture releases the remainder"
    );
    assert_eq!(merchant.credits_posted, 200 + 40 * 5);
    let total_debits: u128 = balances.iter().map(|a| a.debits_posted).sum();
    let total_credits: u128 = balances.iter().map(|a| a.credits_posted).sum();
    assert_eq!(total_debits, total_credits);

    let t = client.lookup_transfers(vec![13]).unwrap();
    assert_eq!(t[0].amount, 200);
    assert_eq!(t[0].debit_account_id, 2);

    // A fresh client session sees the same state.
    let mut other_client = Client::new(cluster.addrs.clone());
    assert_eq!(
        other_client.lookup_accounts(vec![1, 2, 3]).unwrap(),
        balances
    );
}
