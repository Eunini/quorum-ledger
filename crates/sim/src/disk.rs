//! Simulated disk with a page-cache model: writes are volatile until synced.
//! On a crash, a random prefix of the unsynced writes survives, the next
//! write may be torn (partially persisted) and the surviving unsynced bytes
//! may suffer a bit flip. Synced data is never damaged (see the fault model in
//! the README for why).

use std::cell::RefCell;
use std::io;
use std::rc::Rc;

use consensus::prng::Prng;
use consensus::BlockDevice;

#[derive(Default, Debug)]
pub struct SimDisk {
    durable: Vec<u8>,
    unsynced: Vec<Vec<u8>>,
}

#[derive(Default, Debug, Clone, Copy)]
pub struct CrashDamage {
    pub lost_writes: usize,
    pub torn: bool,
    pub bit_flipped: bool,
}

impl SimDisk {
    pub fn crash(&mut self, rng: &mut Prng, torn_prob: f64, corrupt_prob: f64) -> CrashDamage {
        let mut damage = CrashDamage::default();
        let writes = std::mem::take(&mut self.unsynced);
        if writes.is_empty() {
            return damage;
        }
        let keep = rng.below(writes.len() as u64 + 1) as usize;
        let start = self.durable.len();
        for w in &writes[..keep] {
            self.durable.extend_from_slice(w);
        }
        damage.lost_writes = writes.len() - keep;
        if keep < writes.len() && rng.chance(torn_prob) {
            let w = &writes[keep];
            let cut = rng.below(w.len() as u64) as usize;
            self.durable.extend_from_slice(&w[..cut]);
            damage.torn = true;
        }
        let persisted = self.durable.len() - start;
        if persisted > 0 && rng.chance(corrupt_prob) {
            let pos = start + rng.below(persisted as u64) as usize;
            self.durable[pos] ^= 1 << rng.below(8);
            damage.bit_flipped = true;
        }
        damage
    }

    pub fn durable_len(&self) -> usize {
        self.durable.len()
    }
}

#[derive(Clone, Default)]
pub struct SimDevice(pub Rc<RefCell<SimDisk>>);

impl BlockDevice for SimDevice {
    fn read_all(&mut self) -> io::Result<Vec<u8>> {
        let d = self.0.borrow();
        assert!(
            d.unsynced.is_empty(),
            "read_all is only used on recovery after a crash"
        );
        Ok(d.durable.clone())
    }
    fn write(&mut self, data: &[u8]) -> io::Result<()> {
        self.0.borrow_mut().unsynced.push(data.to_vec());
        Ok(())
    }
    fn sync(&mut self) -> io::Result<()> {
        let mut d = self.0.borrow_mut();
        let writes = std::mem::take(&mut d.unsynced);
        for w in writes {
            d.durable.extend_from_slice(&w);
        }
        Ok(())
    }
    fn truncate(&mut self, len: u64) -> io::Result<()> {
        let mut d = self.0.borrow_mut();
        assert!(d.unsynced.is_empty());
        d.durable.truncate(len as usize);
        Ok(())
    }
}
