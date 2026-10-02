//! Small deterministic PRNG (xoshiro256** seeded through splitmix64).
//!
//! Implemented here instead of pulling in `rand` so that a seed produces the
//! exact same sequence forever, independent of dependency upgrades. That
//! property is what makes simulator seeds reproducible.

#[derive(Debug, Clone)]
pub struct Prng {
    s: [u64; 4],
}

fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

impl Prng {
    pub fn new(seed: u64) -> Self {
        let mut sm = seed;
        Prng {
            s: [
                splitmix64(&mut sm),
                splitmix64(&mut sm),
                splitmix64(&mut sm),
                splitmix64(&mut sm),
            ],
        }
    }

    pub fn next_u64(&mut self) -> u64 {
        let result = self.s[1].wrapping_mul(5).rotate_left(7).wrapping_mul(9);
        let t = self.s[1] << 17;
        self.s[2] ^= self.s[0];
        self.s[3] ^= self.s[1];
        self.s[1] ^= self.s[2];
        self.s[0] ^= self.s[3];
        self.s[2] ^= t;
        self.s[3] = self.s[3].rotate_left(45);
        result
    }

    /// Uniform in `[0, n)`. `n` must be non-zero.
    pub fn below(&mut self, n: u64) -> u64 {
        assert!(n > 0);
        // Lemire's multiply-shift; the tiny bias is irrelevant for testing.
        ((u128::from(self.next_u64()) * u128::from(n)) >> 64) as u64
    }

    /// Uniform in `[lo, hi]` (inclusive).
    pub fn range(&mut self, lo: u64, hi: u64) -> u64 {
        assert!(lo <= hi);
        lo + self.below(hi - lo + 1)
    }

    /// True with probability `p` (0.0..=1.0).
    pub fn chance(&mut self, p: f64) -> bool {
        if p <= 0.0 {
            return false;
        }
        ((self.next_u64() >> 11) as f64 / (1u64 << 53) as f64) < p
    }

    pub fn fork(&mut self) -> Prng {
        Prng::new(self.next_u64())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic_sequence() {
        let mut a = Prng::new(42);
        let mut b = Prng::new(42);
        for _ in 0..1000 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
        let mut c = Prng::new(43);
        assert_ne!(Prng::new(42).next_u64(), c.next_u64());
    }

    #[test]
    fn range_bounds() {
        let mut r = Prng::new(1);
        for _ in 0..10_000 {
            let v = r.range(3, 7);
            assert!((3..=7).contains(&v));
        }
    }
}
