//! A hash map split into independently growing shards.
//!
//! A single `HashMap` doubles its table in one step; at a few million large
//! entries that is a stop-the-world copy of hundreds of megabytes, which
//! showed up as multi-second stalls of the replica event loop (see
//! docs/bugs-found.md). With 64 shards each resize moves 1/64 of the data, so
//! the worst pause shrinks accordingly while lookups stay O(1).

use rustc_hash::FxHashMap;

const SHARD_BITS: u32 = 6;
const SHARDS: usize = 1 << SHARD_BITS;

#[derive(Debug, Clone)]
pub struct ShardedMap<V> {
    shards: Vec<FxHashMap<u128, V>>,
    len: usize,
}

impl<V> Default for ShardedMap<V> {
    fn default() -> Self {
        ShardedMap {
            shards: (0..SHARDS).map(|_| FxHashMap::default()).collect(),
            len: 0,
        }
    }
}

/// Shard selection uses a hash independent of the one the shard itself uses
/// (splitmix64 finalizer), so entries within a shard stay well distributed.
#[inline]
fn shard_of(key: u128) -> usize {
    let mut z = (key as u64) ^ ((key >> 64) as u64).rotate_left(32);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^= z >> 31;
    (z >> (64 - SHARD_BITS)) as usize
}

impl<V> ShardedMap<V> {
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline]
    pub fn get(&self, key: &u128) -> Option<&V> {
        self.shards[shard_of(*key)].get(key)
    }

    #[inline]
    pub fn get_mut(&mut self, key: &u128) -> Option<&mut V> {
        self.shards[shard_of(*key)].get_mut(key)
    }

    #[inline]
    pub fn contains_key(&self, key: &u128) -> bool {
        self.shards[shard_of(*key)].contains_key(key)
    }

    pub fn insert(&mut self, key: u128, value: V) -> Option<V> {
        let old = self.shards[shard_of(key)].insert(key, value);
        if old.is_none() {
            self.len += 1;
        }
        old
    }

    pub fn values(&self) -> impl Iterator<Item = &V> {
        self.shards.iter().flat_map(|s| s.values())
    }

    pub fn iter(&self) -> impl Iterator<Item = (&u128, &V)> {
        self.shards.iter().flat_map(|s| s.iter())
    }
}

impl<V> std::ops::Index<&u128> for ShardedMap<V> {
    type Output = V;

    fn index(&self, key: &u128) -> &V {
        self.get(key).expect("key not present in ShardedMap")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn behaves_like_a_map() {
        let mut m = ShardedMap::default();
        for i in 0..10_000u128 {
            assert!(m.insert(i * 7919, i).is_none());
        }
        assert_eq!(m.len(), 10_000);
        assert_eq!(m.insert(7919, 99), Some(1));
        assert_eq!(m.len(), 10_000);
        assert_eq!(m.get(&7919), Some(&99));
        *m.get_mut(&0).unwrap() = 5;
        assert_eq!(m.get(&0), Some(&5));
        assert!(!m.contains_key(&1));
        assert_eq!(m.values().count(), 10_000);
        // Keys spread over all shards.
        assert!(m.shards.iter().all(|s| !s.is_empty()));
    }
}
