//! Minimal little-endian binary codec shared by the ledger, the replication
//! protocol and the write-ahead log. Everything on the wire and on disk is
//! encoded with these primitives so the Java client can mirror them exactly.

use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodeError(pub &'static str);

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "decode error: {}", self.0)
    }
}

impl std::error::Error for DecodeError {}

pub type DecodeResult<T> = Result<T, DecodeError>;

/// Append-only encoder helpers on `Vec<u8>`.
pub trait Put {
    fn put_u8(&mut self, v: u8);
    fn put_u16(&mut self, v: u16);
    fn put_u32(&mut self, v: u32);
    fn put_u64(&mut self, v: u64);
    fn put_u128(&mut self, v: u128);
}

impl Put for Vec<u8> {
    #[inline]
    fn put_u8(&mut self, v: u8) {
        self.push(v);
    }
    #[inline]
    fn put_u16(&mut self, v: u16) {
        self.extend_from_slice(&v.to_le_bytes());
    }
    #[inline]
    fn put_u32(&mut self, v: u32) {
        self.extend_from_slice(&v.to_le_bytes());
    }
    #[inline]
    fn put_u64(&mut self, v: u64) {
        self.extend_from_slice(&v.to_le_bytes());
    }
    #[inline]
    fn put_u128(&mut self, v: u128) {
        self.extend_from_slice(&v.to_le_bytes());
    }
}

/// Bounds-checked cursor over a byte slice.
pub struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Reader { buf, pos: 0 }
    }

    pub fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    pub fn position(&self) -> usize {
        self.pos
    }

    pub fn bytes(&mut self, n: usize) -> DecodeResult<&'a [u8]> {
        if self.remaining() < n {
            return Err(DecodeError("unexpected end of input"));
        }
        let s = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }

    fn array<const N: usize>(&mut self) -> DecodeResult<[u8; N]> {
        let s = self.bytes(N)?;
        let mut a = [0u8; N];
        a.copy_from_slice(s);
        Ok(a)
    }

    pub fn u8(&mut self) -> DecodeResult<u8> {
        Ok(self.array::<1>()?[0])
    }
    pub fn u16(&mut self) -> DecodeResult<u16> {
        Ok(u16::from_le_bytes(self.array()?))
    }
    pub fn u32(&mut self) -> DecodeResult<u32> {
        Ok(u32::from_le_bytes(self.array()?))
    }
    pub fn u64(&mut self) -> DecodeResult<u64> {
        Ok(u64::from_le_bytes(self.array()?))
    }
    pub fn u128(&mut self) -> DecodeResult<u128> {
        Ok(u128::from_le_bytes(self.array()?))
    }

    /// Reads a u32 element count and validates that `count * min_elem_size`
    /// bytes could possibly follow, so a corrupt length cannot trigger a huge
    /// allocation.
    pub fn count(&mut self, min_elem_size: usize) -> DecodeResult<usize> {
        let n = self.u32()? as usize;
        if n.saturating_mul(min_elem_size.max(1)) > self.remaining() {
            return Err(DecodeError("element count exceeds input"));
        }
        Ok(n)
    }

    /// Fails if any bytes are left over: every message must be consumed exactly.
    pub fn finish(&self) -> DecodeResult<()> {
        if self.remaining() != 0 {
            return Err(DecodeError("trailing bytes"));
        }
        Ok(())
    }
}

/// 64-bit FNV-1a, used for deterministic state digests (not for integrity).
#[derive(Clone, Copy)]
pub struct Fnv64(u64);

impl Default for Fnv64 {
    fn default() -> Self {
        Fnv64(0xcbf2_9ce4_8422_2325)
    }
}

impl Fnv64 {
    pub fn write(&mut self, bytes: &[u8]) {
        for b in bytes {
            self.0 ^= *b as u64;
            self.0 = self.0.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
    pub fn finish(&self) -> u64 {
        self.0
    }
}

pub fn fnv64(bytes: &[u8]) -> u64 {
    let mut h = Fnv64::default();
    h.write(bytes);
    h.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_primitives() {
        let mut v = Vec::new();
        v.put_u8(7);
        v.put_u16(0xBEEF);
        v.put_u32(0xDEAD_BEEF);
        v.put_u64(u64::MAX - 1);
        v.put_u128(u128::MAX - 2);
        let mut r = Reader::new(&v);
        assert_eq!(r.u8().unwrap(), 7);
        assert_eq!(r.u16().unwrap(), 0xBEEF);
        assert_eq!(r.u32().unwrap(), 0xDEAD_BEEF);
        assert_eq!(r.u64().unwrap(), u64::MAX - 1);
        assert_eq!(r.u128().unwrap(), u128::MAX - 2);
        r.finish().unwrap();
        assert!(r.u8().is_err());
    }

    #[test]
    fn count_guards_against_huge_lengths() {
        let mut v = Vec::new();
        v.put_u32(u32::MAX);
        let mut r = Reader::new(&v);
        assert!(r.count(16).is_err());
    }
}
