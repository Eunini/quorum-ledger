//! Durable storage for Raft's persistent state (term, vote, log).
//!
//! The replica talks to the [`Storage`] trait. The only implementation is a
//! checksummed, append-only write-ahead log ([`Wal`]) on top of a
//! [`BlockDevice`]. The device is a real file in the server and an in-memory
//! fault-injecting disk in the simulator, so the simulator exercises the exact
//! same encoding and recovery code that runs in production.
//!
//! Record layout (little-endian):
//!
//! ```text
//! u32 payload_len | u32 crc32(payload) | payload
//! payload = u8 kind | body
//!   kind 1 HardState: u64 term | u8 voted_for (0xFF = none)
//!   kind 2 Entry:     encoded Entry
//!   kind 3 Truncate:  u64 from_index (drop entries with index >= from_index)
//! ```
//!
//! Recovery replays records in order and stops at the first record that is
//! incomplete, fails its checksum or does not decode. Everything after that
//! point is cut off: it can only be a torn or corrupted write that was never
//! fsynced, and the replica never acknowledges anything before fsync.

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;

use ledger::codec::{Put, Reader};

use crate::message::Entry;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct HardState {
    pub term: u64,
    pub voted_for: Option<u8>,
}

#[derive(Debug, Clone, Default)]
pub struct Recovered {
    pub hard_state: HardState,
    pub log: Vec<Entry>,
    /// Number of valid records replayed.
    pub records: usize,
    /// Bytes discarded at the tail (torn/corrupt writes).
    pub discarded_bytes: usize,
}

/// What Raft needs from durable storage. Writes may be buffered; only after
/// `sync` returns are they guaranteed to survive a crash.
pub trait Storage {
    fn recover(&mut self) -> io::Result<Recovered>;
    fn set_hard_state(&mut self, hs: HardState) -> io::Result<()>;
    fn append(&mut self, entries: &[Entry]) -> io::Result<()>;
    fn truncate_from(&mut self, index: u64) -> io::Result<()>;
    fn sync(&mut self) -> io::Result<()>;
}

/// Byte-level append-only device.
pub trait BlockDevice {
    fn read_all(&mut self) -> io::Result<Vec<u8>>;
    /// Appends bytes. Not durable until `sync`.
    fn write(&mut self, data: &[u8]) -> io::Result<()>;
    fn sync(&mut self) -> io::Result<()>;
    /// Durably cuts the device to `len` bytes (used by recovery only).
    fn truncate(&mut self, len: u64) -> io::Result<()>;
}

const KIND_HARD_STATE: u8 = 1;
const KIND_ENTRY: u8 = 2;
const KIND_TRUNCATE: u8 = 3;
const HEADER_LEN: usize = 8;

pub struct Wal<D: BlockDevice> {
    dev: D,
    scratch: Vec<u8>,
}

impl<D: BlockDevice> Wal<D> {
    pub fn new(dev: D) -> Self {
        Wal {
            dev,
            scratch: Vec::with_capacity(4096),
        }
    }

    pub fn device(&self) -> &D {
        &self.dev
    }

    fn write_record(&mut self, kind: u8, body: impl FnOnce(&mut Vec<u8>)) -> io::Result<()> {
        self.scratch.clear();
        self.scratch.extend_from_slice(&[0u8; HEADER_LEN]);
        self.scratch.put_u8(kind);
        body(&mut self.scratch);
        let payload_len = (self.scratch.len() - HEADER_LEN) as u32;
        let crc = crc32fast::hash(&self.scratch[HEADER_LEN..]);
        self.scratch[0..4].copy_from_slice(&payload_len.to_le_bytes());
        self.scratch[4..8].copy_from_slice(&crc.to_le_bytes());
        self.dev.write(&self.scratch)
    }
}

/// Parses a WAL image. Returns the recovered state and the length of the
/// valid prefix.
pub fn parse_wal(bytes: &[u8]) -> (Recovered, usize) {
    let mut rec = Recovered::default();
    let mut pos = 0usize;
    loop {
        if bytes.len() - pos < HEADER_LEN {
            break;
        }
        let len = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
        let crc = u32::from_le_bytes(bytes[pos + 4..pos + 8].try_into().unwrap());
        let start = pos + HEADER_LEN;
        if len == 0 || bytes.len() - start < len {
            break;
        }
        let payload = &bytes[start..start + len];
        if crc32fast::hash(payload) != crc {
            break;
        }
        let mut r = Reader::new(&payload[1..]);
        let ok = match payload[0] {
            KIND_HARD_STATE => (|| {
                let term = r.u64().ok()?;
                let v = r.u8().ok()?;
                r.finish().ok()?;
                rec.hard_state = HardState {
                    term,
                    voted_for: (v != u8::MAX).then_some(v),
                };
                Some(())
            })(),
            KIND_ENTRY => (|| {
                let e = Entry::decode(&mut r).ok()?;
                r.finish().ok()?;
                if e.index != rec.log.len() as u64 + 1 {
                    return None;
                }
                rec.log.push(e);
                Some(())
            })(),
            KIND_TRUNCATE => (|| {
                let from = r.u64().ok()?;
                r.finish().ok()?;
                if from == 0 {
                    return None;
                }
                rec.log.truncate((from - 1) as usize);
                Some(())
            })(),
            _ => None,
        };
        if ok.is_none() {
            break;
        }
        rec.records += 1;
        pos = start + len;
    }
    rec.discarded_bytes = bytes.len() - pos;
    (rec, pos)
}

impl<D: BlockDevice> Storage for Wal<D> {
    fn recover(&mut self) -> io::Result<Recovered> {
        let bytes = self.dev.read_all()?;
        let (rec, valid) = parse_wal(&bytes);
        if valid != bytes.len() {
            self.dev.truncate(valid as u64)?;
        }
        Ok(rec)
    }

    fn set_hard_state(&mut self, hs: HardState) -> io::Result<()> {
        self.write_record(KIND_HARD_STATE, |b| {
            b.put_u64(hs.term);
            b.put_u8(hs.voted_for.unwrap_or(u8::MAX));
        })
    }

    fn append(&mut self, entries: &[Entry]) -> io::Result<()> {
        for e in entries {
            self.write_record(KIND_ENTRY, |b| e.encode(b))?;
        }
        Ok(())
    }

    fn truncate_from(&mut self, index: u64) -> io::Result<()> {
        self.write_record(KIND_TRUNCATE, |b| b.put_u64(index))
    }

    fn sync(&mut self) -> io::Result<()> {
        self.dev.sync()
    }
}

/// File-backed device. Writes are buffered in memory and flushed with a single
/// `write` + `fdatasync` on `sync` (group commit). A process crash loses the
/// buffer, which is equivalent to unsynced page-cache data being lost.
pub struct FileDevice {
    file: File,
    buf: Vec<u8>,
}

impl FileDevice {
    pub fn open(path: &Path) -> io::Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(path)?;
        Ok(FileDevice {
            file,
            buf: Vec::with_capacity(1 << 20),
        })
    }
}

impl BlockDevice for FileDevice {
    fn read_all(&mut self) -> io::Result<Vec<u8>> {
        let mut out = Vec::new();
        self.file.seek(SeekFrom::Start(0))?;
        self.file.read_to_end(&mut out)?;
        Ok(out)
    }

    fn write(&mut self, data: &[u8]) -> io::Result<()> {
        self.buf.extend_from_slice(data);
        Ok(())
    }

    fn sync(&mut self) -> io::Result<()> {
        if !self.buf.is_empty() {
            self.file.write_all(&self.buf)?;
            self.buf.clear();
        }
        self.file.sync_data()
    }

    fn truncate(&mut self, len: u64) -> io::Result<()> {
        self.file.set_len(len)?;
        self.file.sync_all()
    }
}

/// Plain in-memory device with no faults (unit tests and benchmarks).
#[derive(Default, Clone)]
pub struct MemDevice {
    pub bytes: Vec<u8>,
}

impl BlockDevice for MemDevice {
    fn read_all(&mut self) -> io::Result<Vec<u8>> {
        Ok(self.bytes.clone())
    }
    fn write(&mut self, data: &[u8]) -> io::Result<()> {
        self.bytes.extend_from_slice(data);
        Ok(())
    }
    fn sync(&mut self) -> io::Result<()> {
        Ok(())
    }
    fn truncate(&mut self, len: u64) -> io::Result<()> {
        self.bytes.truncate(len as usize);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::Payload;

    fn entry(term: u64, index: u64) -> Entry {
        Entry {
            term,
            index,
            timestamp: index * 10,
            payload: Payload::Noop,
        }
    }

    fn write_sample(wal: &mut Wal<MemDevice>) {
        wal.set_hard_state(HardState {
            term: 1,
            voted_for: Some(0),
        })
        .unwrap();
        wal.append(&[entry(1, 1), entry(1, 2), entry(1, 3)])
            .unwrap();
        wal.set_hard_state(HardState {
            term: 2,
            voted_for: None,
        })
        .unwrap();
        wal.truncate_from(3).unwrap();
        wal.append(&[entry(2, 3), entry(2, 4)]).unwrap();
        wal.sync().unwrap();
    }

    #[test]
    fn recovers_hard_state_and_log_with_truncation() {
        let mut wal = Wal::new(MemDevice::default());
        write_sample(&mut wal);
        let rec = wal.recover().unwrap();
        assert_eq!(
            rec.hard_state,
            HardState {
                term: 2,
                voted_for: None
            }
        );
        let terms: Vec<_> = rec.log.iter().map(|e| (e.index, e.term)).collect();
        assert_eq!(terms, vec![(1, 1), (2, 1), (3, 2), (4, 2)]);
        assert_eq!(rec.discarded_bytes, 0);
    }

    #[test]
    fn every_torn_tail_recovers_a_prefix_state() {
        let mut wal = Wal::new(MemDevice::default());
        write_sample(&mut wal);
        let full = wal.device().bytes.clone();
        let (full_rec, _) = parse_wal(&full);
        let mut last_records = 0;
        for cut in 0..=full.len() {
            let (rec, valid) = parse_wal(&full[..cut]);
            assert!(valid <= cut);
            assert!(
                rec.records >= last_records,
                "records must grow monotonically with the cut"
            );
            last_records = rec.records;
            assert!(rec.log.len() <= 4);
            for (i, e) in rec.log.iter().enumerate() {
                assert_eq!(e.index, i as u64 + 1);
            }
        }
        assert_eq!(last_records, full_rec.records);
    }

    #[test]
    fn bit_flips_are_detected() {
        let mut wal = Wal::new(MemDevice::default());
        write_sample(&mut wal);
        let full = wal.device().bytes.clone();
        let (full_rec, _) = parse_wal(&full);
        for i in 0..full.len() {
            for bit in [0u8, 3, 7] {
                let mut corrupt = full.clone();
                corrupt[i] ^= 1 << bit;
                let (rec, valid) = parse_wal(&corrupt);
                // Recovery must stop at or before the damaged record.
                assert!(valid <= i, "flip at byte {i} bit {bit} not detected");
                assert!(
                    rec.records < full_rec.records,
                    "flip at byte {i} bit {bit} not detected"
                );
            }
        }
    }

    #[test]
    fn recovery_cuts_garbage_so_new_writes_are_readable() {
        let mut dev = MemDevice::default();
        {
            let mut wal = Wal::new(dev.clone());
            write_sample(&mut wal);
            dev = wal.device().clone();
        }
        dev.bytes.extend_from_slice(&[0xAB; 13]);
        let mut wal = Wal::new(dev);
        let rec = wal.recover().unwrap();
        assert_eq!(rec.discarded_bytes, 13);
        wal.append(&[entry(2, 5)]).unwrap();
        wal.sync().unwrap();
        let rec = wal.recover().unwrap();
        assert_eq!(rec.log.len(), 5);
    }

    #[test]
    fn file_device_roundtrip() {
        let dir = std::env::temp_dir().join(format!("ql-wal-test-{}", std::process::id()));
        let path = dir.join("replica.wal");
        let _ = std::fs::remove_file(&path);
        {
            let mut wal = Wal::new(FileDevice::open(&path).unwrap());
            assert_eq!(wal.recover().unwrap().log.len(), 0);
            wal.set_hard_state(HardState {
                term: 5,
                voted_for: Some(1),
            })
            .unwrap();
            wal.append(&[entry(5, 1), entry(5, 2)]).unwrap();
            wal.sync().unwrap();
            // Unsynced write is lost when the process "crashes" (drop).
            wal.append(&[entry(5, 3)]).unwrap();
        }
        // Simulate a torn write at the tail.
        {
            let mut f = OpenOptions::new().append(true).open(&path).unwrap();
            f.write_all(&[1, 2, 3]).unwrap();
        }
        let mut wal = Wal::new(FileDevice::open(&path).unwrap());
        let rec = wal.recover().unwrap();
        assert_eq!(
            rec.hard_state,
            HardState {
                term: 5,
                voted_for: Some(1)
            }
        );
        assert_eq!(rec.log.len(), 2);
        assert_eq!(rec.discarded_bytes, 3);
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
