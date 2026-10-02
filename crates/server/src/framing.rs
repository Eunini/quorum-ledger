//! Length-prefixed framing: `u32 little-endian body length || body`.

use std::io::{self, Read, Write};

use consensus::Frame;

/// Hard upper bound on a frame body; larger lengths are treated as a protocol
/// error and the connection is closed.
pub const MAX_FRAME_BYTES: usize = 64 << 20;

pub fn read_frame(r: &mut impl Read) -> io::Result<Frame> {
    let mut len = [0u8; 4];
    r.read_exact(&mut len)?;
    let len = u32::from_le_bytes(len) as usize;
    if len == 0 || len > MAX_FRAME_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("bad frame length {len}"),
        ));
    }
    let mut body = vec![0u8; len];
    r.read_exact(&mut body)?;
    Frame::decode(&body).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

pub fn write_frame(w: &mut impl Write, frame: &Frame) -> io::Result<()> {
    w.write_all(&frame.to_wire())
}
