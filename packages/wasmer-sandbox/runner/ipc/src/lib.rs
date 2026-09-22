// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Experimental, bounded AgentFS stat protocol. All integers are little endian.
//! Each message is prefixed by a u32 payload length. No negotiated extensions,
//! implicit retries, JSON, SQL, or filesystem paths outside /workspace.
use std::io::{self, Read, Write};
pub const MAX_FRAME: usize = 8192;
pub const MAX_REQUESTS: u64 = 100_000;
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Request {
    pub scope: String,
    pub token: String,
    pub sequence: u64,
    pub path: String,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Stat {
    pub ino: u64,
    pub size: u64,
    pub mode: u64,
    pub atime: u64,
    pub mtime: u64,
    pub ctime: u64,
}
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Stale = 1,
    Invalid = 2,
    Access = 3,
    Missing = 4,
    NotDirectory = 5,
    Loop = 6,
    Unsupported = 7,
    Io = 8,
    Busy = 9,
}
impl Error {
    pub fn code(self) -> &'static str {
        match self {
            Self::Stale => "ESTALE",
            Self::Invalid => "EINVAL",
            Self::Access => "EACCES",
            Self::Missing => "ENOENT",
            Self::NotDirectory => "ENOTDIR",
            Self::Loop => "ELOOP",
            Self::Unsupported => "ENOTSUP",
            Self::Io => "EIO",
            Self::Busy => "EBUSY",
        }
    }
}
fn invalid() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "invalid AgentFS IPC frame")
}
fn string(out: &mut Vec<u8>, value: &str, max: usize) -> io::Result<()> {
    if value.len() > max {
        return Err(invalid());
    }
    out.extend_from_slice(&(value.len() as u16).to_le_bytes());
    out.extend_from_slice(value.as_bytes());
    Ok(())
}
fn take_string(input: &mut &[u8], max: usize) -> io::Result<String> {
    let mut len = [0; 2];
    input.read_exact(&mut len)?;
    let len = u16::from_le_bytes(len) as usize;
    if len > max || len > input.len() {
        return Err(invalid());
    }
    let value = std::str::from_utf8(&input[..len])
        .map_err(|_| invalid())?
        .to_owned();
    *input = &input[len..];
    Ok(value)
}
impl Request {
    pub fn encode(&self) -> io::Result<Vec<u8>> {
        let mut out = vec![1, 1]; // version, stat opcode
        string(&mut out, &self.scope, 1024)?;
        string(&mut out, &self.token, 128)?;
        out.extend_from_slice(&self.sequence.to_le_bytes());
        string(&mut out, &self.path, 4096)?;
        Ok(out)
    }
    pub fn decode(mut input: &[u8]) -> io::Result<Self> {
        let mut header = [0; 2];
        input.read_exact(&mut header)?;
        if header != [1, 1] {
            return Err(invalid());
        }
        let scope = take_string(&mut input, 1024)?;
        let token = take_string(&mut input, 128)?;
        let mut seq = [0; 8];
        input.read_exact(&mut seq)?;
        let path = take_string(&mut input, 4096)?;
        if !input.is_empty() {
            return Err(invalid());
        }
        Ok(Self {
            scope,
            token,
            sequence: u64::from_le_bytes(seq),
            path,
        })
    }
}
pub fn encode_response(result: Result<Stat, Error>) -> Vec<u8> {
    match result {
        Err(error) => vec![error as u8],
        Ok(s) => {
            let mut out = vec![0];
            for n in [s.ino, s.size, s.mode, s.atime, s.mtime, s.ctime] {
                out.extend_from_slice(&n.to_le_bytes());
            }
            out
        }
    }
}
pub fn decode_response(input: &[u8]) -> io::Result<Result<Stat, Error>> {
    if input.len() == 1 {
        return Ok(Err(match input[0] {
            1 => Error::Stale,
            2 => Error::Invalid,
            3 => Error::Access,
            4 => Error::Missing,
            5 => Error::NotDirectory,
            6 => Error::Loop,
            7 => Error::Unsupported,
            8 => Error::Io,
            9 => Error::Busy,
            _ => return Err(invalid()),
        }));
    }
    if input.len() != 49 || input[0] != 0 {
        return Err(invalid());
    }
    let mut values = input[1..]
        .chunks_exact(8)
        .map(|b| u64::from_le_bytes(b.try_into().unwrap()));
    Ok(Ok(Stat {
        ino: values.next().unwrap(),
        size: values.next().unwrap(),
        mode: values.next().unwrap(),
        atime: values.next().unwrap(),
        mtime: values.next().unwrap(),
        ctime: values.next().unwrap(),
    }))
}
pub fn read_frame(reader: &mut impl Read) -> io::Result<Vec<u8>> {
    let mut len = [0; 4];
    reader.read_exact(&mut len)?;
    let len = u32::from_le_bytes(len) as usize;
    if len == 0 || len > MAX_FRAME {
        return Err(invalid());
    }
    let mut out = vec![0; len];
    reader.read_exact(&mut out)?;
    Ok(out)
}
pub fn write_frame(writer: &mut impl Write, bytes: &[u8]) -> io::Result<()> {
    if bytes.is_empty() || bytes.len() > MAX_FRAME {
        return Err(invalid());
    }
    writer.write_all(&(bytes.len() as u32).to_le_bytes())?;
    writer.write_all(bytes)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn bounded_strict_frames() {
        let request = Request {
            scope: "Workspace:test".into(),
            token: "secret".into(),
            sequence: 1,
            path: "/workspace/file".into(),
        };
        let bytes = request.encode().unwrap();
        assert_eq!(Request::decode(&bytes).unwrap(), request);
        for n in 0..bytes.len() {
            assert!(Request::decode(&bytes[..n]).is_err());
        }
        let mut extra = bytes.clone();
        extra.push(0);
        assert!(Request::decode(&extra).is_err());
        assert!(read_frame(&mut &(u32::MAX.to_le_bytes())[..]).is_err());
        assert!(read_frame(&mut &[0, 0, 0, 0][..]).is_err());
        let mut bad = bytes;
        bad[0] = 2;
        assert!(Request::decode(&bad).is_err());
    }
    #[test]
    fn replies_round_trip_and_reject_truncation() {
        let s = Stat {
            ino: 4,
            size: 9,
            mode: 0o100644,
            atime: 1,
            mtime: 2,
            ctime: 3,
        };
        let bytes = encode_response(Ok(s.clone()));
        assert_eq!(decode_response(&bytes).unwrap(), Ok(s));
        for n in 0..bytes.len() {
            assert!(decode_response(&bytes[..n]).is_err());
        }
        assert_eq!(
            decode_response(&encode_response(Err(Error::Stale))).unwrap(),
            Err(Error::Stale)
        );
    }
}
