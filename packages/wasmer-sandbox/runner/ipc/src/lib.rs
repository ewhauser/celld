// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//! Version 2 local AgentFS protocol: bounded JSON metadata followed by raw bytes.
//! Outer frames and metadata lengths are u32 little endian. No file data is JSON.
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io::{self, Read, Write};
pub const MAX_FRAME: usize = 2 * 1024 * 1024;
pub const MAX_DATA: usize = 1024 * 1024;
pub const MAX_REQUESTS: u64 = 100_000;
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub scope: String,
    pub token: String,
    pub sequence: u64,
    pub operation: Value,
    #[serde(skip)]
    pub data: Vec<u8>,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Reply {
    pub value: Value,
    #[serde(skip)]
    pub data: Vec<u8>,
}
impl Reply {
    pub fn new(value: Value) -> Self {
        Self {
            value,
            data: vec![],
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Stat {
    pub ino: u64,
    pub size: u64,
    pub mode: u64,
    pub atime: u64,
    pub mtime: u64,
    pub ctime: u64,
}
impl Stat {
    pub fn value(&self) -> Value {
        let mut value = serde_json::to_value(self).unwrap();
        value["dir"] = Value::Bool(self.mode & 0o170000 == 0o40000);
        value
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Error {
    #[serde(rename = "ESTALE")]
    Stale,
    #[serde(rename = "EINVAL")]
    Invalid,
    #[serde(rename = "EACCES")]
    Access,
    #[serde(rename = "ENOENT")]
    Missing,
    #[serde(rename = "ENOTDIR")]
    NotDirectory,
    #[serde(rename = "ELOOP")]
    Loop,
    #[serde(rename = "ENOTSUP")]
    Unsupported,
    #[serde(rename = "EIO")]
    Io,
    #[serde(rename = "EBUSY")]
    Busy,
    #[serde(rename = "EEXIST")]
    Exists,
    #[serde(rename = "EBADF")]
    BadHandle,
    #[serde(rename = "EMFILE")]
    Handles,
    #[serde(rename = "ENOSPC")]
    Space,
    #[serde(rename = "EISDIR")]
    IsDirectory,
    #[serde(rename = "ENOTEMPTY")]
    NotEmpty,
    #[serde(rename = "EFBIG")]
    Big,
    #[serde(rename = "ENAMETOOLONG")]
    Name,
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
            Self::Exists => "EEXIST",
            Self::BadHandle => "EBADF",
            Self::Handles => "EMFILE",
            Self::Space => "ENOSPC",
            Self::IsDirectory => "EISDIR",
            Self::NotEmpty => "ENOTEMPTY",
            Self::Big => "EFBIG",
            Self::Name => "ENAMETOOLONG",
        }
    }
}
fn invalid() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "invalid AgentFS IPC frame")
}
fn pack(metadata: &impl Serialize, data: &[u8]) -> io::Result<Vec<u8>> {
    let json = serde_json::to_vec(metadata)?;
    if data.len() > MAX_DATA || json.len() + data.len() + 5 > MAX_FRAME {
        return Err(invalid());
    }
    let mut out = Vec::with_capacity(5 + json.len() + data.len());
    out.push(2);
    out.extend_from_slice(&(json.len() as u32).to_le_bytes());
    out.extend(json);
    out.extend_from_slice(data);
    Ok(out)
}
fn unpack(input: &[u8], max_metadata: usize) -> io::Result<(&[u8], &[u8])> {
    if input.len() < 5 || input.len() > MAX_FRAME || input[0] != 2 {
        return Err(invalid());
    }
    let n = u32::from_le_bytes(input[1..5].try_into().unwrap()) as usize;
    if n > max_metadata || n > input.len() - 5 || input.len() - 5 - n > MAX_DATA {
        return Err(invalid());
    }
    Ok((&input[5..5 + n], &input[5 + n..]))
}
impl Request {
    pub fn encode(&self) -> io::Result<Vec<u8>> {
        let out = pack(self, &self.data)?;
        Self::decode(&out)?;
        Ok(out)
    }
    pub fn decode(input: &[u8]) -> io::Result<Self> {
        let (json, data) = unpack(input, 16384)?;
        let mut r: Self = serde_json::from_slice(json)?;
        if r.scope.len() > 1024 || r.token.len() > 128 || !r.operation.is_object() {
            return Err(invalid());
        }
        r.data = data.to_vec();
        Ok(r)
    }
}
pub fn encode_response(result: Result<Reply, Error>) -> Vec<u8> {
    let encoded = match result {
        Ok(r) => pack(&serde_json::json!({"value":r.value}), &r.data),
        Err(e) => pack(&serde_json::json!({"code":e}), &[]),
    };
    encoded.unwrap_or_else(|_| pack(&serde_json::json!({"code":Error::Big}), &[]).unwrap())
}
pub fn decode_response(input: &[u8]) -> io::Result<Result<Reply, Error>> {
    let (json, data) = unpack(input, MAX_FRAME)?;
    let v: Value = serde_json::from_slice(json)?;
    if v.as_object().is_none_or(|v| v.len() != 1) {
        return Err(invalid());
    }
    if let Some(code) = v.get("code") {
        if !data.is_empty() {
            return Err(invalid());
        }
        return Ok(Err(serde_json::from_value(code.clone())?));
    }
    Ok(Ok(Reply {
        value: v.get("value").ok_or_else(invalid)?.clone(),
        data: data.to_vec(),
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
    fn binary_roundtrip_and_bounds() {
        let r = Request {
            scope: "Workspace:test".into(),
            token: "secret".into(),
            sequence: 1,
            operation: serde_json::json!({"op":"write","handle":1,"offset":0}),
            data: (0..=255).collect(),
        };
        let b = r.encode().unwrap();
        assert_eq!(Request::decode(&b).unwrap(), r);
        for n in 0..5 {
            assert!(Request::decode(&b[..n]).is_err());
        }
        let mut bad = b.clone();
        bad[0] = 1;
        assert!(Request::decode(&bad).is_err());
        let mut big = r;
        big.data = vec![0; MAX_DATA + 1];
        assert!(big.encode().is_err());
        assert!(read_frame(&mut &u32::MAX.to_le_bytes()[..]).is_err());
        let reply = Reply {
            value: Value::Null,
            data: vec![0, 255, 1],
        };
        assert_eq!(
            decode_response(&encode_response(Ok(reply.clone()))).unwrap(),
            Ok(reply)
        );
        assert_eq!(
            decode_response(&encode_response(Err(Error::Stale))).unwrap(),
            Err(Error::Stale)
        );
        assert!(decode_response(&[2, 255, 255, 255, 255]).is_err());
    }
}
