use futures::future::BoxFuture;
use serde_json::{Value, json};
use std::{
    io::{self, SeekFrom},
    path::{Path, PathBuf},
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncSeek, AsyncWrite, ReadBuf};
use virtual_fs::{
    DirEntry, FileOpener, FileSystem, FileType, FsError, Metadata, OpenOptions, OpenOptionsConfig,
    ReadDir, VirtualFile,
};

#[derive(Debug, serde::Deserialize)]
pub struct NativeFilesystem {
    pub socket: String,
    pub scope: String,
}
#[derive(Debug)]
struct NativeConnection {
    config: NativeFilesystem,
    stream: Option<std::os::unix::net::UnixStream>,
    next: u64,
}
#[derive(Debug, Clone)]
pub struct Remote {
    native_filesystem: Option<Arc<Mutex<NativeConnection>>>,
    native_stat_calls: Arc<std::sync::atomic::AtomicU64>,
    http_stat_calls: Arc<std::sync::atomic::AtomicU64>,
    native_calls: Arc<std::sync::atomic::AtomicU64>,
    http_calls: Arc<std::sync::atomic::AtomicU64>,
    url: String,
    token: String,
    sequence: Arc<Mutex<u64>>,
    pub poisoned: Arc<std::sync::atomic::AtomicBool>,
    callback_token: String,
    pub mounted: bool,
}
impl Remote {
    pub fn new(url: String, token: String, callback_token: String) -> Self {
        Self {
            native_filesystem: None,
            native_stat_calls: Default::default(),
            http_stat_calls: Default::default(),
            native_calls: Default::default(),
            http_calls: Default::default(),
            url,
            token,
            callback_token,
            mounted: false,
            sequence: Arc::new(Mutex::new(0)),
            poisoned: Default::default(),
        }
    }
    pub fn stat_counts(&self) -> Value {
        use std::sync::atomic::Ordering;
        json!({"native":self.native_stat_calls.load(Ordering::Relaxed),"http":self.http_stat_calls.load(Ordering::Relaxed)})
    }
    pub fn fs_counts(&self) -> Value {
        use std::sync::atomic::Ordering;
        json!({"native":self.native_calls.load(Ordering::Relaxed),"http":self.http_calls.load(Ordering::Relaxed)})
    }
    pub fn set_native_filesystem(&mut self, config: Option<NativeFilesystem>) {
        self.native_filesystem = config.map(|config| {
            Arc::new(Mutex::new(NativeConnection {
                config,
                stream: None,
                next: 1,
            }))
        });
    }
    fn native_filesystem(
        &self,
        operation: Value,
        data: &[u8],
        state: &Mutex<NativeConnection>,
    ) -> io::Result<Result<celld_agentfs_ipc::Reply, celld_agentfs_ipc::Error>> {
        let mut state = state
            .lock()
            .map_err(|_| io::Error::other("IPC lock poisoned"))?;
        if state.stream.is_none() {
            let stream = std::os::unix::net::UnixStream::connect(&state.config.socket)?;
            stream.set_read_timeout(Some(std::time::Duration::from_secs(5)))?;
            stream.set_write_timeout(Some(std::time::Duration::from_secs(5)))?;
            state.stream = Some(stream);
        }
        let request = celld_agentfs_ipc::Request {
            scope: state.config.scope.clone(),
            token: self.token.clone(),
            sequence: state.next,
            operation,
            data: data.to_vec(),
        };
        state.next += 1;
        let stream = state.stream.as_mut().unwrap();
        celld_agentfs_ipc::write_frame(stream, &request.encode()?)?;
        celld_agentfs_ipc::decode_response(&celld_agentfs_ipc::read_frame(stream)?)
    }
    pub fn call(&self, value: Value) -> virtual_fs::Result<Value> {
        self.exchange(value, &[]).map(|reply| reply.value)
    }
    fn exchange(
        &self,
        mut value: Value,
        data: &[u8],
    ) -> virtual_fs::Result<celld_agentfs_ipc::Reply> {
        use std::sync::atomic::Ordering;
        // One sequence for all native operations, including heartbeat/sync.
        // A lost response is ambiguous: poison the command, never retry/fallback.
        let mut seq = self.sequence.lock().map_err(|_| FsError::IOError)?;
        if self.poisoned.load(Ordering::SeqCst) {
            return Err(FsError::IOError);
        }
        if self.mounted {
            for key in ["path", "to"] {
                if let Some(path) = value[key].as_str() {
                    value[key] = json!(format!("/workspace/{}", path.trim_start_matches('/')));
                }
            }
        }
        if let Some(native) = &self.native_filesystem {
            self.native_calls.fetch_add(1, Ordering::Relaxed);
            if value["op"] == "stat" {
                self.native_stat_calls.fetch_add(1, Ordering::Relaxed);
            }
            match self.native_filesystem(value, data, native) {
                Ok(Ok(reply)) => return Ok(reply),
                Ok(Err(code)) => {
                    if matches!(
                        code,
                        celld_agentfs_ipc::Error::Stale
                            | celld_agentfs_ipc::Error::Busy
                            | celld_agentfs_ipc::Error::Io
                    ) {
                        self.poisoned.store(true, Ordering::SeqCst);
                    }
                    return Err(fs_error(code.code()));
                }
                Err(_) => {
                    self.poisoned.store(true, Ordering::SeqCst);
                    return Err(FsError::IOError);
                }
            }
        }
        self.http_calls.fetch_add(1, Ordering::Relaxed);
        let is_read = value["op"] == "read";
        if value["op"] == "write" {
            value["data"] = json!(data);
        }
        if value["op"] == "stat" {
            self.http_stat_calls.fetch_add(1, Ordering::Relaxed);
        }
        *seq += 1;
        value["seq"] = json!(*seq);
        value["token"] = json!(self.token);
        let agent = ureq::AgentBuilder::new()
            .redirects(0)
            .timeout(std::time::Duration::from_secs(5))
            .build();
        let result = agent
            .post(&self.url)
            .set("Authorization", &format!("Bearer {}", self.callback_token))
            .send_json(value);
        let response = match result {
            Ok(r) => r,
            Err(_) => {
                self.poisoned.store(true, Ordering::SeqCst);
                return Err(FsError::IOError);
            }
        };
        let v: Value = match serde_json::from_reader(std::io::Read::take(
            response.into_reader(),
            4 * 1024 * 1024,
        )) {
            Ok(v) => v,
            Err(_) => {
                self.poisoned.store(true, Ordering::SeqCst);
                return Err(FsError::IOError);
            }
        };
        if let Some(code) = v["code"].as_str() {
            return Err(fs_error(code));
        }
        if v.get("value").is_none() {
            self.poisoned.store(true, Ordering::SeqCst);
            return Err(FsError::IOError);
        }
        let data = if is_read {
            serde_json::from_value(v["value"]["data"].clone()).map_err(|_| FsError::IOError)?
        } else {
            vec![]
        };
        Ok(celld_agentfs_ipc::Reply {
            value: v["value"].clone(),
            data,
        })
    }
    fn meta(v: Value) -> Metadata {
        Metadata {
            ft: if v["dir"].as_bool().unwrap_or(false) {
                FileType::new_dir()
            } else {
                FileType::new_file()
            },
            len: v["size"].as_u64().unwrap_or(0),
            accessed: v["atime"].as_u64().unwrap_or(0) * 1_000_000_000,
            modified: v["mtime"].as_u64().unwrap_or(0) * 1_000_000_000,
            created: v["ctime"].as_u64().unwrap_or(0) * 1_000_000_000,
        }
    }
}
impl FileSystem for Remote {
    fn readlink(&self, _: &Path) -> virtual_fs::Result<PathBuf> {
        Err(FsError::Unsupported)
    }
    fn read_dir(&self, p: &Path) -> virtual_fs::Result<ReadDir> {
        if p == Path::new("/") && !self.mounted {
            return Ok(ReadDir::new(vec![DirEntry {
                path: "/workspace".into(),
                metadata: Ok(Metadata {
                    ft: FileType::new_dir(),
                    ..Default::default()
                }),
            }]));
        }
        let v = self.call(json!({"op":"list","path":p}))?;
        Ok(ReadDir::new(
            v.as_array()
                .unwrap()
                .iter()
                .map(|v| DirEntry {
                    path: p.join(v["name"].as_str().unwrap()),
                    metadata: Ok(Self::meta(v.clone())),
                })
                .collect(),
        ))
    }
    fn create_dir(&self, p: &Path) -> virtual_fs::Result<()> {
        self.call(json!({"op":"mkdir","path":p})).map(|_| ())
    }
    fn remove_dir(&self, p: &Path) -> virtual_fs::Result<()> {
        self.call(json!({"op":"rmdir","path":p})).map(|_| ())
    }
    fn rename<'a>(&'a self, p: &'a Path, to: &'a Path) -> BoxFuture<'a, virtual_fs::Result<()>> {
        Box::pin(async move {
            self.call(json!({"op":"rename","path":p,"to":to}))
                .map(|_| ())
        })
    }
    fn metadata(&self, p: &Path) -> virtual_fs::Result<Metadata> {
        if p == Path::new("/") && !self.mounted {
            return Ok(Metadata {
                ft: FileType::new_dir(),
                ..Default::default()
            });
        }
        self.call(json!({"op":"stat","path":p})).map(Self::meta)
    }
    fn symlink_metadata(&self, p: &Path) -> virtual_fs::Result<Metadata> {
        self.metadata(p)
    }
    fn remove_file(&self, p: &Path) -> virtual_fs::Result<()> {
        self.call(json!({"op":"unlink","path":p})).map(|_| ())
    }
    fn new_open_options(&self) -> OpenOptions<'_> {
        OpenOptions::new(self)
    }
}
impl FileOpener for Remote {
    fn open(
        &self,
        p: &Path,
        c: &OpenOptionsConfig,
    ) -> virtual_fs::Result<Box<dyn VirtualFile + Send + Sync>> {
        let v=self.call(json!({"op":"open","path":p,"read":c.read(),"write":c.write(),"append":c.append(),"create":c.create(),"create_new":c.create_new(),"truncate":c.truncate()}))?;
        Ok(Box::new(RemoteFile {
            remote: self.clone(),
            handle: v["handle"].as_u64().unwrap(),
            offset: 0,
        }))
    }
}
#[derive(Debug)]
struct RemoteFile {
    remote: Remote,
    handle: u64,
    offset: u64,
}
impl RemoteFile {
    fn call(&self, mut v: Value) -> io::Result<Value> {
        v["handle"] = json!(self.handle);
        self.remote.call(v).map_err(Into::into)
    }
}
// Guest tasks have their own helper process and bounded 5s RPC deadline.
// Synchronous Wasmer host APIs require a blocking boundary; never run these
// polls on celld's executor. The supervisor imposes the overall deadline.
impl AsyncRead for RemoteFile {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        b: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let bytes = self.remote.exchange(json!({"op":"read","handle":self.handle,"offset":self.offset,"size":b.remaining().min(65536)}), &[])?.data;
        if bytes.len() > b.remaining() {
            return Poll::Ready(Err(io::ErrorKind::InvalidData.into()));
        }
        self.offset += bytes.len() as u64;
        b.put_slice(&bytes);
        Poll::Ready(Ok(()))
    }
}
impl AsyncWrite for RemoteFile {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        b: &[u8],
    ) -> Poll<io::Result<usize>> {
        let n = b.len().min(65536);
        let v = self
            .remote
            .exchange(
                json!({"op":"write","handle":self.handle,"offset":self.offset}),
                &b[..n],
            )?
            .value;
        let n = v["written"].as_u64().unwrap();
        if n > b.len() as u64 {
            return Poll::Ready(Err(io::ErrorKind::InvalidData.into()));
        }
        self.offset = v["offset"].as_u64().ok_or(io::ErrorKind::InvalidData)?;
        Poll::Ready(Ok(n as usize))
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.call(json!({"op":"sync"}))?;
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, c: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.poll_flush(c)
    }
}
impl AsyncSeek for RemoteFile {
    fn start_seek(mut self: Pin<&mut Self>, p: SeekFrom) -> io::Result<()> {
        let n = match p {
            SeekFrom::Start(n) => n as i128,
            SeekFrom::Current(n) => self.offset as i128 + n as i128,
            SeekFrom::End(n) => {
                self.call(json!({"op":"fstat"}))?["size"].as_u64().unwrap() as i128 + n as i128
            }
        };
        if n < 0 || n > u64::MAX as i128 {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        self.offset = n as u64;
        Ok(())
    }
    fn poll_complete(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<u64>> {
        Poll::Ready(Ok(self.offset))
    }
}
impl VirtualFile for RemoteFile {
    fn last_accessed(&self) -> u64 {
        self.attribute("atime") * 1_000_000_000
    }
    fn last_modified(&self) -> u64 {
        self.attribute("mtime") * 1_000_000_000
    }
    fn created_time(&self) -> u64 {
        self.attribute("ctime") * 1_000_000_000
    }
    fn size(&self) -> u64 {
        self.attribute("size")
    }
    fn set_len(&mut self, n: u64) -> virtual_fs::Result<()> {
        self.call(json!({"op":"truncate","size":n}))
            .map(|_| ())
            .map_err(Into::into)
    }
    fn unlink(&mut self) -> virtual_fs::Result<()> {
        Err(FsError::Unsupported)
    }
    fn poll_read_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<usize>> {
        Poll::Ready(Ok(self.size().saturating_sub(self.offset) as usize))
    }
    fn poll_write_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<usize>> {
        Poll::Ready(Ok(65536))
    }
}

impl RemoteFile {
    fn attribute(&self, name: &str) -> u64 {
        match self
            .call(json!({"op":"fstat"}))
            .ok()
            .and_then(|v| v[name].as_u64())
        {
            Some(n) => n,
            None => {
                // Scalar trait methods cannot return errors. Poison the entire
                // execution; the heartbeat terminates it and no output succeeds.
                self.remote
                    .poisoned
                    .store(true, std::sync::atomic::Ordering::SeqCst);
                0
            }
        }
    }
}
impl Drop for RemoteFile {
    fn drop(&mut self) {
        let _ = self.call(json!({"op":"close"}));
    }
}

fn fs_error(code: &str) -> FsError {
    match code {
        "ENOENT" => FsError::EntryNotFound,
        "EEXIST" => FsError::AlreadyExists,
        "EACCES" => FsError::PermissionDenied,
        "EBADF" => FsError::InvalidFd,
        "ENOTDIR" => FsError::BaseNotDirectory,
        "EISDIR" => FsError::NotAFile,
        "ENOTEMPTY" => FsError::DirectoryNotEmpty,
        "EINVAL" => FsError::InvalidInput,
        "ENOSPC" => FsError::StorageFull,
        "EMFILE" => FsError::IOError,
        "ENOTSUP" => FsError::Unsupported,
        "EBUSY" => FsError::Lock,
        _ => FsError::IOError,
    }
}

#[cfg(test)]
mod ipc_tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(1);
    fn socket() -> (String, std::os::unix::net::UnixListener) {
        let path = format!(
            "/tmp/celld-stat-{}-{}.sock",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        );
        let listener = std::os::unix::net::UnixListener::bind(&path).unwrap();
        (path, listener)
    }
    #[test]
    fn native_errors_preserve_connection_and_do_not_advance_http_sequence() {
        let (path, listener) = socket();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            for seq in 1..=2 {
                let r = celld_agentfs_ipc::Request::decode(
                    &celld_agentfs_ipc::read_frame(&mut stream).unwrap(),
                )
                .unwrap();
                assert_eq!(r.sequence, seq);
                assert_eq!(r.operation["path"], "/workspace/file");
                celld_agentfs_ipc::write_frame(
                    &mut stream,
                    &celld_agentfs_ipc::encode_response(Err(celld_agentfs_ipc::Error::Missing)),
                )
                .unwrap();
            }
        });
        let mut remote = Remote::new(
            "http://127.0.0.1:1".into(),
            "token".into(),
            "callback".into(),
        );
        remote.mounted = true;
        remote.set_native_filesystem(Some(NativeFilesystem {
            socket: path.clone(),
            scope: "Workspace:test".into(),
        }));
        for _ in 0..2 {
            assert_eq!(
                remote.call(json!({"op":"stat","path":"/file"})),
                Err(FsError::EntryNotFound)
            );
        }
        assert_eq!(*remote.sequence.lock().unwrap(), 0);
        assert!(!remote.poisoned.load(Ordering::SeqCst));
        server.join().unwrap();
        std::fs::remove_file(path).unwrap();
    }
    #[test]
    fn lost_response_poisons_execution_without_retry_or_http_fallback() {
        let (path, listener) = socket();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let r = celld_agentfs_ipc::Request::decode(
                &celld_agentfs_ipc::read_frame(&mut stream).unwrap(),
            )
            .unwrap();
            assert_eq!(r.operation["op"], "write");
            assert_eq!(r.data, b"ambiguous mutation");
        });
        let mut remote = Remote::new(
            "http://127.0.0.1:1".into(),
            "token".into(),
            "callback".into(),
        );
        remote.set_native_filesystem(Some(NativeFilesystem {
            socket: path.clone(),
            scope: "Workspace:test".into(),
        }));
        assert!(
            remote
                .exchange(
                    json!({"op":"write","handle":1,"offset":0}),
                    b"ambiguous mutation"
                )
                .is_err()
        );
        server.join().unwrap();
        assert!(remote.poisoned.load(Ordering::SeqCst));
        assert!(
            remote
                .exchange(
                    json!({"op":"write","handle":1,"offset":0}),
                    b"ambiguous mutation"
                )
                .is_err()
        );
        assert_eq!(remote.fs_counts(), json!({"native":1,"http":0}));
        std::fs::remove_file(path).unwrap();
    }
}
