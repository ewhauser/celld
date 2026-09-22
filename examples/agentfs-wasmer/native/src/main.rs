use futures::future::BoxFuture;
use serde_json::{Value, json};
use std::{
    io::{self, SeekFrom},
    path::{Path, PathBuf},
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::Instant,
};
use tokio::io::{AsyncRead, AsyncSeek, AsyncWrite, ReadBuf};
use virtual_fs::{
    DirEntry, FileOpener, FileSystem, FileType, FsError, Metadata, OpenOptions, OpenOptionsConfig,
    ReadDir, VirtualFile,
};

#[derive(Debug, Clone)]
struct Remote {
    url: String,
    token: String,
    counts: Arc<Mutex<std::collections::BTreeMap<String, u64>>>,
}
impl Remote {
    fn call(&self, mut value: Value) -> virtual_fs::Result<Value> {
        let op = value["op"].as_str().unwrap().to_owned();
        *self.counts.lock().unwrap().entry(op).or_default() += 1;
        value["token"] = json!(self.token);
        let result = ureq::post(&self.url)
            .timeout(std::time::Duration::from_secs(10))
            .send_json(value);
        match result {
            Ok(r) => r.into_json().map_err(|_| FsError::IOError),
            Err(ureq::Error::Status(_, r)) => {
                let v: Value = r.into_json().unwrap_or_default();
                eprintln!("filesystem error: {v}");
                Err(match v["code"].as_str() {
                    Some("ENOENT") => FsError::EntryNotFound,
                    Some("EEXIST") => FsError::AlreadyExists,
                    _ => FsError::IOError,
                })
            }
            Err(e) => {
                eprintln!("filesystem transport: {e}");
                Err(FsError::IOError)
            }
        }
    }
    fn meta(v: Value) -> Metadata {
        Metadata {
            ft: if v["dir"].as_bool().unwrap_or(false) {
                FileType::new_dir()
            } else {
                FileType::new_file()
            },
            len: v["size"].as_u64().unwrap_or(0),
            ..Default::default()
        }
    }
}
impl FileSystem for Remote {
    fn readlink(&self, _: &Path) -> virtual_fs::Result<PathBuf> {
        Err(FsError::Unsupported)
    }
    fn read_dir(&self, p: &Path) -> virtual_fs::Result<ReadDir> {
        if p == Path::new("/") {
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
        if p == Path::new("/") {
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
            path: p.to_path_buf(),
        }))
    }
}
#[derive(Debug)]
struct RemoteFile {
    remote: Remote,
    handle: u64,
    offset: u64,
    path: PathBuf,
}
impl RemoteFile {
    fn call(&self, mut v: Value) -> io::Result<Value> {
        v["handle"] = json!(self.handle);
        self.remote.call(v).map_err(Into::into)
    }
}
// Deliberately blocking RPCs in poll_*: this probe has a dedicated native runner
// process and no other work on its calling thread. Production needs Pending +
// bounded RPC workers, especially for synchronous metadata/open/size interfaces.
impl AsyncRead for RemoteFile {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        b: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let v = self.call(json!({"op":"read","offset":self.offset,"size":b.remaining()}))?;
        let bytes: Vec<u8> = serde_json::from_value(v["data"].clone()).map_err(io::Error::other)?;
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
        let v = self.call(json!({"op":"write","offset":self.offset,"data":&b[..n]}))?;
        let n = v["written"].as_u64().unwrap();
        if self.path == Path::new("/workspace/output.txt") && self.offset == 0 {
            eprintln!(
                "do_read_mid_guest={:?}",
                self.remote.call(json!({"op":"do-read"}))
            );
        }
        self.offset += n;
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
        0
    }
    fn last_modified(&self) -> u64 {
        0
    }
    fn created_time(&self) -> u64 {
        0
    }
    fn size(&self) -> u64 {
        self.call(json!({"op":"fstat"}))
            .map(|v| v["size"].as_u64().unwrap())
            .unwrap_or(0)
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
fn main() -> anyhow::Result<()> {
    let args: Vec<_> = std::env::args().collect();
    let remote = Remote {
        url: args[1].clone(),
        token: args[2].clone(),
        counts: Default::default(),
    };
    let runtime = tokio::runtime::Runtime::new()?;
    let _guard = runtime.enter();
    let t = Instant::now();
    let mut store = wasmer::Store::default();
    let module = wasmer::Module::from_file(&store, &args[3])?;
    eprintln!("compile_ms={}", t.elapsed().as_millis());
    let t = Instant::now();
    let mut out = virtual_fs::Pipe::new();
    let mut err = virtual_fs::Pipe::new();
    let mut builder = wasmer_wasix::WasiEnv::builder("probe")
        .engine(store.engine().clone())
        .arg(&args[4])
        .fs(Arc::new(remote.clone()) as Arc<dyn FileSystem + Send + Sync>)
        .current_dir("/workspace")
        .stdout(Box::new(out.clone()))
        .stderr(Box::new(err.clone()));
    builder.add_preopen_build(|p| {
        p.directory("/workspace")
            .read(true)
            .write(true)
            .create(true)
    })?;
    let (instance, _env) = builder.instantiate(module, &mut store)?;
    eprintln!("instantiate_ms={}", t.elapsed().as_millis());
    let t = Instant::now();
    let result = instance
        .exports
        .get_typed_function::<(), ()>(&store, "_start")?
        .call(&mut store);
    let mut buf = [0; 4096];
    while let Some(n) = out.try_read(&mut buf) {
        if n == 0 {
            break;
        }
        print!("{}", String::from_utf8_lossy(&buf[..n]));
    }
    while let Some(n) = err.try_read(&mut buf) {
        if n == 0 {
            break;
        }
        eprint!("{}", String::from_utf8_lossy(&buf[..n]));
    }
    eprintln!(
        "execute_ms={}; calls={:?}",
        t.elapsed().as_millis(),
        remote.counts.lock().unwrap()
    );
    // Explicitly inspect the DO's own read while its /exec event still awaits.
    eprintln!(
        "do_read_during_exec={:?}",
        remote.call(json!({"op":"do-read"}))
    );
    result?;
    Ok(())
}
