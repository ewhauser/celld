use futures::future::BoxFuture;
use std::{
    path::{Component, Path, PathBuf},
    sync::Arc,
};
use virtual_fs::{
    FileOpener, FileSystem, FsError, Metadata, OpenOptions, OpenOptionsConfig, ReadDir, Result,
    VirtualFile,
};

#[derive(Debug)]
struct Policy {
    enforced: Arc<std::sync::atomic::AtomicBool>,
    inner: wasmer_wasix::fs::WasiFsRoot,
}
fn normalized(p: &Path) -> Result<PathBuf> {
    let mut path = PathBuf::from("/");
    for component in p.components() {
        match component {
            Component::Normal(v) => path.push(v),
            Component::ParentDir => {
                if !path.pop() {
                    return Err(FsError::PermissionDenied);
                }
            }
            Component::CurDir | Component::RootDir => {}
            _ => return Err(FsError::PermissionDenied),
        }
    }
    Ok(path)
}
fn domain(p: &Path) -> Result<u8> {
    let p = normalized(p)?;
    Ok(if p.starts_with("/workspace") {
        1
    } else if p.starts_with("/tmp") {
        2
    } else {
        0
    })
}
impl Policy {
    fn writable(&self, p: &Path) -> Result<()> {
        if self.enforced.load(std::sync::atomic::Ordering::SeqCst) && domain(p)? == 0 {
            Err(FsError::PermissionDenied)
        } else {
            Ok(())
        }
    }
}
impl FileSystem for Policy {
    fn readlink(&self, p: &Path) -> Result<PathBuf> {
        self.inner.readlink(p)
    }
    fn read_dir(&self, p: &Path) -> Result<ReadDir> {
        self.inner.read_dir(p)
    }
    fn create_dir(&self, p: &Path) -> Result<()> {
        self.writable(p)?;
        self.inner.create_dir(p)
    }
    fn remove_dir(&self, p: &Path) -> Result<()> {
        self.writable(p)?;
        self.inner.remove_dir(p)
    }
    fn rename<'a>(&'a self, p: &'a Path, q: &'a Path) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            self.writable(p)?;
            self.writable(q)?;
            if domain(p)? != domain(q)? {
                return Err(FsError::InvalidInput);
            }
            self.inner.rename(p, q).await
        })
    }
    fn metadata(&self, p: &Path) -> Result<Metadata> {
        self.inner.metadata(p)
    }
    fn symlink_metadata(&self, p: &Path) -> Result<Metadata> {
        self.inner.symlink_metadata(p)
    }
    fn remove_file(&self, p: &Path) -> Result<()> {
        self.writable(p)?;
        self.inner.remove_file(p)
    }
    fn new_open_options(&self) -> OpenOptions<'_> {
        OpenOptions::new(self)
    }
}
impl FileOpener for Policy {
    fn open(&self, p: &Path, c: &OpenOptionsConfig) -> Result<Box<dyn VirtualFile + Send + Sync>> {
        if c.write() || c.append() || c.create() || c.create_new() || c.truncate() {
            let path = normalized(p)?;
            if ![
                Path::new("/dev/stdout"),
                Path::new("/dev/stderr"),
                Path::new("/dev/null"),
                Path::new("/dev/tty"),
            ]
            .contains(&path.as_path())
            {
                self.writable(&path)?;
            }
        }
        self.inner.new_open_options().options(c.clone()).open(p)
    }
}
pub fn build(builder: wasmer_wasix::WasiEnvBuilder) -> anyhow::Result<wasmer_wasix::WasiEnv> {
    let enforced = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let during_build = enforced.clone();
    let env = builder
        .setup_fs(Box::new(move |_, fs| {
            fs.root_fs = wasmer_wasix::fs::WasiFsRoot::from_filesystem(Arc::new(Policy {
                inner: fs.root_fs.clone(),
                enforced: during_build.clone(),
            }));
            Ok(())
        }))
        .build()?;
    // Package command installation occurs during build, before any guest code.
    // Seal after that step. Runtime packages have no mutable mount directives.
    enforced.store(true, std::sync::atomic::Ordering::SeqCst);
    Ok(env)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn mutation_domains_follow_normalized_paths() {
        assert_eq!(domain(Path::new("/workspace/a/../b")).unwrap(), 1);
        assert_eq!(domain(Path::new("/workspace/../bin/x")).unwrap(), 0);
        assert_eq!(domain(Path::new("/workspace-other/x")).unwrap(), 0);
        assert!(domain(Path::new("/../../x")).is_err());
    }
}
