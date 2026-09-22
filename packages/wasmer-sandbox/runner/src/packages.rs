use sha2::{Digest, Sha256};
use std::{collections::HashMap, sync::Arc};
use wasmer_wasix::{
    bin_factory::BinaryPackage,
    runtime::{
        package_loader::{PackageLoader, load_package_tree},
        resolver::{
            DistributionInfo, InMemorySource, PackageInfo, PackageSummary, Resolution, WebcHash,
        },
    },
};
use webc::Container;

#[derive(Debug, serde::Deserialize)]
pub struct Package {
    pub path: String,
    pub sha256: String,
    pub package: Option<String>,
}
#[derive(Debug)]
pub struct OfflineLoader(HashMap<String, Container>);
#[async_trait::async_trait]
impl PackageLoader for OfflineLoader {
    async fn load(&self, summary: &PackageSummary) -> anyhow::Result<Container> {
        self.0
            .get(summary.dist.webc.path().trim_start_matches('/'))
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("dependency is not in the pinned package set"))
    }
    async fn load_package_tree(
        &self,
        root: &Container,
        resolution: &Resolution,
        local: bool,
    ) -> anyhow::Result<BinaryPackage> {
        load_package_tree(root, self, resolution, local).await
    }
}
pub fn parse(bytes: Vec<u8>) -> anyhow::Result<Container> {
    let version = webc::detect(bytes.as_slice())?;
    Ok(Container::from_bytes_and_version(bytes.into(), version)?)
}
pub fn offline(packages: &[Package]) -> anyhow::Result<(InMemorySource, OfflineLoader)> {
    let mut source = InMemorySource::new();
    let mut containers = HashMap::new();
    anyhow::ensure!(packages.len() <= 32, "too many packages");
    for package in packages {
        let bytes = std::fs::read(&package.path)?;
        anyhow::ensure!(bytes.len() <= 256 * 1024 * 1024, "package too large");
        let hash = format!("{:x}", Sha256::digest(&bytes));
        anyhow::ensure!(hash == package.sha256, "dependency hash mismatch");
        let container = parse(bytes)?;
        let id = if let Some(alias) = &package.package {
            let (name, version) = alias
                .rsplit_once('@')
                .ok_or_else(|| anyhow::anyhow!("package identity needs an exact version"))?;
            wasmer_config::package::PackageId::Named(wasmer_config::package::NamedPackageId {
                full_name: name.into(),
                version: version.parse()?,
            })
        } else {
            PackageInfo::package_id_from_manifest(container.manifest())?.unwrap_or_else(|| {
                wasmer_config::package::PackageId::Hash(
                    wasmer_config::package::PackageHash::from_sha256_bytes(
                        container.webc_hash().expect("versioned WebC hash"),
                    ),
                )
            })
        };
        let summary = PackageSummary {
            pkg: PackageInfo::from_manifest(id, container.manifest(), container.version())?,
            dist: DistributionInfo {
                webc: format!("https://offline.invalid/{hash}").parse()?,
                webc_sha256: WebcHash::from_bytes(
                    container
                        .webc_hash()
                        .ok_or_else(|| anyhow::anyhow!("package hash missing"))?,
                ),
            },
        };
        source.add(summary);
        containers.insert(hash, container);
    }
    Ok((source, OfflineLoader(containers)))
}

#[derive(Debug, Default)]
pub struct MemoryLimit(std::sync::atomic::AtomicUsize);
impl virtual_fs::limiter::FsMemoryLimiter for MemoryLimit {
    fn on_grow(&self, n: usize) -> Result<(), virtual_fs::FsError> {
        use std::sync::atomic::Ordering;
        self.0
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |used| {
                used.checked_add(n).filter(|next| *next <= 16 * 1024 * 1024)
            })
            .map(|_| ())
            .map_err(|_| virtual_fs::FsError::StorageFull)
    }
    fn on_shrink(&self, n: usize) {
        self.0.fetch_sub(n, std::sync::atomic::Ordering::SeqCst);
    }
}
pub fn root(
    out: &crate::output::Output,
    err: &crate::output::Output,
    input: &str,
) -> wasmer_wasix::fs::WasiFsRoot {
    let limiter = Arc::new(MemoryLimit::default());
    let root = virtual_fs::RootFileSystemBuilder::default()
        .with_memory_limiter(limiter.clone())
        .with_stdin(Box::new(virtual_fs::StaticFile::new(
            shared_buffer::OwnedBuffer::from(input.as_bytes().to_vec()),
        )))
        .with_stdout(Box::new(out.clone()))
        .with_stderr(Box::new(err.clone()))
        .with_tty(Box::<virtual_fs::NullFile>::default())
        .build();
    wasmer_wasix::fs::WasiFsRoot::from_mount_fs(root).with_memory_limiter_opt(Some(limiter))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;
    use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
    use virtual_fs::{FileSystem, limiter::TrackedVec};

    #[test]
    fn rejected_growth_preserves_bytes_and_accounting() {
        let limit = Arc::new(MemoryLimit::default());
        let mut bytes = TrackedVec::new(Some(limit.clone()));
        bytes.extend_from_slice(b"keep").unwrap();
        let used = limit.0.load(Ordering::SeqCst);
        for _ in 0..2 {
            assert!(bytes.resize(17 * 1024 * 1024, 0).is_err());
            assert_eq!(bytes.len(), 4);
            assert_eq!(&*bytes, b"keep");
            assert_eq!(limit.0.load(Ordering::SeqCst), used);
        }
        drop(bytes);
        assert_eq!(limit.0.load(Ordering::SeqCst), 0);
        TrackedVec::with_capacity(1, Some(limit)).unwrap();
    }

    #[test]
    fn append_split_and_clone_reserve_shared_quota_before_mutation() {
        let limit = Arc::new(MemoryLimit::default());
        let mut full = TrackedVec::with_capacity(16 * 1024 * 1024, Some(limit.clone())).unwrap();
        full.extend_from_slice(b"keep").unwrap();
        let mut extra = TrackedVec::new(None);
        extra.extend_from_slice(b"x").unwrap();
        assert!(full.split_off(3).is_err());
        assert!(full.try_clone().is_err());
        full.resize(16 * 1024 * 1024, 0).unwrap();
        assert!(full.append(&mut extra).is_err());
        assert_eq!(full.len(), 16 * 1024 * 1024);
        assert_eq!(&full[..4], b"keep");
        assert_eq!(&*extra, b"x");
        drop(full);
        assert_eq!(limit.0.load(Ordering::SeqCst), 0);
        let mut small = TrackedVec::new(Some(limit.clone()));
        small.extend_from_slice(b"copy").unwrap();
        let copy = small.clone();
        assert_eq!(limit.0.load(Ordering::SeqCst), 8);
        drop(copy);
        drop(small);
        assert_eq!(limit.0.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn allocator_failure_releases_reserved_quota() {
        #[derive(Debug, Default)]
        struct Counting(std::sync::atomic::AtomicUsize);
        impl virtual_fs::limiter::FsMemoryLimiter for Counting {
            fn on_grow(&self, n: usize) -> Result<(), virtual_fs::FsError> {
                self.0.fetch_add(n, Ordering::SeqCst);
                Ok(())
            }
            fn on_shrink(&self, n: usize) {
                self.0.fetch_sub(n, Ordering::SeqCst);
            }
        }
        let limit = Arc::new(Counting::default());
        let mut bytes = TrackedVec::new(Some(limit.clone()));
        // Vec rejects capacities above isize::MAX before calling the allocator.
        assert!(bytes.reserve_exact(usize::MAX).is_err());
        assert_eq!(limit.0.load(Ordering::SeqCst), 0);
        bytes.extend_from_slice(b"ok").unwrap();
        drop(bytes);
        assert_eq!(limit.0.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn failed_temporary_file_growth_is_atomic_and_quota_is_reusable() {
        let fs = root(&Default::default(), &Default::default(), "");
        let mut a = fs
            .new_open_options()
            .read(true)
            .write(true)
            .create(true)
            .open("/tmp/a")
            .unwrap();
        a.write_all(b"keep").await.unwrap();
        for _ in 0..2 {
            assert!(a.set_len(17 * 1024 * 1024).is_err());
            assert_eq!(a.size(), 4);
        }
        a.seek(std::io::SeekFrom::Start(16 * 1024 * 1024 - 1))
            .await
            .unwrap();
        assert!(a.write_all(b"xx").await.is_err());
        a.rewind().await.unwrap();
        let mut data = Vec::new();
        a.read_to_end(&mut data).await.unwrap();
        assert_eq!(data, b"keep");
        drop(a);
        fs.remove_file(std::path::Path::new("/tmp/a")).unwrap();
        let mut b = fs
            .new_open_options()
            .write(true)
            .create(true)
            .open("/tmp/b")
            .unwrap();
        b.set_len(16 * 1024 * 1024).unwrap();
        let mut c = fs
            .new_open_options()
            .write(true)
            .create(true)
            .open("/tmp/c")
            .unwrap();
        assert!(c.write_all(b"x").await.is_err());
        drop(b);
        fs.remove_file(std::path::Path::new("/tmp/b")).unwrap();
        c.write_all(b"x").await.unwrap();
    }
}
