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
