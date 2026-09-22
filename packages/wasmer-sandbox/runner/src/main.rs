mod memory;
mod output;
mod packages;
mod policy;
mod remote;
use serde::Deserialize;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    io::Read,
    sync::{Arc, atomic::Ordering},
    time::Duration,
};
use virtual_fs::FileSystem;
use wasmer_wasix::runners::wasi::{PackageOrHash, RuntimeOrEngine, WasiRunner};

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Config {
    module: String,
    sha256: String,
    callback: String,
    token: String,
    callback_token: String,
    native_filesystem: Option<remote::NativeFilesystem>,
    tool: String,
    args: Vec<String>,
    env: BTreeMap<String, String>,
    cwd: String,
    stdin: String,
    timeout_ms: u64,
    development: bool,
    entrypoint: Option<String>,
    #[serde(default)]
    packages: Vec<packages::Package>,
}

fn limits(config: &Config) -> anyhow::Result<()> {
    #[cfg(target_os = "linux")]
    unsafe {
        // Defense in depth under a service-level cgroup/container memory and
        // PID budget. Wasmer reserves a large virtual address range for Wasm.
        for (resource, value) in [
            (libc::RLIMIT_AS, 128u64 * 1024 * 1024 * 1024),
            (libc::RLIMIT_CPU, config.timeout_ms / 1000 + 2),
            (libc::RLIMIT_NOFILE, 256),
            (libc::RLIMIT_CORE, 0),
        ] {
            let limit = libc::rlimit {
                rlim_cur: value,
                rlim_max: value,
            };
            anyhow::ensure!(libc::setrlimit(resource, &limit) == 0, "setrlimit failed");
        }
        anyhow::ensure!(
            libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) == 0,
            "no_new_privs failed"
        );
    }
    #[cfg(not(target_os = "linux"))]
    anyhow::ensure!(config.development, "Linux is required outside development");
    Ok(())
}

fn run() -> anyhow::Result<()> {
    let config: Config = serde_json::from_reader(std::io::stdin().take(256 * 1024))?;
    anyhow::ensure!(
        (100..=120000).contains(&config.timeout_ms),
        "invalid deadline"
    );
    limits(&config)?;
    let bytes = std::fs::read(&config.module)?;
    anyhow::ensure!(bytes.len() <= 256 * 1024 * 1024, "module too large");
    anyhow::ensure!(
        format!("{:x}", Sha256::digest(&bytes)) == config.sha256,
        "module digest mismatch"
    );
    let mut remote = remote::Remote::new(config.callback, config.token, config.callback_token);
    remote.set_native_filesystem(config.native_filesystem);
    remote
        .call(json!({"op":"heartbeat"}))
        .map_err(|e| anyhow::anyhow!(e))?;
    let heartbeat = remote.clone();
    std::thread::spawn(move || {
        loop {
            std::thread::sleep(Duration::from_secs(1));
            if heartbeat.call(json!({"op":"heartbeat"})).is_err() {
                std::process::exit(75);
            }
        }
    });
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    let _guard = runtime.enter();
    let store = memory::store();
    let out = output::Output {
        overflow: remote.poisoned.clone(),
        ..Default::default()
    };
    let err = output::Output {
        overflow: remote.poisoned.clone(),
        ..Default::default()
    };
    let tasks = Arc::new(
        wasmer_wasix::runtime::task_manager::tokio::TokioTaskManager::new(runtime.handle().clone()),
    );
    let mut rt = wasmer_wasix::runtime::PluggableRuntime::new(tasks);
    rt.set_networking_implementation(virtual_net::UnsupportedVirtualNetworking::default());
    rt.engine = store.engine().clone();
    let mut caps = wasmer_wasix::capabilities::Capabilities::default();
    caps.threading.max_threads = Some(8);
    let (source, loader) = packages::offline(if bytes.starts_with(b"\0asm") {
        &[]
    } else {
        &config.packages
    })?;
    rt.set_source(source);
    rt.set_package_loader(loader);
    let mut rt = Arc::new(rt);
    remote.mounted = true;
    let mut runner = WasiRunner::new();
    runner
        .with_args(config.args)
        .with_envs(config.env)
        .with_current_dir(config.cwd.clone())
        .with_capabilities(caps)
        .with_forward_host_env(false)
        .with_mount(
            "/workspace".into(),
            Arc::new(remote.clone()) as Arc<dyn FileSystem + Send + Sync>,
        )
        .with_stdin(Box::new(virtual_fs::StaticFile::new(
            shared_buffer::OwnedBuffer::from(config.stdin.as_bytes().to_vec()),
        )))
        .with_stdout(Box::new(out.clone()))
        .with_stderr(Box::new(err.clone()));
    let root = Some(packages::root(&out, &err, &config.stdin));
    let result = if bytes.starts_with(b"\0asm") {
        let module = wasmer::Module::new(&store, &bytes)?;
        let annotation = webc::metadata::annotations::Wasi::new(&config.tool);
        let hash = wasmer_types::ModuleHash::sha256(&bytes);
        let builder = runner.prepare_webc_env(
            &config.tool,
            &annotation,
            PackageOrHash::Hash(hash),
            RuntimeOrEngine::Runtime(rt.clone()),
            root,
        )?;
        let env = policy::build(builder)?;
        let mut task = wasmer_wasix::bin_factory::spawn_exec_module(
            module,
            env,
            &(rt.clone() as Arc<dyn wasmer_wasix::Runtime + Send + Sync>),
        )?;
        runtime.block_on(task.wait_finished())
    } else {
        let container = packages::parse(bytes)?;
        let mut pkg = runtime.block_on(wasmer_wasix::bin_factory::BinaryPackage::from_webc(
            &container,
            rt.as_ref(),
        ))?;
        // Resolve all dependencies before exposing the guest. No dynamic
        // package downloads or mount directives remain available at runtime.
        let rt_mut = Arc::get_mut(&mut rt)
            .ok_or_else(|| anyhow::anyhow!("runtime unexpectedly shared before guest admission"))?;
        rt_mut.set_source(wasmer_wasix::runtime::resolver::InMemorySource::new());
        rt_mut.set_package_loader(wasmer_wasix::runtime::package_loader::UnsupportedPackageLoader);
        if let Some(mounts) = pkg.package_mounts.take() {
            runner.with_mount("/".into(), Arc::new(mounts.to_mount_fs()?));
        }
        let entry = match config.entrypoint.as_deref() {
            Some(entry) => entry,
            None => pkg.infer_entrypoint()?,
        };
        let annotation = pkg
            .get_command(entry)
            .ok_or_else(|| anyhow::anyhow!("entrypoint missing"))?
            .metadata()
            .annotation("wasi")?
            .unwrap_or_else(|| webc::metadata::annotations::Wasi::new(entry));
        let mut builder = runner.prepare_webc_env(
            entry,
            &annotation,
            PackageOrHash::Hash(pkg.hash()),
            RuntimeOrEngine::Runtime(rt.clone()),
            root,
        )?;
        builder
            .add_webc(pkg.clone())
            .include_packages(pkg.package_ids.clone());
        // Administrator-selected cwd wins over a package manifest default.
        builder.set_current_dir(config.cwd);
        let env = policy::build(builder)?;
        let runtime_trait = rt.clone() as Arc<dyn wasmer_wasix::Runtime + Send + Sync>;
        runtime.block_on(async {
            let mut task =
                wasmer_wasix::bin_factory::spawn_exec(pkg.clone(), entry, env, &runtime_trait)
                    .await?;
            Ok::<_, anyhow::Error>(task.wait_finished().await)
        })?
    };
    let (mut reason, mut code) = match result {
        Ok(code) => ("exited", code.raw()),
        Err(_) => ("failed", 125),
    };
    // A failed scalar FS method or overflowing stdio must never turn into a
    // successful result merely because the guest ignored the host error.
    if remote.poisoned.load(Ordering::SeqCst)
        || out.overflow.load(Ordering::SeqCst)
        || err.overflow.load(Ordering::SeqCst)
    {
        reason = "failed";
        code = 125;
    }
    remote
        .call(json!({"op":"sync"}))
        .map_err(|e| anyhow::anyhow!(e))?;
    println!(
        "{}",
        json!({"fsCalls":remote.fs_counts(),"statCalls":remote.stat_counts(),"reason":reason,"exitCode":code,"stdout":String::from_utf8_lossy(&out.bytes.lock().unwrap()),"stderr":String::from_utf8_lossy(&err.bytes.lock().unwrap())})
    );
    Ok(())
}
fn main() {
    if let Err(e) = run() {
        eprintln!("runner failed: {e}");
        std::process::exit(125);
    }
}
