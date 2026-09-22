// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//! Local Unix-socket adapter for the native AgentFS filesystem.
use crate::*;
use celld_agentfs_ipc::{encode_response, Error as FsError, Request as FsRequest, MAX_FRAME};
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

static NEXT_SESSION: AtomicU64 = AtomicU64::new(1);
struct BoundConnection {
    scope: String,
    token: String,
    command: String,
    epoch: u64,
    session: u64,
}

pub(super) fn start(app: AppHandle) -> anyhow::Result<()> {
    let Some(path) = std::env::var_os("CELLD_AGENTFS_SOCKET") else {
        return Ok(());
    };
    let path = std::path::PathBuf::from(path);
    anyhow::ensure!(path.is_absolute(), "AgentFS socket path must be absolute");
    let parent = path
        .parent()
        .context("AgentFS socket needs a private parent directory")?;
    let metadata = std::fs::symlink_metadata(parent)?;
    anyhow::ensure!(
        metadata.is_dir()
            && metadata.mode() & 0o777 == 0o700
            && metadata.uid() == unsafe { libc::geteuid() },
        "AgentFS socket parent must be a directory owned by this user with mode 0700"
    );
    // Never unlink an existing socket: it may belong to a live owner. Remove
    // stale sockets explicitly after verifying that their process has stopped.
    let listener = tokio::net::UnixListener::bind(&path)?;
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
    let permits = Arc::new(tokio::sync::Semaphore::new(32));
    celld::asyncrt::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            if app.draining.load(Ordering::Acquire) {
                break;
            }
            let Ok(permit) = permits.clone().try_acquire_owned() else {
                drop(stream);
                continue;
            };
            let app = app.clone();
            celld::asyncrt::spawn(async move {
                let _permit = permit;
                let _ = connection(stream, app).await;
            })
            .detach();
        }
    })
    .detach();
    Ok(())
}
async fn connection(mut stream: tokio::net::UnixStream, app: AppHandle) -> anyhow::Result<()> {
    let session = NEXT_SESSION.fetch_add(1, AtomicOrdering::Relaxed);
    let mut bound: Option<BoundConnection> = None;
    let result = connection_requests(&mut stream, &app, session, &mut bound).await;
    if let (Some(binding), Some(runtime)) = (bound, app.runtime.as_ref()) {
        runtime
            .agentfs_revoke_session(&binding.scope, binding.session, binding.epoch)
            .await;
    }
    result
}
async fn connection_requests(
    stream: &mut tokio::net::UnixStream,
    app: &AppHandle,
    session: u64,
    bound: &mut Option<BoundConnection>,
) -> anyhow::Result<()> {
    for _ in 0..celld_agentfs_ipc::MAX_REQUESTS {
        // Bound idle leases separately from partial frames and active requests.
        let length = tokio::time::timeout(std::time::Duration::from_secs(310), stream.read_u32_le())
            .await?? as usize;
        anyhow::ensure!(length > 0 && length <= MAX_FRAME, "invalid IPC frame size");
        let mut body = vec![0; length];
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            stream.read_exact(&mut body),
        )
        .await??;
        let request = FsRequest::decode(&body)?;
        anyhow::ensure!(
            celld_logic::cell::valid_cell_scope(&request.scope),
            "invalid IPC scope"
        );
        let epoch = if let Some(binding) = bound.as_ref() {
            // Metadata after the first frame carries no authority. Reject any
            // attempt to change the already bound scope or capability.
            anyhow::ensure!(
                request.scope == binding.scope
                    && request.token == binding.token
                    && request.command == binding.command,
                "IPC connection attempted to switch grant"
            );
            binding.epoch
        } else {
            let epoch = app
                .runtime
                .as_ref()
                .and_then(|runtime| runtime.published_epoch(&request.scope))
                .ok_or_else(|| anyhow::anyhow!("IPC cell is not resident"))?;
            *bound = Some(BoundConnection {
                scope: request.scope.clone(),
                token: request.token.clone(),
                command: request.command.clone(),
                epoch,
                session,
            });
            epoch
        };
        let answer = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            dispatch(app, request, session, epoch),
        )
        .await?;
        let stale = matches!(answer, Err(FsError::Stale));
        let bytes = encode_response(answer);
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            stream.write_u32_le(bytes.len() as u32).await?;
            stream.write_all(&bytes).await
        })
        .await??;
        if stale {
            break;
        }
    }
    Ok(())
}
async fn dispatch(
    app: &AppHandle,
    request: FsRequest,
    session: u64,
    epoch: u64,
) -> Result<celld_agentfs_ipc::Reply, FsError> {
    if app.draining.load(Ordering::Acquire) {
        return Err(FsError::Stale);
    }
    let runtime = app.runtime.as_ref().ok_or(FsError::Stale)?;
    // IPC never wakes a dormant object or follows ownership to another node.
    if runtime.published_epoch(&request.scope) != Some(epoch) {
        return Err(FsError::Stale);
    }
    let Routed {
        request: activity,
        route,
    } = app
        .request(request.scope.clone())
        .await
        .map_err(|_| FsError::Stale)?;
    if !matches!(route, Route::Local) {
        return Err(FsError::Stale);
    }
    let completed = app
        .local_request(activity, request.scope.clone(), None, "agentfs-ipc")
        .run(async {
            anyhow::ensure!(
                runtime.published_epoch(&request.scope) == Some(epoch),
                "IPC owner changed"
            );
            runtime.agentfs_operation(&request, session, epoch).await
        })
        .await;
    // Gate failure releases no filesystem data (including state-dependent errors).
    completed.result.map_err(|_| FsError::Stale)?.result
}
