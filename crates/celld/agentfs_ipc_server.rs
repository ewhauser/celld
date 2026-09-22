// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//! Local Unix-socket adapter for the native AgentFS filesystem.
use crate::*;
use celld_agentfs_ipc::{encode_response, Error as FsError, Request as FsRequest, MAX_FRAME};
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

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
        let answer =
            tokio::time::timeout(std::time::Duration::from_secs(5), dispatch(&app, request))
                .await?;
        let bytes = encode_response(answer);
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            stream.write_u32_le(bytes.len() as u32).await?;
            stream.write_all(&bytes).await
        })
        .await??;
    }
    Ok(())
}
async fn dispatch(
    app: &AppHandle,
    request: FsRequest,
) -> Result<celld_agentfs_ipc::Reply, FsError> {
    if app.draining.load(Ordering::Acquire) {
        return Err(FsError::Stale);
    }
    let runtime = app.runtime.as_ref().ok_or(FsError::Stale)?;
    // IPC never wakes a dormant object or follows ownership to another node.
    let epoch = runtime
        .published_epoch(&request.scope)
        .ok_or(FsError::Stale)?;
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
            runtime.agentfs_operation(&request).await
        })
        .await;
    // Gate failure releases no filesystem data (including state-dependent errors).
    completed.result.map_err(|_| FsError::Stale)?.result
}
