use std::{
    io,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncSeek, AsyncWrite, ReadBuf};
use virtual_fs::{FsError, VirtualFile};

#[derive(Debug, Clone, Default)]
pub struct Output {
    pub bytes: Arc<Mutex<Vec<u8>>>,
    pub overflow: Arc<std::sync::atomic::AtomicBool>,
}
impl AsyncRead for Output {
    fn poll_read(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        _: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Poll::Ready(Err(io::ErrorKind::PermissionDenied.into()))
    }
}
impl AsyncWrite for Output {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let mut bytes = self.bytes.lock().unwrap();
        if bytes.len() + buf.len() > 65536 {
            self.overflow
                .store(true, std::sync::atomic::Ordering::SeqCst);
            return Poll::Ready(Err(io::ErrorKind::OutOfMemory.into()));
        }
        bytes.extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}
impl AsyncSeek for Output {
    fn start_seek(self: Pin<&mut Self>, _: io::SeekFrom) -> io::Result<()> {
        Err(io::ErrorKind::Unsupported.into())
    }
    fn poll_complete(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<u64>> {
        Poll::Ready(Err(io::ErrorKind::Unsupported.into()))
    }
}
impl VirtualFile for Output {
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
        self.bytes.lock().unwrap().len() as u64
    }
    fn set_len(&mut self, _: u64) -> virtual_fs::Result<()> {
        Err(FsError::PermissionDenied)
    }
    fn unlink(&mut self) -> virtual_fs::Result<()> {
        Err(FsError::PermissionDenied)
    }
    fn poll_read_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<usize>> {
        Poll::Ready(Ok(0))
    }
    fn poll_write_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<usize>> {
        Poll::Ready(Ok(65536 - self.bytes.lock().unwrap().len()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;
    #[tokio::test]
    async fn bounded_output_remembers_overflow() {
        let mut out = Output::default();
        out.write_all(&vec![0; 65536]).await.unwrap();
        assert!(out.write_all(b"x").await.is_err());
        assert_eq!(out.size(), 65536);
        assert!(out.overflow.load(std::sync::atomic::Ordering::SeqCst));
    }
}
