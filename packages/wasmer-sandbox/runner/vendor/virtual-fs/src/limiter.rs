use std::sync::Arc;

use crate::FsError;

pub use self::tracked_vec::TrackedVec;

/// Allows tracking and limiting the memory usage of a memfs [`FileSystem`](crate::FileSystem).
pub trait FsMemoryLimiter: Send + Sync + std::fmt::Debug {
    fn on_grow(&self, grown_bytes: usize) -> std::result::Result<(), FsError>;
    fn on_shrink(&self, shrunk_bytes: usize);
}

pub type DynFsMemoryLimiter = Arc<dyn FsMemoryLimiter + Send + Sync>;

#[cfg(feature = "tracking")]
mod tracked_vec {
    use crate::FsError;

    use super::DynFsMemoryLimiter;

    #[derive(Debug)]
    pub struct TrackedVec {
        data: Vec<u8>,
        pub(super) limiter: Option<DynFsMemoryLimiter>,
    }

    impl TrackedVec {
        pub fn new(limiter: Option<DynFsMemoryLimiter>) -> Self {
            Self {
                data: Vec::new(),
                limiter,
            }
        }

        pub fn limiter(&self) -> Option<&DynFsMemoryLimiter> {
            self.limiter.as_ref()
        }

        pub fn with_capacity(
            capacity: usize,
            limiter: Option<DynFsMemoryLimiter>,
        ) -> Result<Self, FsError> {
            let mut result = Self::new(limiter);
            result.reserve_exact(capacity)?;
            Ok(result)
        }

        pub fn clear(&mut self) {
            self.data.clear();
        }

        pub fn append(&mut self, other: &mut Self) -> Result<(), FsError> {
            self.reserve_exact(other.data.len())?;
            self.data.append(&mut other.data);
            Ok(())
        }

        pub fn split_off(&mut self, at: usize) -> Result<Self, FsError> {
            let count = self
                .data
                .len()
                .checked_sub(at)
                .ok_or(FsError::InvalidInput)?;
            let mut other = Self::with_capacity(count, self.limiter.clone())?;
            other.data.extend_from_slice(&self.data[at..]);
            self.data.truncate(at);
            Ok(other)
        }

        pub fn resize(&mut self, new_len: usize, value: u8) -> Result<(), FsError> {
            self.reserve_exact(new_len.saturating_sub(self.data.len()))?;
            self.data.resize(new_len, value);
            Ok(())
        }

        pub fn extend_from_slice(&mut self, other: &[u8]) -> Result<(), FsError> {
            self.reserve_exact(other.len())?;
            self.data.extend_from_slice(other);
            Ok(())
        }

        /// Reserve quota before any allocation or mutation. Exact reservation
        /// avoids Vec's geometric growth allocating unaccounted capacity.
        pub fn reserve_exact(&mut self, additional: usize) -> Result<(), FsError> {
            let required = self
                .data
                .len()
                .checked_add(additional)
                .ok_or(FsError::StorageFull)?;
            let delta = required.saturating_sub(self.data.capacity());
            if delta == 0 {
                return Ok(());
            }
            if let Some(limiter) = &self.limiter {
                limiter.on_grow(delta)?;
            }
            if self.data.try_reserve_exact(additional).is_err() {
                if let Some(limiter) = &self.limiter {
                    limiter.on_shrink(delta);
                }
                return Err(FsError::StorageFull);
            }
            Ok(())
        }

        pub fn try_clone(&self) -> Result<Self, FsError> {
            let mut copy = Self::with_capacity(self.data.len(), self.limiter.clone())?;
            copy.data.extend_from_slice(&self.data);
            Ok(copy)
        }
    }

    impl Clone for TrackedVec {
        fn clone(&self) -> Self {
            // Clone cannot return a quota error. Fail before allocation rather
            // than silently duplicating bytes outside the shared quota.
            self.try_clone()
                .expect("filesystem clone exceeds memory quota")
        }
    }

    impl Drop for TrackedVec {
        fn drop(&mut self) {
            if let Some(limiter) = &self.limiter {
                limiter.on_shrink(self.data.capacity());
            }
        }
    }

    impl std::ops::Deref for TrackedVec {
        type Target = [u8];

        fn deref(&self) -> &Self::Target {
            &self.data
        }
    }

    impl std::ops::DerefMut for TrackedVec {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.data
        }
    }
}

#[cfg(not(feature = "tracking"))]
mod tracked_vec {
    use crate::FsError;

    use super::DynFsMemoryLimiter;

    #[derive(Debug)]
    pub struct TrackedVec {
        data: Vec<u8>,
    }

    impl TrackedVec {
        pub fn new(_limiter: Option<DynFsMemoryLimiter>) -> Self {
            Self { data: Vec::new() }
        }

        pub fn limiter(&self) -> Option<&DynFsMemoryLimiter> {
            None
        }

        pub fn with_capacity(
            capacity: usize,
            _limiter: Option<DynFsMemoryLimiter>,
        ) -> Result<Self, FsError> {
            Ok(Self {
                data: Vec::with_capacity(capacity),
            })
        }

        pub fn clear(&mut self) {
            self.data.clear();
        }

        pub fn append(&mut self, other: &mut Self) -> Result<(), FsError> {
            self.data.append(&mut other.data);
            Ok(())
        }

        pub fn split_off(&mut self, at: usize) -> Result<Self, FsError> {
            let other = self.data.split_off(at);
            Ok(Self { data: other })
        }

        pub fn resize(&mut self, new_len: usize, value: u8) -> Result<(), FsError> {
            self.data.resize(new_len, value);
            Ok(())
        }

        pub fn extend_from_slice(&mut self, other: &[u8]) -> Result<(), FsError> {
            self.data.extend_from_slice(other);
            Ok(())
        }

        pub fn reserve_exact(&mut self, additional: usize) -> Result<(), FsError> {
            self.data.reserve_exact(additional);
            Ok(())
        }
    }

    impl std::ops::Deref for TrackedVec {
        type Target = Vec<u8>;

        fn deref(&self) -> &Self::Target {
            &self.data
        }
    }

    impl std::ops::DerefMut for TrackedVec {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.data
        }
    }
}
