# virtual-fs memory-accounting patch

`virtual-fs/` contains the registry source and normalized Cargo.toml for
`virtual-fs` 0.704.0. Original crate archive SHA-256:
`e9dac889f2eeb604513014328d0f7c524473c6668dca67e4a432803a897e8cfe`. The MIT license is included from Wasmer.
Only `src/limiter.rs` and `src/mem_fs/file.rs` differ from that registry source.
Cargo's `[patch.crates-io]` applies this copy to the runner and all transitive
Wasmer dependencies; Docker copies it into its build context as well.

The upstream tracking implementation allocates/mutates before asking the memory
limiter, and derives an unaccounted deep Clone. A rejected growth can therefore
leave extra bytes allocated and later underflow the limiter when the file drops.
This patch reserves shared quota first, reserves exact Vec capacity with a
fallible allocation, releases the reservation if allocation fails, and only then
mutates contents. Append and split preserve both inputs on rejection; cloning
also charges the shared quota. The infallible Clone trait fails before allocation
if quota is exhausted; callers that need a recoverable error can use try_clone.

File writes reserve their complete growth before overwriting an existing prefix
or materializing a sparse gap. This keeps failed growth atomic for both content
and size. Freed capacity is returned when its buffer drops; truncating a buffer
does not release capacity retained by Vec.

Runner tests in `src/packages.rs` cover repeated rejection, unchanged bytes,
allocation-failure rollback, append/split/clone accounting, aggregate file quota,
and deletion/reuse. The real WASI guest tests repeated and 1 TiB truncate attempts,
sparse writes, aggregate quota, and reuse. Keep these tests when replacing the
patch with an upstream release. Do not delete this patch just because one failed
write returns an error: the old implementation already did that.
