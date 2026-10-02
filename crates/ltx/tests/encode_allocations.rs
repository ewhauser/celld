//! Encoding an LTX file allocates about one output buffer, not compression
//! state per page or per file.
//!
//! The capture path encodes one small file per commit, and before compression
//! state was reused it allocated and zeroed about 136 KiB per file plus
//! 160 KiB per legacy page. This binary installs a counting allocator, so it
//! holds only these tests.

use celld_ltx::ltx::{self, Header, HEADER_FLAG_NO_CHECKSUM};
use celld_ltx::TXID;
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

struct Counting;

thread_local! {
    static ALLOCATED: Cell<usize> = const { Cell::new(0) };
}

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let _ = ALLOCATED.try_with(|total| total.set(total.get() + layout.size()));
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let _ = ALLOCATED.try_with(|total| total.set(total.get() + new_size));
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

/// Bytes allocated on this thread while `f` runs.
fn allocated(f: impl FnOnce()) -> usize {
    let before = ALLOCATED.with(Cell::get);
    f();
    ALLOCATED.with(Cell::get) - before
}

fn pages(count: u32) -> Vec<(u32, Vec<u8>)> {
    (1..=count)
        .map(|pgno| {
            let mut data = vec![0u8; 4096];
            for (index, byte) in data.iter_mut().enumerate().skip(1024) {
                *byte = (index as u32 * 31 + pgno) as u8 % 24 + b'a';
            }
            (pgno, data)
        })
        .collect()
}

fn header(commit: u32) -> Header {
    Header {
        version: ltx::VERSION,
        flags: HEADER_FLAG_NO_CHECKSUM,
        page_size: 4096,
        commit,
        min_txid: TXID(2),
        max_txid: TXID(2),
        timestamp: 1_790_000_000_000,
        ..Header::default()
    }
}

#[test]
fn legacy_encode_allocates_about_one_output_buffer() {
    for count in [1, 4, 64] {
        let (header, pages) = (header(count), pages(count));
        // The first file on a thread builds that thread's frame encoder.
        ltx::encode_file(&header, &pages, 0).expect("warm up");

        let mut encoded = Vec::new();
        let bytes = allocated(|| encoded = ltx::encode_file(&header, &pages, 0).expect("encode"));
        // The output is sized for incompressible pages, about one page each;
        // the page index and its map add a little per page.
        let output = count as usize * 4200 + 256;
        let slack = count as usize * 128 + 1024;
        assert!(
            bytes <= output + slack,
            "{count} pages: {bytes} bytes allocated, {} encoded",
            encoded.len()
        );
    }
}
