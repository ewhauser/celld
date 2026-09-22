use std::{
    fs::{self, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    time::Instant,
};
fn main() {
    let mode = std::env::args().nth(1).unwrap_or("basic".into());
    let t = Instant::now();
    let input = fs::read("/workspace/input.txt").unwrap();
    assert_eq!(input, b"hello from TypeScript\n");
    fs::write("/workspace/output.txt", b"hello from Wasmer\n").unwrap();
    let mut f = OpenOptions::new()
        .read(true)
        .write(true)
        .open("/workspace/output.txt")
        .unwrap();
    f.seek(SeekFrom::Start(0)).unwrap();
    f.write_all(b"HELLO").unwrap();
    f.sync_all().unwrap();
    let mut a = OpenOptions::new()
        .append(true)
        .open("/workspace/output.txt")
        .unwrap();
    a.write_all(b"append\n").unwrap();
    drop(a);
    if mode == "cancel" || mode == "owner" {
        fs::write(
            if mode == "owner" {
                "/workspace/owner-partial.txt"
            } else {
                "/workspace/partial.txt"
            },
            b"committed prefix",
        )
        .unwrap();
        loop {
            std::hint::spin_loop();
        }
    }
    if mode == "edge" {
        f.set_len(32).unwrap();
        f.seek(SeekFrom::Start(0)).unwrap();
        let mut b = Vec::new();
        f.read_to_end(&mut b).unwrap();
        assert_eq!(b.len(), 32, "truncate growth must read as zeros");
        assert_eq!(&b[25..], &[0; 7]);
    }
    let mt = Instant::now();
    for _ in 0..100 {
        assert!(fs::metadata("/workspace/input.txt").unwrap().is_file());
    }
    println!("metadata_100_us={}", mt.elapsed().as_micros());
    let io = Instant::now();
    let b = fs::read("/workspace/large.bin").unwrap();
    assert_eq!(b.len(), 256 * 1024);
    fs::write("/workspace/large-out.bin", &b).unwrap();
    println!("io_256k_read_write_us={}", io.elapsed().as_micros());
    fs::write("/workspace/rename-from.txt", b"rename-data").unwrap();
    fs::rename("/workspace/rename-from.txt", "/workspace/rename-to.txt").unwrap();
    assert_eq!(
        fs::read("/workspace/rename-to.txt").unwrap(),
        b"rename-data"
    );
    fs::remove_file("/workspace/rename-to.txt").unwrap();
    println!("guest_total_us={}", t.elapsed().as_micros());
}
