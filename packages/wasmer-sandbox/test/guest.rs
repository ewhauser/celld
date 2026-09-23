use std::{
    fs,
    io::{Read, Seek, SeekFrom, Write},
};
fn main() {
    let mode = std::env::args().nth(1).unwrap_or("basic".into());
    match mode.as_str() {
        "agent-check" | "agent-hold" => {
            let secret = std::env::var("AGENT_SECRET").unwrap();
            assert_eq!(fs::read_to_string("/workspace/agent-private").unwrap(), secret);
            assert_eq!(std::env::var("HOME").unwrap(), "/tmp/home");
            assert_eq!(std::env::var("TMPDIR").unwrap(), "/tmp");
            assert!(std::env::var("CELLD_SANDBOX_TOKEN").is_err());
            assert!(std::env::var("PRIVATE_PARENT_SECRET").is_err());
            for path in ["/tmp/agent-private", "/tmp/home/.cache/agent-private"] {
                assert!(fs::read(path).is_err(), "private state survived another execution");
                fs::write(path, &secret).unwrap();
            }
            fs::write("/workspace/agent-ready", b"ready").unwrap();
            if mode == "agent-hold" {
                let until = std::time::Instant::now() + std::time::Duration::from_secs(6);
                while std::time::Instant::now() < until {
                    assert_eq!(fs::read_to_string("/tmp/agent-private").unwrap(), secret);
                    assert_eq!(fs::read_to_string("/tmp/home/.cache/agent-private").unwrap(), secret);
                    std::thread::sleep(std::time::Duration::from_millis(25));
                }
            }
            println!("{secret}");
            eprintln!("private-stderr-{secret}");
        }
        "stat-bench" => {
            let count: u32 = std::env::args().nth(2).unwrap_or("1000".into()).parse().unwrap();
            let started = std::time::Instant::now();
            for _ in 0..count { assert_eq!(fs::metadata("/workspace/input.txt").unwrap().len(), 15); }
            println!("{}", started.elapsed().as_micros());
        }
        "wait" => {
            fs::write("/workspace/partial", b"committed").unwrap();
            loop {
                std::hint::spin_loop();
            }
        }
        "exit" => std::process::exit(42),
        "output" => loop {
            let _ = std::io::stdout().write_all(&[b'x'; 4096]);
        },
        "trap" => panic!("intentional trap"),
        "quota" => {
            let mut f = fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open("/tmp/quota")
                .unwrap();
            f.write_all(b"keep").unwrap();
            for _ in 0..2 {
                assert!(f.set_len(17 * 1024 * 1024).is_err());
                assert_eq!(f.metadata().unwrap().len(), 4);
            }
            // A huge sparse growth must reject without trying to allocate it.
            assert!(f.set_len(1u64 << 40).is_err());
            f.seek(SeekFrom::Start(16 * 1024 * 1024 - 1)).unwrap();
            assert!(f.write_all(b"xx").is_err());
            f.rewind().unwrap();
            let mut bytes = Vec::new();
            f.read_to_end(&mut bytes).unwrap();
            assert_eq!(bytes, b"keep");
            drop(f);
            fs::remove_file("/tmp/quota").unwrap();
            fs::write("/tmp/small", b"x").unwrap();
            fs::remove_file("/tmp/small").unwrap();
            let f = fs::File::create("/tmp/full").unwrap();
            f.set_len(16 * 1024 * 1024).unwrap();
            assert!(fs::write("/tmp/extra", b"x").is_err());
            drop(f);
            fs::remove_file("/tmp/full").unwrap();
            fs::write("/tmp/extra", b"x").unwrap();
            println!("quota-preserved");
        }
        "isolation" => {
            assert_eq!(core::arch::wasm32::memory_grow::<0>(9000), usize::MAX);
            assert!(fs::read("/etc/passwd").is_err());
            assert!(fs::read("/workspace/../../etc/passwd").is_err());
            assert!(std::env::var("CELLD_SANDBOX_TOKEN").is_err());
            assert!(std::net::TcpStream::connect("127.0.0.1:19876").is_err());
            assert!(fs::write("/bin/forbidden", b"x").is_err());
            fs::write("/tmp/ephemeral", b"temporary").unwrap();
            assert!(fs::rename("/tmp/ephemeral", "/workspace/cross-mount").is_err());
            assert!(fs::write("/tmp/too-large", vec![0; 17 * 1024 * 1024]).is_err());
            println!("isolated");
        }
        _ => {
            assert_eq!(
                fs::read("/workspace/input.txt").unwrap(),
                b"from TypeScript"
            );
            assert_eq!(std::env::var("TEST_VALUE").unwrap(), "visible");
            let mut stdin = String::new();
            std::io::stdin().read_to_string(&mut stdin).unwrap();
            assert_eq!(stdin, "stdin-data");
            let mut file = fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open("/workspace/output.txt")
                .unwrap();
            file.write_all(b"hello").unwrap();
            file.set_len(9000).unwrap();
            file.seek(SeekFrom::Start(8193)).unwrap();
            file.write_all(b"z").unwrap();
            file.seek(SeekFrom::Start(0)).unwrap();
            let mut data = vec![];
            file.read_to_end(&mut data).unwrap();
            assert_eq!(data.len(), 9000);
            assert_eq!(&data[..5], b"hello");
            assert_eq!(data[8193], b'z');
            assert!(data[5..8193].iter().all(|v| *v == 0));
            file.set_len(2).unwrap();
            file.set_len(12).unwrap();
            file.sync_all().unwrap();
            drop(file);
            let mut append = fs::OpenOptions::new()
                .append(true)
                .open("/workspace/output.txt")
                .unwrap();
            append.write_all(b"tail").unwrap();
            drop(append);
            fs::rename("/workspace/output.txt", "/workspace/renamed.txt").unwrap();
            fs::write("/workspace/delete", b"delete").unwrap();
            fs::remove_file("/workspace/delete").unwrap();
            println!("guest-ok");
            eprintln!("guest-stderr");
        }
    }
}
