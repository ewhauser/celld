//! The LTX encoders write the same bytes they wrote before they started
//! reusing compression state across pages and files.
//!
//! `reference_encode` is a copy of the encoder as it was before that change:
//! a fresh `lz4_flex` frame encoder for every legacy page and a fresh block
//! compressor for every file. Each case compares the library's output with it
//! byte for byte, decodes the output, and checks a fingerprint captured from
//! the encoder before the change, so the reference copy cannot drift with it.

use celld_ltx::internal::lz4_block::Compressor;
use celld_ltx::ltx::{
    self, checksum_page, lock_pgno, Crc64, Header, PageHeader, Trailer, HEADER_FLAG_NO_CHECKSUM,
    PAGE_HEADER_FLAG_SIZE, PAGE_HEADER_SIZE,
};
use celld_ltx::{CHECKSUM_FLAG, TXID};
use std::collections::BTreeMap;
use std::io::Write;

/// A deterministic xorshift generator.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn fill(&mut self, bytes: &mut [u8]) {
        for chunk in bytes.chunks_mut(8) {
            let word = self.next().to_le_bytes();
            chunk.copy_from_slice(&word[..chunk.len()]);
        }
    }
}

#[derive(Clone, Copy)]
enum Fill {
    Zero,
    Random,
    /// Text-like records with repeated structure, like a SQLite table leaf.
    Text,
    /// Alternates the other three by page number.
    Mixed,
}

fn page(page_size: u32, pgno: u32, fill: Fill, seed: u64) -> Vec<u8> {
    let mut data = vec![0u8; page_size as usize];
    let mut rng = Rng(u64::from(pgno).wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ seed ^ 0xDEAD_BEEF);
    match fill {
        Fill::Zero => {}
        Fill::Random => rng.fill(&mut data),
        Fill::Text => {
            const WORDS: [&[u8]; 6] = [
                b"user",
                b"session",
                b"2026-10-01",
                b"cell",
                b"value",
                b"null",
            ];
            let mut at = data.len() / 4;
            while at < data.len() {
                let word = WORDS[(rng.next() % 6) as usize];
                let end = (at + word.len()).min(data.len());
                data[at..end].copy_from_slice(&word[..end - at]);
                at = end;
                if at < data.len() {
                    data[at] = (rng.next() % 64) as u8;
                    at += 1;
                }
            }
        }
        Fill::Mixed => {
            let fill = [Fill::Zero, Fill::Random, Fill::Text][(pgno % 3) as usize];
            return page(page_size, pgno, fill, seed);
        }
    }
    data
}

struct Case {
    name: &'static str,
    page_size: u32,
    pgnos: Vec<u32>,
    fill: Fill,
    /// A snapshot with checksums (min txid 1), rather than a checksum-free L0.
    snapshot: bool,
}

impl Case {
    fn header(&self) -> Header {
        let commit = self.pgnos.last().copied().unwrap_or(1).max(1);
        let (flags, min_txid) = if self.snapshot {
            (0, 1)
        } else {
            (HEADER_FLAG_NO_CHECKSUM, 7)
        };
        Header {
            version: ltx::VERSION,
            flags,
            page_size: self.page_size,
            commit,
            min_txid: TXID(min_txid),
            max_txid: TXID(7),
            timestamp: 1_790_000_000_000,
            ..Header::default()
        }
    }

    fn pages(&self) -> Vec<(u32, Vec<u8>)> {
        self.pgnos
            .iter()
            .map(|&pgno| {
                (
                    pgno,
                    page(self.page_size, pgno, self.fill, self.page_size.into()),
                )
            })
            .collect()
    }

    fn post_apply_checksum(&self, pages: &[(u32, Vec<u8>)]) -> u64 {
        if !self.snapshot {
            return 0;
        }
        let lock = lock_pgno(self.page_size);
        CHECKSUM_FLAG
            | pages
                .iter()
                .filter(|(pgno, _)| *pgno != lock)
                .fold(0, |sum, (pgno, data)| sum ^ checksum_page(*pgno, data))
    }
}

fn cases() -> Vec<Case> {
    let case = |name, page_size, pgnos: Vec<u32>, fill, snapshot| Case {
        name,
        page_size,
        pgnos,
        fill,
        snapshot,
    };
    let scattered: Vec<u32> = (0..200).map(|index| 1 + index * 7).collect();
    vec![
        case("empty", 4096, vec![], Fill::Text, false),
        case("one_text", 4096, vec![1], Fill::Text, false),
        case("one_zero", 4096, vec![3], Fill::Zero, false),
        case("one_random", 4096, vec![5], Fill::Random, false),
        case("many_text", 4096, scattered.clone(), Fill::Text, false),
        case("many_mixed", 4096, scattered, Fill::Mixed, false),
        case(
            "snapshot_mixed",
            4096,
            (1..=64).collect(),
            Fill::Mixed,
            true,
        ),
        case("min_page_mixed", 512, (1..=40).collect(), Fill::Mixed, true),
        case("max_page_zero", 65536, vec![1, 2], Fill::Zero, false),
        case("max_page_random", 65536, vec![1, 2, 9], Fill::Random, false),
        case(
            "max_page_mixed",
            65536,
            (1..=6).collect(),
            Fill::Mixed,
            true,
        ),
    ]
}

/// An encoded file's length and the CRC64 of its bytes.
type Fingerprint = (usize, u64);

/// `(case, legacy fingerprint, v0.5.2 fingerprint)`, from the encoder before
/// the change.
const FINGERPRINTS: &[(&str, Fingerprint, Fingerprint)] = &[
    (
        "empty",
        (131, 13651605238157579335),
        (131, 13651605238157579335),
    ),
    (
        "one_text",
        (1859, 17622901126858041018),
        (2081, 833504766759222567),
    ),
    (
        "one_zero",
        (186, 7827602580932796332),
        (182, 7876042529003979767),
    ),
    (
        "one_random",
        (4256, 94966442598492614),
        (4259, 2605680869724047752),
    ),
    (
        "many_text",
        (355569, 16026787974332479391),
        (396824, 9109511347285344999),
    ),
    (
        "many_mixed",
        (399595, 11077995066150453293),
        (413432, 10382833451148907227),
    ),
    (
        "snapshot_mixed",
        (129545, 7627776621817849511),
        (134083, 11417036834944545244),
    ),
    (
        "min_page_mixed",
        (12287, 3876877894361066117),
        (12201, 10974775280656403863),
    ),
    (
        "max_page_zero",
        (726, 8359211740691776199),
        (718, 6255071388919995658),
    ),
    (
        "max_page_random",
        (196833, 13718215283244452910),
        (197562, 11751701587018249528),
    ),
    (
        "max_page_mixed",
        (177448, 12812654673708325935),
        (173371, 10457964847510850728),
    ),
];

/// The pre-change encoder: what `codec::Encoder` wrote for these inputs.
fn reference_encode(
    header: &Header,
    pages: &[(u32, Vec<u8>)],
    post_apply_checksum: u64,
    block: bool,
) -> Vec<u8> {
    let mut out = Vec::new();
    let mut hash = Crc64::new();
    let mut index = BTreeMap::new();
    let mut compressor = Compressor::default();

    let bytes = header.marshal();
    out.extend_from_slice(&bytes);
    hash.update(&bytes);
    for (pgno, data) in pages {
        let offset = out.len() as u64;
        let compressed = if block {
            let compressed = compressor.compress(data);
            let bytes = PageHeader {
                pgno: *pgno,
                flags: PAGE_HEADER_FLAG_SIZE,
            }
            .marshal();
            out.extend_from_slice(&bytes);
            hash.update(&bytes);
            let size = (compressed.len() as u32).to_be_bytes();
            out.extend_from_slice(&size);
            hash.update(&size);
            compressed
        } else {
            let bytes = PageHeader {
                pgno: *pgno,
                flags: 0,
            }
            .marshal();
            out.extend_from_slice(&bytes);
            hash.update(&bytes);
            let frame_info = lz4_flex::frame::FrameInfo::new()
                .block_size(lz4_flex::frame::BlockSize::Max64KB)
                .block_mode(lz4_flex::frame::BlockMode::Independent)
                .content_checksum(true);
            let mut encoder =
                lz4_flex::frame::FrameEncoder::with_frame_info(frame_info, Vec::new());
            encoder.write_all(data).unwrap();
            encoder.finish().unwrap()
        };
        out.extend_from_slice(&compressed);
        hash.update(data);
        index.insert(*pgno, (offset, out.len() as u64 - offset));
    }

    let zero = [0; PAGE_HEADER_SIZE];
    out.extend_from_slice(&zero);
    hash.update(&zero);
    let mut index_bytes = Vec::new();
    for (&pgno, &(offset, size)) in &index {
        for value in [u64::from(pgno), offset, size] {
            write_uvarint(&mut index_bytes, value);
        }
    }
    write_uvarint(&mut index_bytes, 0);
    out.extend_from_slice(&index_bytes);
    hash.update(&index_bytes);
    let index_size = (index_bytes.len() as u64).to_be_bytes();
    out.extend_from_slice(&index_size);
    hash.update(&index_size);
    hash.update(&post_apply_checksum.to_be_bytes());
    let trailer = Trailer {
        post_apply_checksum,
        file_checksum: CHECKSUM_FLAG | hash.sum64(),
    };
    out.extend_from_slice(&trailer.marshal());
    out
}

fn write_uvarint(bytes: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        bytes.push(value as u8 | 0x80);
        value >>= 7;
    }
    bytes.push(value as u8);
}

fn fingerprint(bytes: &[u8]) -> Fingerprint {
    let mut hash = Crc64::new();
    hash.update(bytes);
    (bytes.len(), hash.sum64())
}

/// Encodes one case both ways, checks each against the reference and a
/// decode, and returns the two fingerprints.
fn encode_case(case: &Case) -> (Fingerprint, Fingerprint) {
    let header = case.header();
    let pages = case.pages();
    let post_apply = case.post_apply_checksum(&pages);

    let legacy = ltx::encode_file(&header, &pages, post_apply).expect("encode legacy");
    let block = ltx::encode_file_v0_5_2(&header, &pages, post_apply).expect("encode block");
    assert!(
        legacy == reference_encode(&header, &pages, post_apply, false),
        "{}: legacy bytes differ from the reference encoder",
        case.name
    );
    assert!(
        block == reference_encode(&header, &pages, post_apply, true),
        "{}: v0.5.2 bytes differ from the reference encoder",
        case.name
    );
    for bytes in [&legacy, &block] {
        assert_eq!(
            ltx::decode_file_pages(bytes).expect("decode"),
            pages,
            "{}",
            case.name
        );
    }
    (fingerprint(&legacy), fingerprint(&block))
}

#[test]
fn encoders_match_the_reference_and_pinned_bytes() {
    let cases = cases();
    let got: Vec<_> = cases.iter().map(encode_case).collect();
    let listing: Vec<_> = cases
        .iter()
        .zip(&got)
        .map(|(case, (legacy, block))| format!("    (\"{}\", {legacy:?}, {block:?}),", case.name))
        .collect();
    assert_eq!(
        FINGERPRINTS.len(),
        cases.len(),
        "pin one fingerprint per case; the current values are:\n{}",
        listing.join("\n")
    );
    for ((case, (legacy, block)), (name, want_legacy, want_block)) in
        cases.iter().zip(&got).zip(FINGERPRINTS)
    {
        assert_eq!(case.name, *name);
        assert_eq!(legacy, want_legacy, "{}: legacy fingerprint", case.name);
        assert_eq!(block, want_block, "{}: v0.5.2 fingerprint", case.name);
    }
}

/// The encoders keep compression state per thread and reuse it. Interleaving
/// page sizes and encodings on one thread, and encoding on several threads at
/// once, writes the same bytes as a fresh encoder each time.
#[test]
fn reused_state_writes_the_same_bytes() {
    let cases = cases();
    let expected: Vec<_> = cases.iter().map(encode_case).collect();
    for round in 0..3 {
        for (index, case) in cases.iter().enumerate().rev() {
            assert_eq!(
                encode_case(case),
                expected[index],
                "{} round {round}",
                case.name
            );
        }
    }
    std::thread::scope(|scope| {
        for offset in 0..4 {
            let (cases, expected) = (&cases, &expected);
            scope.spawn(move || {
                for step in 0..cases.len() * 2 {
                    let index = (step + offset) % cases.len();
                    assert_eq!(encode_case(&cases[index]), expected[index]);
                }
            });
        }
    });
}
