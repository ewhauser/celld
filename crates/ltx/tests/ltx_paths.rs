//! The LTX path and key builders must produce the same bytes as the Go-shaped
//! `path.Join` / `format!` compositions they replaced.

use celld_ltx::internal::path_join;
use celld_ltx::{ltx_dir, ltx_file_path, ltx_level_dir, LtxPaths, TXID};

/// `LTXLevelDir`: `path.Join(path.Join(root, "ltx"), strconv.Itoa(level))`.
fn reference_level_dir(root: &str, level: u32) -> String {
    path_join(&[&path_join(&[root, "ltx"]), &level.to_string()])
}

/// `LTXFilePath`: `path.Join(LTXLevelDir(root, level), FormatFilename(min, max))`.
fn reference_file_path(root: &str, level: u32, min: TXID, max: TXID) -> String {
    let filename = format!("{}-{}.ltx", min, max);
    path_join(&[&reference_level_dir(root, level), &filename])
}

const ROOTS: &[&str] = &[
    "",
    ".",
    "./",
    "/",
    "//",
    "..",
    "../",
    "../..",
    "a",
    "a/",
    "a//",
    "a/..",
    "a/../..",
    "/..",
    "/../a",
    "/a/b/../c/./d/",
    "./a/./b",
    "/tmp/.db-litestream",
    "/tmp/x/.app.db-litestream/",
    "rel/.app.db-litestream",
    "ltx",
    "a/ltx/..",
    "a/b/c/../../..",
    "a/b/c/../../../..",
    "...",
    ".hidden/..x",
    "dir with space/ü",
];

const LEVELS: &[u32] = &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 99, 100, 65535, u32::MAX];

const TXIDS: &[(u64, u64)] = &[
    (0, 0),
    (1, 1),
    (0, u64::MAX),
    (u64::MAX, u64::MAX),
    (0x0123_4567_89ab_cdef, 0xfedc_ba98_7654_3210),
    (0xf, 0x10),
];

#[test]
fn local_paths_match_path_join() {
    for &root in ROOTS {
        let paths = LtxPaths::new(root);
        let want_dir = path_join(&[root, "ltx"]);
        assert_eq!(paths.dir(), want_dir, "root {root:?}");
        assert_eq!(ltx_dir(root), want_dir, "root {root:?}");
        for &level in LEVELS {
            let want = reference_level_dir(root, level);
            assert_eq!(paths.level_dir(level), want, "root {root:?} level {level}");
            assert_eq!(
                ltx_level_dir(root, level),
                want,
                "root {root:?} level {level}"
            );
            for &(min, max) in TXIDS {
                let (min, max) = (TXID(min), TXID(max));
                let want = reference_file_path(root, level, min, max);
                assert_eq!(
                    paths.file_path(level, min, max),
                    want,
                    "root {root:?} level {level} txids {min}-{max}"
                );
                assert_eq!(ltx_file_path(root, level, min, max), want);
            }
        }
    }
}

#[cfg(feature = "s3")]
#[test]
fn bucket_keys_match_format() {
    use celld_ltx::internal::object_store::{level_prefix, ltx_key};
    use celld_ltx::ltx::format_filename;
    use celld_ltx::{ObjectStoreClient, ObjectStoreConfig};

    let paths = [
        "",
        "/",
        "db",
        "a/b/",
        "tenant/7/epoch-3",
        "..",
        "dir with space/ü",
    ];
    let levels = [
        0,
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9,
        10,
        0xffff,
        0x1_0000,
        i32::MAX,
        -1,
        i32::MIN,
    ];
    for path in paths {
        let client = ObjectStoreClient::new(ObjectStoreConfig {
            path: path.to_string(),
            ..Default::default()
        });
        for level in levels {
            assert_eq!(
                level_prefix(&client, level),
                format!("{}/{:04x}/", path, level),
                "path {path:?} level {level}"
            );
            for &(min, max) in TXIDS {
                let (min, max) = (TXID(min), TXID(max));
                assert_eq!(
                    ltx_key(&client, level, min, max),
                    format!("{}/{:04x}/{}", path, level, format_filename(min, max)),
                    "path {path:?} level {level}"
                );
            }
        }
    }
}
