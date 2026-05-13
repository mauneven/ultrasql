//! WAL segment file naming and enumeration.
//!
//! Segments live in the WAL directory as `segment_<index>` files,
//! where `<index>` is a zero-padded 10-digit decimal index. The
//! zero-padding is deliberate: lexicographic sort of these names is
//! identical to numeric sort of the indices, so directory listings
//! can be sorted with `Vec::sort_unstable` without parsing.

// The module is `pub(crate)` and only used internally by `writer` and
// `recovery`. Marking each item `pub(crate)` is redundant *to clippy*,
// but using bare `pub` triggers `unreachable_pub` from the workspace
// rust lint set. We pick `pub(crate)` and silence the clippy complaint
// at module scope.
#![allow(clippy::redundant_pub_crate)]

use std::path::{Path, PathBuf};

/// File-name prefix common to every segment.
pub(crate) const SEGMENT_PREFIX: &str = "segment_";

/// Width of the zero-padded segment index in characters.
///
/// Ten characters covers `u32::MAX`, which at 16 MiB per segment maps
/// to roughly 64 PiB of WAL — far past any plausible operational
/// lifetime for a single database instance.
pub(crate) const SEGMENT_INDEX_WIDTH: usize = 10;

// Compile-time sanity check that the literal width in `segment_path`
// matches the public constant. If you bump `SEGMENT_INDEX_WIDTH`, also
// update the `:010` width specifier in `segment_path`.
const _: () = assert!(SEGMENT_INDEX_WIDTH == 10);

/// Build the path of segment `index` inside `dir`.
pub(crate) fn segment_path(dir: &Path, index: u32) -> PathBuf {
    dir.join(format!("{SEGMENT_PREFIX}{index:010}"))
}

/// Parse a segment file name like `segment_0000000007` and return its
/// numeric index. Returns `None` if the name does not match.
pub(crate) fn parse_segment_index(name: &str) -> Option<u32> {
    let rest = name.strip_prefix(SEGMENT_PREFIX)?;
    if rest.len() != SEGMENT_INDEX_WIDTH {
        return None;
    }
    rest.parse::<u32>().ok()
}

/// Enumerate segment files in `dir`, sorted by index ascending.
///
/// Non-segment files are ignored. Returns `(index, path)` pairs.
pub(crate) fn list_segments(dir: &Path) -> std::io::Result<Vec<(u32, PathBuf)>> {
    let mut out = Vec::new();
    let read = match std::fs::read_dir(dir) {
        Ok(r) => r,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(out),
        Err(e) => return Err(e),
    };
    for entry in read {
        let entry = entry?;
        let file_type = entry.file_type()?;
        if !file_type.is_file() {
            continue;
        }
        let name = entry.file_name();
        let Some(name_str) = name.to_str() else {
            continue;
        };
        if let Some(idx) = parse_segment_index(name_str) {
            out.push((idx, entry.path()));
        }
    }
    out.sort_unstable_by_key(|(idx, _)| *idx);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn segment_path_zero_padded() {
        let p = segment_path(Path::new("/wal"), 7);
        assert_eq!(p, PathBuf::from("/wal/segment_0000000007"));
    }

    #[test]
    fn parse_segment_index_round_trips() {
        for &n in &[0_u32, 1, 42, u32::MAX] {
            let name = format!("{SEGMENT_PREFIX}{n:010}");
            assert_eq!(parse_segment_index(&name), Some(n));
        }
    }

    #[test]
    fn parse_segment_index_rejects_garbage() {
        assert!(parse_segment_index("foo").is_none());
        assert!(parse_segment_index("segment_").is_none());
        assert!(parse_segment_index("segment_xyz").is_none());
        assert!(parse_segment_index("segment_1").is_none()); // wrong width
    }
}
