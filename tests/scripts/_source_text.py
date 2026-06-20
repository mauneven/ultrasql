"""Shared helper for source-style contract tests.

Reading a Rust source file by path breaks when that file is split into a
directory module (``foo.rs`` -> ``foo/`` with ``mod.rs`` + submodules). This
helper transparently reads either form so style checks keep covering the whole
module after a split.
"""

from pathlib import Path


def module_text(path: Path) -> str:
    """Return the text of a ``.rs`` source file, or—if it was split into a
    directory module (``foo.rs`` -> ``foo/``)—the concatenated text of every
    ``.rs`` file in that module tree (sorted for determinism).
    """
    if path.is_file():
        return path.read_text()
    module_dir = path.with_suffix("")
    if module_dir.is_dir():
        return "\n".join(p.read_text() for p in sorted(module_dir.rglob("*.rs")))
    raise FileNotFoundError(path)
