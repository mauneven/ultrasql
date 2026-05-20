//! Shared helpers for benchmark script contract tests.

use std::process::Command;

#[cfg(windows)]
use std::env;
#[cfg(windows)]
use std::path::{Path, PathBuf};

/// Return a Bash command suitable for `bash -n` syntax checks.
///
/// Windows has a system `bash.exe` shim for WSL. On GitHub runners that
/// shim fails when no Linux distribution is installed, so Windows tests
/// must use Git Bash explicitly or skip the syntax-only check.
pub(crate) fn bash_command() -> Option<Command> {
    #[cfg(windows)]
    {
        find_git_bash().map(Command::new)
    }

    #[cfg(not(windows))]
    {
        Some(Command::new("bash"))
    }
}

#[cfg(windows)]
fn find_git_bash() -> Option<PathBuf> {
    let mut candidates = Vec::new();

    if let Some(path) = env::var_os("GIT_BASH") {
        candidates.push(PathBuf::from(path));
    }

    for var in ["ProgramFiles", "ProgramFiles(x86)"] {
        if let Some(root) = env::var_os(var) {
            push_git_layout(&mut candidates, PathBuf::from(root).join("Git"));
        }
    }

    if let Some(root) = env::var_os("LocalAppData") {
        push_git_layout(
            &mut candidates,
            PathBuf::from(root).join("Programs").join("Git"),
        );
    }

    add_where_git_candidates(&mut candidates);

    candidates.into_iter().find(|path| path.is_file())
}

#[cfg(windows)]
fn push_git_layout(candidates: &mut Vec<PathBuf>, root: PathBuf) {
    candidates.push(root.join("bin").join("bash.exe"));
    candidates.push(root.join("usr").join("bin").join("bash.exe"));
}

#[cfg(windows)]
fn add_where_git_candidates(candidates: &mut Vec<PathBuf>) {
    let Ok(output) = Command::new("where").arg("git").output() else {
        return;
    };
    if !output.status.success() {
        return;
    }

    for line in String::from_utf8_lossy(&output.stdout).lines() {
        let git_path = Path::new(line.trim());
        for ancestor in git_path.ancestors() {
            if ancestor
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.eq_ignore_ascii_case("Git"))
            {
                push_git_layout(candidates, ancestor.to_path_buf());
                break;
            }
        }
    }
}
