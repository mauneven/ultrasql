# Contributing to UltraSQL

Pull requests are welcome;
this document tells you how to send one that has a good chance of landing.

---

## 1. Before you start

For anything beyond a typo fix or a localized bug fix:

1. Open an issue describing what you want to do and why. The
   maintainer will respond with directional feedback, a pointer to a
   related design, or a request for an RFC.
2. For changes that cross subsystem boundaries or touch on-disk
   formats / wire protocols / public APIs, open an RFC under `rfcs/`
   using the template described in [RFC_PROCESS.md](RFC_PROCESS.md).
3. For changes within a single crate that match the existing
   architecture, an issue is sufficient.

This matters because PRs that wander into shared invariants without
discussion either get rewritten or rejected, which wastes everyone's
time.

---

## 2. Development setup

```bash
git clone https://github.com/mauneven/ultrasql.git
cd ultrasql

# Toolchain is pinned via rust-toolchain.toml. Rustup installs it on demand.
rustup show

# Build and test.
cargo build --workspace
cargo test  --workspace

# Lint and format.
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

The default development host is Apple Mac mini M4 running macOS 26.
Linux x86_64 and Linux ARM64 are also supported. Test locally on your target
platform before pushing; GitHub Actions is not used (to avoid metered costs).

### Pre-push gate vs CI

The pre-push hook is a **fast smoke gate** (target < 90 s total):

| Step | What runs | Why |
|------|-----------|-----|
| `cargo fmt --check` | format check | ~1 s |
| `cargo clippy` | lints | ~30 s after first compile |
| `cargo test --lib` | lib-level unit tests only | faster than `--workspace` |
| `cargo doc` (strict) | doc warnings | ~20 s |
| `cargo deny check advisories` | CVE scan | skips bans/licenses |
| `regression-gate --smoke` | 1 run/benchmark, no floors | ≤ 5 s |

Full suite gates (integration tests, direct `cargo audit --deny yanked`,
full `cargo-deny`, full bench sweep) run on the remote CI at merge time.
This is intentional: pre-push is a "did this compile and not obviously
crash?" check, not a correctness gate.

**Before merging a performance-sensitive change**, run the full bench sweep
locally and include before/after numbers in your PR description:

```bash
make bench-full     # full sweep (iterations=8, warmup=2)
make bench-record   # full sweep + write baselines/<stage>.json
```

Slow tests (multi-thread contention stress, real-time sleeps) are tagged
`#[ignore]` and skipped by `--lib`. Run them with:

```bash
cargo test --workspace -- --ignored
```

---

## 3. Sending a pull request

A good PR has the following properties:

1. **Single concern.** One feature, one bug fix, one refactor. Mixing
   them makes review harder and bisects more painful.
2. **Compiles and tests cleanly.** `cargo test --workspace` is green
   locally before you push.
3. **Linted and formatted.** `cargo clippy` clean,
   `cargo fmt --all -- --check` clean.
4. **Tested where it counts.** New behavior has a test exercising it;
   bug fixes have a regression test that fails before the fix.
5. **Documented where it counts.** Public API changes update doc
   comments and ARCHITECTURE.md if relevant.
6. **Benchmarked where it matters.** Changes that touch hot paths
   include before/after numbers in the description.
7. **Conventional commit messages.** See [AGENTS.md §10](AGENTS.md#10-commit-standards).
8. **Drafted as draft if WIP.** Marking a PR draft signals reviewers
   to skip until you're ready.

The PR description template is in `.github/pull_request_template.md`.
It asks for:

- A one-line summary.
- Motivation: why this change.
- Approach: how the change works.
- Testing: how you verified correctness.
- Performance: how you verified performance, if relevant.
- Risk: what could break, and how to roll back.

---

## 4. Review etiquette

- Reviewers ask questions and propose alternatives. Authors decide.
  Maintainers break ties.
- Reviews are about the change, not the author.
- A blocking comment names the blocker explicitly and offers a path
  to resolution. "I don't like this" is not a review.
- Authors respond to every comment, either by addressing it or by
  explaining why they will not.
- Reviews from non-maintainers are welcome and weighted on technical
  merit.

---

## 5. CLA and licensing

UltraSQL is dual-licensed under Apache 2.0 and MIT. By submitting a
contribution, you agree that:

- Your contribution is your own work or you have permission to submit
  it under both licenses.
- You license the contribution under both Apache 2.0 and MIT
  simultaneously.

We do not require a separate CLA document. The act of submitting a
PR is the agreement.

---

## 6. Areas that need help

Some parts of the project are best contributed by people with specific
backgrounds. We will document up-for-grabs items with the `help-wanted`
label.

Categories of contribution that are particularly welcome:

- **Optimizer rules.** Concrete, well-tested transformations with a
  before/after EXPLAIN diff.
- **Vectorized kernels.** Filter, hash, aggregate, sort. Scalar
  implementation first, intrinsics second.
- **Compatibility patches.** A pg_catalog view, a system function,
  an operator missing from the dialect, a wire protocol corner case.
- **Documentation.** Cookbook recipes, tuning guides, ops runbooks.
- **Benchmarks.** New workloads, especially industry benchmarks we do
  not cover. The methodology is non-negotiable; the workloads are
  not.
- **Bug fixes with reproductions.** A reduced reproduction is worth
  five PRs.

---

## 7. Code of Conduct

We expect everyone interacting in UltraSQL's spaces — issues, PRs,
discussions — to follow [our Code of Conduct](CODE_OF_CONDUCT.md). The
short version: be a colleague.

---

## 8. Maintainer expectations

If you want to become a maintainer:

- Contribute regularly across multiple subsystems.
- Review other people's PRs.
- Help triage issues.
- Stick around for at least six months.

Maintainers are nominated by existing maintainers and announced in
the release notes when added.

---

## 9. Questions

If you are not sure whether your change is welcome, open a Discussion
or an issue first. We would rather have the conversation early.
