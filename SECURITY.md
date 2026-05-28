# UltraSQL Security Policy

UltraSQL is a database engine. It is the durable trust boundary for
the data its operators put in it. We take security seriously.

This document describes how to report vulnerabilities, what we do when
we receive a report, and what guarantees and non-guarantees apply to
the project at its current pre-1.0 stage.

---

## 1. Reporting a vulnerability

**Please do not file public issues for security vulnerabilities.**

Report via **GitHub Security Advisory**:
<https://github.com/mauneven/ultrasql/security/advisories/new>

Include in the report:

- A description of the vulnerability.
- The component or area of code affected (crate, module, file, line if
  possible).
- A reproducer if you have one, ideally as a self-contained Rust test
  or a Bash script with a Docker image.
- An assessment of severity, with reasoning.
- Your name and how you would like to be credited (or "anonymous").

We acknowledge reports within 72 hours. We aim to provide a triage
response within 7 days and a fix or mitigation plan within 30 days
for high or critical severity issues.

---

## 2. Supported versions

| Version          | Supported           |
| ---------------- | ------------------- |
| `0.x`            | Latest minor only   |
| `1.x`            | Will support latest two minor versions when released |

Pre-1.0 means the API surface, on-disk format, and wire protocol may
change between minor versions. Security fixes land in the latest
minor; backports to older minors are best-effort.

---

## 3. Severity rubric

We use CVSS v3.1 base scoring with the following project-specific
guidance:

- **Critical.** Authentication bypass, arbitrary code execution in
  the server process, durable data loss without operator error,
  silent corruption of committed data.
- **High.** Unauthenticated reads or writes to data the requester
  should not have access to, denial of service that requires server
  restart, MVCC violations producing wrong query results.
- **Medium.** Information disclosure to an authenticated client of
  data they should not have access to in their session, recoverable
  denial of service, planner returning a syntactically-valid plan
  that returns wrong results in a specific case.
- **Low.** Local-only information leaks (e.g., log entries containing
  data that should be redacted), defense-in-depth gaps that do not
  enable a direct attack.

Project maintainers assign severity and document the reasoning in the advisory.

---

## 4. Disclosure timeline

We follow a 90-day coordinated disclosure timeline by default.

- Day 0: vulnerability received.
- Day ≤ 3: acknowledgement to reporter.
- Day ≤ 7: triage decision communicated.
- Day ≤ 30: fix or mitigation in main branch, behind a private branch
  if needed.
- Day ≤ 60: release containing the fix.
- Day ≤ 90: public advisory.

Reporters who prefer a longer or shorter window may negotiate.
Critical issues with active exploitation in the wild are disclosed
faster.

---

## 5. Hall of fame

We credit reporters in release notes and in `SECURITY_ACKNOWLEDGEMENTS.md`
(created when the first acknowledgement lands). Reporters who prefer
anonymity are credited as "anonymous."

---

## 6. What is and is not in scope

In scope:

- Bugs in UltraSQL that allow unauthenticated or under-authenticated
  access to data, the server process, or the host.
- Bugs in UltraSQL that cause silent data corruption or violation of
  documented isolation levels.
- Wire protocol implementation flaws (message parsing, auth handshake,
  copy protocol).
- Cryptographic flaws in SCRAM-SHA-256, TLS termination (when added),
  or any other cryptographic primitive used in UltraSQL.

Not in scope:

- Issues caused by misconfiguration (e.g., running with auth
  disabled), unless UltraSQL's defaults are themselves insecure.
- Denial of service via volume of legitimate queries — DoS via cost-
  unbounded queries is a tuning concern.
- Bugs in third-party libraries that do not affect UltraSQL's
  security posture as built.
- Social engineering of project maintainers.
- Speculative findings without a reproducer.

---

## 7. Hardening practices we follow

- `forbid(unsafe_op_in_unsafe_fn)` workspace-wide.
- `cargo audit --deny yanked` runs in CI and release verification.
- ASAN and TSAN nightly jobs.
- Fuzz targets for the parser, wire protocol, and WAL record decoder.
- All untrusted input is parsed in dedicated parsers with bounded
  recursion and bounded allocation.
- Resource quotas (memory, work_mem, statement_timeout) are enforced
  per-connection.
- Persistent data directories are canonicalized before open; startup
  rejects a symlinked data directory and, on Unix, a final directory
  not owned by the server's effective UID.

---

## 8. What we will not do

- We will not negotiate bug bounty payments.
- We will not silently fix vulnerabilities without crediting the
  reporter (unless the reporter requests anonymity).
- We will not publish exploit code in advisories.
