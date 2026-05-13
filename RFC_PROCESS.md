# UltraSQL RFC Process

UltraSQL uses RFCs to capture and approve design changes that cross
subsystem boundaries, affect public contracts, or commit the project
to a particular direction. The process is a tool, not a ceremony: the
goal is shared understanding before code lands.

This document is short on purpose. The substance of an RFC lives in
the RFC itself.

---

## 1. When an RFC is required

You need an RFC for any change that:

- Modifies the on-disk format, the WAL record format, or any other
  durable representation.
- Modifies the PostgreSQL wire protocol implementation in ways that
  affect compatibility.
- Adds, removes, or changes a public API in a versioned crate.
- Adds or removes a workspace-level dependency that compiles into
  the server binary.
- Changes the supported MSRV or compiler/edition target.
- Introduces a new top-level subsystem or removes an existing one.
- Changes the supported isolation levels or transaction semantics.
- Changes benchmark methodology in a way that invalidates comparisons.

You do not need an RFC for:

- Localized bug fixes within a crate.
- Performance improvements that do not change observable semantics.
- Documentation updates.
- Tests, fuzz targets, benchmarks within the existing methodology.

When in doubt, open a short issue first asking whether an RFC is
needed.

---

## 2. RFC template

RFCs live under `rfcs/` and are numbered `NNNN-short-title.md`. The
template:

```markdown
# RFC NNNN — Title

- Champion: <name>
- Status:   Draft | In Review | Accepted | Rejected | Superseded
- Filed:   YYYY-MM-DD
- Last updated: YYYY-MM-DD
- Tracking issue: #N
- Implementation: PR list

## Summary
One paragraph: what changes, who benefits, what is given up.

## Motivation
Why are we doing this? What is broken or missing today? What does
the world look like if we do not do it?

## Proposal
The detailed design. Be precise. Include type signatures, on-disk
layouts, message formats, EBNF grammars — whatever is needed to
implement the proposal without further design.

## Rationale and alternatives
Why this design? What alternatives were considered, and why were
they rejected? What is the impact of not doing this?

## Compatibility
How does this affect existing users? Existing on-disk data? Existing
clients? Existing benchmarks?

## Migration
If the change is breaking, what is the migration path?

## Drawbacks
Honest description of what this design gives up.

## Prior art
References to similar designs in other systems, with citations.

## Unresolved questions
What is still open and what needs to be answered before this RFC
can be accepted?

## Future work
What does this design enable in subsequent RFCs?
```

---

## 3. Lifecycle

1. **Draft.** The author opens a PR adding `rfcs/NNNN-...md`. The PR
   stays open for review for at least 7 days.
2. **In Review.** Maintainers and the community comment. The author
   revises. Changes during review are made in-place; the PR captures
   the history.
3. **Decision.** A maintainer (the RFC's *shepherd*) calls for
   resolution. Acceptance requires consensus among the maintainer
   team — meaning no maintainer objects after a reasonable time. If
   a maintainer objects, the RFC remains in review until objections
   are resolved or the author withdraws.
4. **Accepted.** The PR merges. The RFC's status is updated. The
   author or a co-implementer opens a tracking issue and links the
   implementation PRs to the RFC.
5. **Implemented.** When the last implementation PR lands, the RFC's
   status changes to `Accepted (implemented)` in a follow-up PR.
6. **Rejected.** The PR merges with status `Rejected`. We keep
   rejected RFCs in the tree so future contributors can see what was
   considered and why.
7. **Superseded.** When an RFC is later replaced, the new RFC marks
   the old one as superseded and links forward and back.

---

## 4. Editorial expectations

- RFCs are written in clear, direct English. No marketing prose.
- Diagrams use ASCII or commit-friendly Mermaid blocks.
- Code samples compile (or are clearly marked as pseudocode).
- Benchmark numbers cited in an RFC follow [BENCHMARKS.md](BENCHMARKS.md).
- The RFC is a self-contained document. Linking to external chats or
  call notes is fine; depending on them is not.

---

## 5. Quick decisions

For trivially-uncontroversial changes that nonetheless meet the
"RFC required" criteria (e.g., bumping a workspace dependency from
0.4 → 0.5 with no API impact), a maintainer may file a
*Lightweight RFC* of one or two paragraphs and merge it after a
24-hour comment window if no objection arrives. The goal is to
maintain a public design record without ceremony where ceremony is
not earned.
