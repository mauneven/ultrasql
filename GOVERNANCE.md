# UltraSQL Governance

UltraSQL is an open-source project. Governance is deliberately
lightweight: enough structure to make decisions cleanly, not enough
to slow them down.

## 1. Roles

### Contributor

A person who submits a PR, files an issue, reviews code, or participates
in discussions. There is no application; you become a contributor by
contributing.

### Reviewer

A long-standing contributor who is trusted to review PRs in a specific
area. Reviewers do not have merge rights but their approval carries
weight with maintainers. Reviewers are nominated by maintainers and
listed in `MAINTAINERS.md` with their area of focus.

### Maintainer

A reviewer who has been granted merge rights. Maintainers:

- Merge PRs (their own and others').
- Triage issues.
- Shepherd RFCs.
- Cut releases.
- Speak for the project on technical matters.

Maintainers are nominated by existing maintainers and added by
consensus among them. Maintainers may step down at any time and
return on the same basis.

### Steering Committee

For decisions that affect the project as a whole — long-term
direction, governance changes, license changes, security policy
changes — a steering committee makes the call. The steering committee
is a subset of maintainers, currently identical to the maintainer
set in the early life of the project. Once the project has more than
seven maintainers, the steering committee will be the three most-
senior maintainers.

---

## 2. Decision making

We prefer decisions made by people doing the work, with input from
the people most affected.

- **Implementation decisions** are made by the PR author, informed
  by reviewer feedback. Disagreements escalate to the relevant
  subsystem's maintainer.
- **Design decisions** that cross subsystems go through the
  [RFC process](RFC_PROCESS.md). Acceptance is by maintainer
  consensus.
- **Project-direction decisions** (governance, license, etc.) are
  made by the steering committee, on advice from the community.

Consensus is the default. When consensus does not emerge, the
steering committee may take a vote among themselves. Votes are
recorded in the relevant PR or issue.

---

## 3. Conduct

All participants follow [the Code of Conduct](CODE_OF_CONDUCT.md).
Code of Conduct enforcement is the responsibility of the steering
committee, advised by an external moderator where appropriate.

---

## 4. Removing maintainers

A maintainer who is no longer active or whose behavior is
incompatible with the project's values may be removed. Removal
requires consensus among the remaining maintainers, less the
maintainer in question. Inactivity is defined as no meaningful
contribution or review for six months.

---

## 5. Forking

UltraSQL is dual-licensed; forks are legally permitted. The project
does not police forks. If you fork, please remove the UltraSQL
trademark and project name from your fork's distinguishing identity.

---

## 6. Trademark

"UltraSQL" is a project name. It is not yet a registered trademark.
Use the name to refer to this project. Do not use it to imply
endorsement of unrelated software.

---

## 7. Changes to this document

Governance changes go through the RFC process and require steering
committee consensus.
