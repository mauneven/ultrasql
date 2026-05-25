# UltraSQL @RELEASE_TAG@

These GitHub release notes are rendered by the release workflow. They are not a
production claim unless every gate below is linked to evidence.

## Release Status

- Release workflow: @RELEASE_RUN_URL@
- Operator soak status: @OPERATOR_SOAK_STATUS@
- GitHub release notes: this body plus attached assets and checksums.

## Green workflow evidence

Attach these run ids before declaring the release production-ready:

- latest green CI workflow run id,
- latest green benchmark certification workflow run id,
- latest green docs workflow run id,
- release workflow run id: @RELEASE_RUN_URL@.

## 30-day operator reports

The release workflow validates `operator-reports/*.json` with
`scripts/validate-operator-soak.py --strict`. Three independent 30-day operator
reports are required. The rendered status artifact is
`operator_soak_status.json`.

## Assets

Release assets include:

- platform archives plus `.sha256` files,
- `SHASUMS256.txt`,
- `ultrasql.rb` Homebrew formula,
- Linux `.deb` and `.rpm` packages,
- `operator_soak_status.json`.

## Known Gaps

See `CHANGELOG.md`, `ROADMAP.md`, and `docs/known-incompatibilities.md`.
