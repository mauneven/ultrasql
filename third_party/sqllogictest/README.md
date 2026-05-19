# SQLLogicTest Import Area

No upstream SQLLogicTest corpus is vendored here by default.

This directory holds import tooling, provenance records, license notes, and
filters used to audit any future import. Imported files should land under
`tests/slt/portable/imported/` or another reviewed destination, not directly in
this control directory.

Rules:

- Do not import SQLite TH3. TH3 is proprietary.
- Do not import any file without recording upstream URL, commit, and license.
- Do not assume every SQLite test asset is open source.
- Keep imported suites small and filtered. Prefer a reproducible import command
  over committing a large opaque corpus.
- Preserve upstream license notices next to imported files.

Import flow:

```sh
python3 third_party/sqllogictest/import.py \
  --source /path/to/audited/sqllogictest-checkout \
  --commit <upstream-commit-sha> \
  --dest tests/slt/portable/imported
```

The script refuses to import if it cannot find a license or copyright file in
the source checkout.
