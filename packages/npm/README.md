# UltraSQL npm package

Install UltraSQL command-line binaries with npm-compatible package managers.

```bash
npm install -g ultrasql
pnpm add -g ultrasql
```

The package downloads the matching GitHub Release archive for your platform,
verifies the published SHA-256 checksum, and exposes:

- `ultrasqld`
- `ultrasql`
- `ultrasql-local`

Applications should keep using standard PostgreSQL clients such as `pg`,
Prisma, SQLAlchemy, or `pgx`; UltraSQL speaks the PostgreSQL wire protocol.

Set `ULTRASQL_NPM_SKIP_DOWNLOAD=1` to skip binary download during package
installation, or `ULTRASQL_VERSION=vX.Y.Z` to install a specific release tag.
