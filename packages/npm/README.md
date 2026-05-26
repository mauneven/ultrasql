# UltraSQL

PostgreSQL-compatible OLTP + OLAP database binaries for Node.js package
managers.

UltraSQL speaks the PostgreSQL wire protocol, so Node applications use standard
PostgreSQL clients such as `pg`, Prisma, Drizzle, Kysely, TypeORM, or any other
driver that connects to PostgreSQL.

This npm package installs command shims for:

- `ultrasqld` - PostgreSQL-wire database server.
- `ultrasql` - CLI client and admin tool.
- `ultrasql-local` - local query helper.

The package is a binary launcher. It does not ship a JavaScript database client
API and it does not replace `pg`. On first command run it downloads the matching
GitHub Release archive for your platform, verifies the published SHA-256
checksum, vendors the binaries inside the package, and then launches the
requested command.

## Install

```bash
npm install -g ultrasql
pnpm add -g ultrasql
bun add -g ultrasql
```

Project-local install:

```bash
pnpm add -D ultrasql
```

## Quick Start

Start a local server:

```bash
pnpm exec ultrasqld --listen 127.0.0.1:5433
```

In another terminal:

```bash
pnpm add pg
```

Create `index.mjs`:

```js
import pg from "pg";

const { Client } = pg;

const db = new Client({
  host: "127.0.0.1",
  port: 5433,
  database: "ultrasql",
  user: "ultrasql",
});

await db.connect();

const result = await db.query(
  "SELECT id, name, score FROM users ORDER BY id"
);

console.table(result.rows);

await db.end();
```

Run it:

```bash
node index.mjs
```

## Supported Targets

| Platform | Architecture | Release target |
| --- | --- | --- |
| macOS | Apple Silicon | `aarch64-apple-darwin` |
| macOS | Intel | `x86_64-apple-darwin` |
| Linux | ARM64 | `aarch64-unknown-linux-gnu` |
| Linux | x64 | `x86_64-unknown-linux-gnu` |
| Windows | x64 | `x86_64-pc-windows-msvc` |

Unsupported platforms fail with an explicit error instead of silently building
or downloading the wrong binary.

## Version Selection

By default, the package downloads the release tag that matches its npm version.
Override the binary release tag when needed:

```bash
ULTRASQL_VERSION=v0.0.5 pnpm exec ultrasqld --listen 127.0.0.1:5433
```

Skip the binary download entirely:

```bash
ULTRASQL_NPM_SKIP_DOWNLOAD=1 pnpm add -D ultrasql
```

## Security Model

- Release archives are downloaded from GitHub Releases.
- Each archive is verified against its published `.sha256` file.
- No install-time `postinstall` script is used, so pnpm does not require build
  approval.
- Binaries are vendored under the package's platform-specific `vendor/`
  directory on first run.

## Status

UltraSQL is pre-alpha. It is useful for local testing, compatibility work, and
benchmark reproduction, but it is not a v1.0 production PostgreSQL replacement
yet. See the project roadmap and known incompatibilities before using it with
important data.

## Links

- Repository: https://github.com/mauneven/ultrasql
- Install docs: https://github.com/mauneven/ultrasql/blob/main/docs/install.md
- Packaging docs: https://github.com/mauneven/ultrasql/blob/main/docs/packaging.md
- Roadmap: https://github.com/mauneven/ultrasql/blob/main/ROADMAP.md
