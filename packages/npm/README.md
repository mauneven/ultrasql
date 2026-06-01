# UltraSQL

Embedded UltraSQL database API plus server and CLI binaries for Node.js package
managers.

Use `Database.open(":memory:")` for an in-process database. Use `ultrasqld`
when an application should connect to a long-running SQL server process.

## Embedded Quick Start

```bash
pnpm add ultrasql
npm install ultrasql
bun add ultrasql
```

```js
const { Database } = require("ultrasql");

const db = await Database.open(":memory:");

db.run("CREATE TABLE lorem (info TEXT)");

const stmt = db.prepare("INSERT INTO lorem VALUES (?)");
for (let i = 0; i < 10; i++) {
  stmt.run(`Ipsum ${i}`);
}
stmt.finalize();

db.each("SELECT info FROM lorem", (err, row) => {
  if (err) throw err;
  console.log(row.info);
});

db.close();
```

`Database.open()` downloads the matching GitHub Release archive on first use,
verifies the published SHA-256 checksum, loads the native Node-API addon, and
opens the engine in-process. The same API is intended for Node.js and Bun's
Node-API runtime. `new Database(":memory:")` is also available after the native
addon is already present.

This npm package installs command shims for:

- `ultrasqld` - SQL database server.
- `ultrasql` - CLI client and admin tool.
- `ultrasql-local` - local query helper.

The command shims use the same release archive and checksum verification path as
embedded mode.

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

## Server Mode

Start a local server:

```bash
pnpm exec ultrasqld --listen 127.0.0.1:5433
```

Use the CLI from another terminal:

```bash
pnpm exec ultrasql -- "host=127.0.0.1 port=5433 user=ultrasql"
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
ULTRASQL_VERSION=v0.0.9 pnpm exec ultrasqld --listen 127.0.0.1:5433
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

UltraSQL is pre-alpha. It is useful for local SQL testing, embedded experiments,
and benchmark reproduction, but it is not a v1.0 production database yet. See
the project roadmap and known limitations before using it with important data.

## Links

- Repository: https://github.com/mauneven/ultrasql
- Install docs: https://github.com/mauneven/ultrasql/blob/main/docs/install.md
- Packaging docs: https://github.com/mauneven/ultrasql/blob/main/docs/packaging.md
- Roadmap: https://github.com/mauneven/ultrasql/blob/main/ROADMAP.md
