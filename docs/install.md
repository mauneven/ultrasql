# Install UltraSQL

UltraSQL release artifacts are built by GitHub Actions from version tags.
Each release contains:

- `ultrasqld` / `ultrasqld.exe` — PostgreSQL-wire server.
- `ultrasql` / `ultrasql.exe` — CLI client and admin tool.
- `ultrasql-local` / `ultrasql-local.exe` — local read-only query helper.
- Per-asset `.sha256` files and a `SHASUMS256.txt` manifest.

## Supported binary targets

| Platform | Target | Archive |
| --- | --- | --- |
| Linux x86_64 | `x86_64-unknown-linux-gnu` | `.tar.gz` |
| Linux ARM64 | `aarch64-unknown-linux-gnu` | `.tar.gz` |
| macOS Intel | `x86_64-apple-darwin` | `.tar.gz` |
| macOS Apple Silicon | `aarch64-apple-darwin` | `.tar.gz` |
| Windows x86_64 | `x86_64-pc-windows-msvc` | `.zip` |

## macOS / Linux install script

```bash
curl -fsSL https://raw.githubusercontent.com/mauneven/ultrasql/main/scripts/install.sh | sh
```

Install a specific tag:

```bash
curl -fsSL https://raw.githubusercontent.com/mauneven/ultrasql/main/scripts/install.sh | sh -s -- v0.0.1
```

Install somewhere else:

```bash
ULTRASQL_INSTALL_DIR=/usr/local/bin \
  curl -fsSL https://raw.githubusercontent.com/mauneven/ultrasql/main/scripts/install.sh | sh
```

The script downloads the platform archive and `.sha256` file, verifies the
checksum, and installs the binaries. It does not edit shell startup files.

## Windows install script

```powershell
iwr https://raw.githubusercontent.com/mauneven/ultrasql/main/scripts/install.ps1 -UseB | iex
```

Install a specific tag:

```powershell
& ([scriptblock]::Create((iwr https://raw.githubusercontent.com/mauneven/ultrasql/main/scripts/install.ps1 -UseB))) -Version v0.0.1
```

Add the install directory to the user `PATH`:

```powershell
& ([scriptblock]::Create((iwr https://raw.githubusercontent.com/mauneven/ultrasql/main/scripts/install.ps1 -UseB))) -AddToPath
```

## Manual install

1. Open the GitHub release for the desired tag.
2. Download the archive for your platform and the matching `.sha256` file.
3. Verify the checksum.
4. Extract the archive and put the binaries on `PATH`.

macOS / Linux checksum:

```bash
shasum -a 256 -c ultrasql-v0.0.1-aarch64-apple-darwin.tar.gz.sha256
```

Windows checksum:

```powershell
$expected = (Get-Content .\ultrasql-v0.0.1-x86_64-pc-windows-msvc.zip.sha256).Split(" ")[0]
$actual = (Get-FileHash .\ultrasql-v0.0.1-x86_64-pc-windows-msvc.zip -Algorithm SHA256).Hash.ToLower()
if ($expected -ne $actual) { throw "checksum mismatch" }
```

## Build from source

```bash
git clone https://github.com/mauneven/ultrasql.git
cd ultrasql
cargo build --locked --profile release-ship --bin ultrasqld --bin ultrasql --bin ultrasql-local
```

## Release-readiness note

Binary packaging does not mean the database is production-stable. The stable
release decision is controlled by `docs/release-checklist.md`, `ROADMAP.md`,
CI, benchmark artifacts, security evidence, and operator-soak evidence.
