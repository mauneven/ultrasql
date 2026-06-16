# Packaging and Docs Site

This page records the release packaging surface. Packaging makes artifacts
installable; it does not by itself make UltraSQL production-stable.

## Docs site

The documentation site is built with MkDocs from `mkdocs.yml` and published by
the `docs` GitHub Actions workflow. The custom domain is `docs.ultrasql.org`,
declared by `docs/CNAME`.

Local verification:

```bash
python3 -m pip install -r docs/requirements.txt
mkdocs build --strict
```

## Docker

The Docker image is built from `Dockerfile` and published by the release
workflow for `linux/amd64` and `linux/arm64` to:

```text
ghcr.io/mauneven/ultrasql:<tag>
```

The image runs as UID/GID `10001`, listens on `0.0.0.0:5432`, and stores data
under `/var/lib/ultrasql`. Docker provenance and SBOM attestations are disabled
for release images so GHCR presents a clean GHCR platform list instead of an
extra `unknown/unknown` attestation manifest row.

Local smoke build:

```bash
docker build -t ultrasql:local .
docker run --rm -p 5432:5432 -v ultrasql-data:/var/lib/ultrasql ultrasql:local
```

## npm / pnpm / Bun

The Node package lives in `packages/npm` and publishes the clean package name
`ultrasql` to the public npm registry:

```bash
npm install -g ultrasql
pnpm add -g ultrasql
bun add -g ultrasql
```

The release workflow also packs `ultrasql-<version>.tgz` and attaches it to the
GitHub Release. The npm registry publish is a required release step and uses
npm Trusted Publishing through GitHub OIDC. Configure the npm package with:

```text
Publisher: GitHub Actions
Organization or user: mauneven
Repository: ultrasql
Workflow filename: release.yml
Environment name: main
Allowed actions: npm publish
```

The workflow uses the `main` GitHub environment, requests `id-token: write`,
and does not use a long-lived npm write token.

The package exposes a Node-API embedded `Database` class plus command shims.
`Database.open(":memory:")` verifies the GitHub release archive checksum,
vendors `ultrasql.node`, and opens the engine in-process. The command shims use
the same archive and launch `ultrasql`, `ultrasqld`, or `ultrasql-local`.

Server-mode applications use the supported driver surface listed in
`docs/driver-certification.md`. Embedded mode is for in-process local
databases through the `Database` API.

The release workflow runs the package tests and calls:

```bash
npm publish --access public
```

GitHub Packages remains the container registry surface through GHCR. GitHub's
npm registry requires scoped package names, so the unscoped `ultrasql` package
is published to npmjs. Trusted Publishing automatically records provenance for
the npm package.

## Homebrew

The release workflow renders `ultrasql.rb` from
`packaging/homebrew/ultrasql.rb.in` and the release checksum manifest. The
formula is source-built: it downloads `ultrasql-v<version>-source.tar.gz`,
uses Homebrew's `rust` build dependency, runs `cargo install --locked` for the
server and CLI crates, and installs `ultrasqld`, `ultrasql`, and
`ultrasql-local`. This matches Homebrew core expectations better than a binary
formula. When `HOMEBREW_TAP_TOKEN` is configured, the workflow pushes the
rendered formula to the Homebrew tap repository. The default tap is
`mauneven/homebrew-tap`; set `HOMEBREW_TAP_REPOSITORY` to override it.

```bash
brew install mauneven/tap/ultrasql
```

After the tap is installed once, the short command works:

```bash
brew tap mauneven/tap
brew install ultrasql
```

The single-command, no-tap form:

```bash
brew install ultrasql
```

requires acceptance into `homebrew/core`. That is a separate upstream Homebrew
review path, not something the release workflow can force. The formula is now
source-built so it is shaped for that path, but the project should submit to
`homebrew/core` only after UltraSQL has a stable tagged release that is no
longer advertised as alpha or beta, builds and passes tests on
Homebrew-supported macOS and Linux targets, and satisfies Homebrew's notability
and maintainability checks. Until then, the tap is the correct distribution
channel.

## AUR

The release workflow renders `packaging/aur/PKGBUILD.in` and
`packaging/aur/.SRCINFO.in` into `ultrasql-aur-<tag>.tar.gz`. The package name
is `ultrasql-bin` because it installs the checksummed binary release tarballs.

When `AUR_SSH_PRIVATE_KEY` is configured, the workflow pushes those files to:

```text
aur@aur.archlinux.org:ultrasql-bin.git
```

Arch users install with:

```bash
yay -S ultrasql-bin
```

## Windows setup EXE and Chocolatey

The Windows release job builds a setup EXE from
`packaging/windows/ultrasql.nsi.in` with NSIS. The installer copies
`ultrasqld.exe`, `ultrasql.exe`, and `ultrasql-local.exe` to
`Program Files\UltraSQL\bin`, registers an uninstaller, and adds the bin
directory to the machine `PATH`.

The same job renders `packaging/chocolatey/ultrasql.nuspec.in`, embeds the
setup EXE checksum in `chocolateyInstall.ps1`, and runs `choco pack` to produce
`ultrasql.<version>.nupkg`. When `CHOCOLATEY_API_KEY` is configured, the
workflow runs `choco push`.

## Debian and RPM

Debian and RPM packages are built with nFPM from `packaging/nfpm.yaml.in`. They
install:

- `/usr/bin/ultrasqld`
- `/usr/bin/ultrasql`
- `/usr/bin/ultrasql-local`
- `/lib/systemd/system/ultrasqld.service`
- `/etc/ultrasql/ultrasqld.env`

The package creates a system `ultrasql` user and group when missing. The
systemd unit is hardened and writes only to `/var/lib/ultrasql`.

## Release workflow

Tagged releases build archives, the Windows setup EXE, Deb/RPM packages, the
Homebrew formula, the AUR source package, the Chocolatey nupkg, the GHCR Docker
image, and the npm package. Release publication evidence is the GitHub Actions
run id plus release assets, container digest, and publish output for npm,
Chocolatey, AUR, and the Homebrew tap.

Registry publishing is automatic once these secrets or variables are present:

| Surface | Secret / variable |
| --- | --- |
| npmjs `ultrasql` | npm Trusted Publishing: `release.yml`, environment `main` |
| Chocolatey | `CHOCOLATEY_API_KEY` |
| AUR `ultrasql-bin` | `AUR_SSH_PRIVATE_KEY` |
| Homebrew tap | `HOMEBREW_TAP_TOKEN`, optional `HOMEBREW_TAP_REPOSITORY` |
