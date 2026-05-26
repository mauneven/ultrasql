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
workflow to:

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

## npm / pnpm

The Node package lives in `packages/npm` and publishes the clean package name
`ultrasql` to the public npm registry:

```bash
npm install -g ultrasql
pnpm add -g ultrasql
```

The release workflow also packs `ultrasql-<version>.tgz` and attaches it to the
GitHub Release, so npm-compatible installers can consume the same package before
registry credentials are configured.

The package is a binary installer, not a replacement for PostgreSQL driver
libraries. It verifies the GitHub release archive checksum during install and
then launches `ultrasql`, `ultrasqld`, or `ultrasql-local` from the vendored
release binaries.

The release workflow runs the package tests and calls:

```bash
npm publish --access public --provenance
```

GitHub Packages remains the container registry surface through GHCR. GitHub's
npm registry requires scoped package names, so the unscoped `ultrasql` package
is published to npmjs when `NPM_TOKEN` is configured.

## Homebrew

The release workflow renders `ultrasql.rb` from
`packaging/homebrew/ultrasql.rb.in` and the release checksum manifest. The
rendered formula installs the macOS release archives for Intel and Apple
Silicon hosts. When `HOMEBREW_TAP_TOKEN` is configured, the workflow also
pushes the rendered formula to the Homebrew tap repository. The default tap is
`mauneven/homebrew-tap`; set `HOMEBREW_TAP_REPOSITORY` to override it.

```bash
brew install mauneven/tap/ultrasql
```

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
| npmjs `ultrasql` | `NPM_TOKEN` |
| Chocolatey | `CHOCOLATEY_API_KEY` |
| AUR `ultrasql-bin` | `AUR_SSH_PRIVATE_KEY` |
| Homebrew tap | `HOMEBREW_TAP_TOKEN`, optional `HOMEBREW_TAP_REPOSITORY` |
