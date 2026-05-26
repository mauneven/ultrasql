#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 3 ]; then
    echo "usage: $0 <release-tag> <SHASUMS256.txt> <output-dir>" >&2
    exit 2
fi

tag="$1"
checksums="$2"
outdir="$3"
version="${tag#v}"
template_dir="packaging/aur"

sha_for() {
    file="$1"
    awk -v file="$file" '$2 == file || $2 == "./" file { print $1 }' "$checksums" | head -n 1
}

linux_amd64_asset="ultrasql-v${version}-x86_64-unknown-linux-gnu.tar.gz"
linux_arm64_asset="ultrasql-v${version}-aarch64-unknown-linux-gnu.tar.gz"
SHA256_LINUX_AMD64="$(sha_for "$linux_amd64_asset")"
SHA256_LINUX_ARM64="$(sha_for "$linux_arm64_asset")"

if [ -z "$SHA256_LINUX_AMD64" ]; then
    echo "checksum missing for $linux_amd64_asset" >&2
    exit 1
fi
if [ -z "$SHA256_LINUX_ARM64" ]; then
    echo "checksum missing for $linux_arm64_asset" >&2
    exit 1
fi

tmp="$(mktemp -d)"
cleanup() {
    rm -rf "$tmp"
}
trap cleanup EXIT

for file in PKGBUILD .SRCINFO; do
    sed \
        -e "s|@VERSION@|${version}|g" \
        -e "s|@SHA256_LINUX_AMD64@|${SHA256_LINUX_AMD64}|g" \
        -e "s|@SHA256_LINUX_ARM64@|${SHA256_LINUX_ARM64}|g" \
        "$template_dir/${file}.in" > "$tmp/$file"
done

mkdir -p "$outdir"
tar -C "$tmp" -czf "$outdir/ultrasql-aur-${tag}.tar.gz" PKGBUILD .SRCINFO
