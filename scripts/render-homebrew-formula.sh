#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 3 ]; then
    echo "usage: $0 <release-tag> <SHASUMS256.txt> <output.rb>" >&2
    exit 2
fi

tag="$1"
checksums="$2"
out="$3"
version="${tag#v}"
template="packaging/homebrew/ultrasql.rb.in"

sha_for() {
    file="$1"
    awk -v file="$file" '$2 == file || $2 == "./" file { print $1 }' "$checksums" | head -n 1
}

darwin_arm64_asset="ultrasql-v${version}-aarch64-apple-darwin.tar.gz"
darwin_amd64_asset="ultrasql-v${version}-x86_64-apple-darwin.tar.gz"
SHA256_DARWIN_ARM64="$(sha_for "$darwin_arm64_asset")"
SHA256_DARWIN_AMD64="$(sha_for "$darwin_amd64_asset")"

if [ -z "$SHA256_DARWIN_ARM64" ]; then
    echo "checksum missing for $darwin_arm64_asset" >&2
    exit 1
fi
if [ -z "$SHA256_DARWIN_AMD64" ]; then
    echo "checksum missing for $darwin_amd64_asset" >&2
    exit 1
fi

mkdir -p "$(dirname "$out")"
sed \
    -e "s|@VERSION@|${version}|g" \
    -e "s|@SHA256_DARWIN_ARM64@|${SHA256_DARWIN_ARM64}|g" \
    -e "s|@SHA256_DARWIN_AMD64@|${SHA256_DARWIN_AMD64}|g" \
    "$template" > "$out"
