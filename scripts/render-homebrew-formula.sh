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

source_asset="ultrasql-v${version}-source.tar.gz"
SHA256_SOURCE="$(sha_for "$source_asset")"

if [ -z "$SHA256_SOURCE" ]; then
    echo "source tarball checksum missing for $source_asset" >&2
    exit 1
fi

mkdir -p "$(dirname "$out")"
sed \
    -e "s|@VERSION@|${version}|g" \
    -e "s|@SHA256_SOURCE@|${SHA256_SOURCE}|g" \
    "$template" > "$out"
