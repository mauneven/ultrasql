#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 4 ]; then
    echo "usage: $0 <version> <arch> <package-root> <output.yaml>" >&2
    exit 2
fi

version="$1"
arch="$2"
root="$3"
out="$4"
template="packaging/nfpm.yaml.in"

mkdir -p "$(dirname "$out")"
sed \
    -e "s|@VERSION@|${version#v}|g" \
    -e "s|@ARCH@|${arch}|g" \
    -e "s|@ROOT@|${root}|g" \
    "$template" > "$out"
