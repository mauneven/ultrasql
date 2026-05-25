#!/usr/bin/env bash
set -euo pipefail

if ! getent group ultrasql >/dev/null 2>&1; then
    groupadd --system ultrasql
fi

if ! id -u ultrasql >/dev/null 2>&1; then
    useradd --system \
        --gid ultrasql \
        --home-dir /var/lib/ultrasql \
        --shell /usr/sbin/nologin \
        ultrasql
fi

install -d -o ultrasql -g ultrasql -m 0750 /var/lib/ultrasql
