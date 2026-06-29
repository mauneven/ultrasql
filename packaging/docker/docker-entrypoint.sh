#!/bin/sh
# UltraSQL container entrypoint.
#
# Makes the container's default configuration both BOOTABLE and SECURE. The
# server refuses to expose an unauthenticated listener on a public address, so
# a bare `docker run` must be told how clients authenticate. This mirrors the
# well-known official-image contract (cf. POSTGRES_PASSWORD /
# POSTGRES_HOST_AUTH_METHOD): no password and no explicit opt-in => the
# container refuses to start with an actionable message, never booting open.
#
# Environment:
#   ULTRASQL_PASSWORD        password for the auth user -> enables SCRAM auth
#   ULTRASQL_PASSWORD_FILE   read the password from this file instead (secret mount)
#   ULTRASQL_USER            auth user name (default: ultrasql)
#   ULTRASQL_AUTH_METHOD     scram (default) | md5
#   ULTRASQL_HOST_AUTH_METHOD=trust  accept ALL clients with no password (UNSAFE)
#   ULTRASQL_LISTEN          wire listener bind (default: 0.0.0.0:5432)
#   ULTRASQL_DATA_DIR        data directory (default: /var/lib/ultrasql)
#   ULTRASQL_OPS_LISTEN      ops /health /ready /metrics bind (default: 127.0.0.1:9100)
#   ULTRASQL_TLS_CERT/KEY    enable TLS (read by ultrasqld directly)
#
# Any extra arguments are forwarded to ultrasqld and take precedence over the
# values this script would otherwise inject.
set -eu

# `docker run <image> --flag ...` (a leading dash) means "run ultrasqld with
# these flags". An empty command also defaults to ultrasqld.
if [ "$#" -eq 0 ] || [ "${1#-}" != "$1" ]; then
    set -- ultrasqld "$@"
fi

# Any non-ultrasqld command (e.g. `ultrasql`, `sh`) runs verbatim.
if [ "$1" != "ultrasqld" ]; then
    exec "$@"
fi
shift

# POSIX-portable check for whether the operator already passed a given flag
# (as `--flag` or `--flag=value`) in the remaining positional args.
args_have() {
    want="$1"
    shift
    for a in "$@"; do
        case "$a" in
        "$want" | "$want"=*) return 0 ;;
        esac
    done
    return 1
}

LISTEN="${ULTRASQL_LISTEN:-0.0.0.0:5432}"
DATA_DIR="${ULTRASQL_DATA_DIR:-/var/lib/ultrasql}"
USER_NAME="${ULTRASQL_USER:-ultrasql}"
AUTH_METHOD="${ULTRASQL_AUTH_METHOD:-scram}"
# Expose health/readiness/metrics on loopback by default so the HEALTHCHECK has
# a target; operators override ULTRASQL_OPS_LISTEN to scrape metrics externally.
export ULTRASQL_OPS_LISTEN="${ULTRASQL_OPS_LISTEN:-127.0.0.1:9100}"

# Respect an operator who configured authentication explicitly; otherwise derive
# it from the environment.
if args_have --auth-user "$@" || args_have --hba-file "$@" || args_have --insecure-no-auth "$@"; then
    : # operator-managed auth; inject nothing
else
    pw_file=""
    if [ -n "${ULTRASQL_PASSWORD_FILE:-}" ]; then
        pw_file="$ULTRASQL_PASSWORD_FILE"
    elif [ -n "${ULTRASQL_PASSWORD:-}" ]; then
        pw_file="$(mktemp "${TMPDIR:-/tmp}/ultrasql-pw.XXXXXX")"
        chmod 600 "$pw_file"
        printf '%s' "$ULTRASQL_PASSWORD" >"$pw_file"
    fi

    if [ -n "$pw_file" ]; then
        set -- --auth-user "$USER_NAME" --auth-password-file "$pw_file" --auth-method "$AUTH_METHOD" "$@"
    elif [ "${ULTRASQL_HOST_AUTH_METHOD:-}" = "trust" ]; then
        echo "ultrasql-entrypoint: WARNING ULTRASQL_HOST_AUTH_METHOD=trust — accepting ALL clients with no password. Use only behind a trusted network boundary." >&2
        set -- --insecure-no-auth "$@"
    else
        cat >&2 <<'MSG'
ultrasql-entrypoint: FATAL — no authentication configured.

UltraSQL will not expose an unauthenticated listener on a public address.
Provide exactly one of:

  -e ULTRASQL_PASSWORD=secret            require SCRAM-SHA-256 password auth (recommended)
  -e ULTRASQL_PASSWORD_FILE=/run/secret  read the password from a mounted secret file
  -e ULTRASQL_HOST_AUTH_METHOD=trust     accept ALL clients with no password (UNSAFE; trusted nets only)

Optional: ULTRASQL_USER (default: ultrasql), ULTRASQL_AUTH_METHOD (scram|md5).
MSG
        exit 1
    fi
fi

# Inject the base flags only when the operator did not supply them.
args_have --listen "$@" || set -- --listen "$LISTEN" "$@"
args_have --data-dir "$@" || set -- --data-dir "$DATA_DIR" "$@"

exec ultrasqld "$@"
