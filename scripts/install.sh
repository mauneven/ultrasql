#!/usr/bin/env sh
set -eu

repo="${ULTRASQL_REPO:-mauneven/ultrasql}"
version="${1:-${ULTRASQL_VERSION:-latest}}"
install_dir="${ULTRASQL_INSTALL_DIR:-$HOME/.local/bin}"

fail() {
  echo "ultrasql install: $*" >&2
  exit 1
}

need() {
  command -v "$1" >/dev/null 2>&1 || {
    fail "missing required command: $1"
  }
}

validate_repo() {
  value="$1"
  case "$value" in
    */*) ;;
    *) fail "invalid repository: $value" ;;
  esac
  owner="${value%%/*}"
  name="${value#*/}"
  if [ -z "$owner" ] || [ -z "$name" ] || [ "$name" != "${name#*/}" ]; then
    fail "invalid repository: $value"
  fi
  case "$owner" in
    *[!ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789._-]*)
      fail "invalid repository owner: $owner"
      ;;
  esac
  case "$name" in
    *[!ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789._-]*)
      fail "invalid repository name: $name"
      ;;
  esac
}

validate_release_version() {
  value="$1"
  case "$value" in
    v[0-9]*) ;;
    *) fail "invalid release version: $value" ;;
  esac
  case "$value" in
    *[!ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789._+-]*)
      fail "invalid release version: $value"
      ;;
  esac
}

validate_tar_members() {
  archive="$1"
  expected_prefix="$2"
  member_list="${tmp_dir}/archive-members.txt"
  tar -tzf "$archive" >"$member_list"
  found=0
  while IFS= read -r member; do
    if [ -z "$member" ]; then
      fail "archive contains empty path"
    fi
    case "$member" in
      /*|../*|*/../*|*/..|*\\*)
        fail "archive contains unexpected path: $member"
        ;;
    esac
    case "$member" in
      "$expected_prefix"*) found=1 ;;
      *) fail "archive contains unexpected path: $member" ;;
    esac
  done <"$member_list"
  if [ "$found" -ne 1 ]; then
    fail "archive contains no installable files"
  fi
}

validate_repo "$repo"

if [ "$version" != "latest" ]; then
  validate_release_version "$version"
fi

need curl
need tar

if ! command -v sha256sum >/dev/null 2>&1 && ! command -v shasum >/dev/null 2>&1; then
  fail "missing sha256sum or shasum"
fi

os="$(uname -s)"
arch="$(uname -m)"
case "${os}:${arch}" in
  Linux:x86_64) target="x86_64-unknown-linux-gnu" ;;
  Linux:aarch64|Linux:arm64) target="aarch64-unknown-linux-gnu" ;;
  Darwin:x86_64) target="x86_64-apple-darwin" ;;
  Darwin:arm64|Darwin:aarch64) target="aarch64-apple-darwin" ;;
  *)
    fail "unsupported platform ${os}/${arch}"
    ;;
esac

if [ "$version" = "latest" ]; then
  version="$(
    {
      curl -fsSL "https://api.github.com/repos/${repo}/releases/latest" 2>/dev/null \
        | sed -n 's/.*"tag_name":[[:space:]]*"\([^"]*\)".*/\1/p' \
        | head -n 1
    } || true
  )"
  if [ -z "$version" ]; then
    version="$(
      {
        curl -fsSL "https://api.github.com/repos/${repo}/tags" 2>/dev/null \
          | sed -n 's/.*"name":[[:space:]]*"\(v[0-9][^"]*\)".*/\1/p' \
          | head -n 1
      } || true
    )"
  fi
fi

if [ -z "$version" ]; then
  fail "could not resolve release version"
fi
validate_release_version "$version"

asset="ultrasql-${version}-${target}.tar.gz"
archive_prefix="ultrasql-${version}-${target}/"
base_url="https://github.com/${repo}/releases/download/${version}"
tmp_dir="$(mktemp -d)"
cleanup() {
  rm -rf "$tmp_dir"
}
trap cleanup EXIT INT TERM

curl -fsSL "${base_url}/${asset}" -o "${tmp_dir}/${asset}"
curl -fsSL "${base_url}/${asset}.sha256" -o "${tmp_dir}/${asset}.sha256"

(
  cd "$tmp_dir"
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum -c "${asset}.sha256"
  else
    shasum -a 256 -c "${asset}.sha256"
  fi
)

validate_tar_members "${tmp_dir}/${asset}" "$archive_prefix"

mkdir -p "$install_dir"
tar -xzf "${tmp_dir}/${asset}" -C "$tmp_dir"
cp "${tmp_dir}/${archive_prefix}ultrasqld" "$install_dir/"
cp "${tmp_dir}/${archive_prefix}ultrasql" "$install_dir/"
cp "${tmp_dir}/${archive_prefix}ultrasql-local" "$install_dir/"
chmod +x "$install_dir/ultrasqld" "$install_dir/ultrasql" "$install_dir/ultrasql-local"

echo "UltraSQL ${version} installed to ${install_dir}"
echo "Add ${install_dir} to PATH if it is not already present."
