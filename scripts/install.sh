#!/usr/bin/env sh
set -eu

repo="${ULTRASQL_REPO:-mauneven/ultrasql}"
version="${1:-${ULTRASQL_VERSION:-latest}}"
install_dir="${ULTRASQL_INSTALL_DIR:-$HOME/.local/bin}"

need() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "ultrasql install: missing required command: $1" >&2
    exit 1
  }
}

need curl
need tar

os="$(uname -s)"
arch="$(uname -m)"
case "${os}:${arch}" in
  Linux:x86_64) target="x86_64-unknown-linux-gnu" ;;
  Linux:aarch64|Linux:arm64) target="aarch64-unknown-linux-gnu" ;;
  Darwin:x86_64) target="x86_64-apple-darwin" ;;
  Darwin:arm64|Darwin:aarch64) target="aarch64-apple-darwin" ;;
  *)
    echo "ultrasql install: unsupported platform ${os}/${arch}" >&2
    exit 1
    ;;
esac

if [ "$version" = "latest" ]; then
  version="$(
    curl -fsSL "https://api.github.com/repos/${repo}/releases/latest" \
      | sed -n 's/.*"tag_name":[[:space:]]*"\([^"]*\)".*/\1/p' \
      | head -n 1
  )"
fi

if [ -z "$version" ]; then
  echo "ultrasql install: could not resolve release version" >&2
  exit 1
fi

asset="ultrasql-${version}-${target}.tar.gz"
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
  elif command -v shasum >/dev/null 2>&1; then
    shasum -a 256 -c "${asset}.sha256"
  else
    echo "ultrasql install: missing sha256sum or shasum" >&2
    exit 1
  fi
)

mkdir -p "$install_dir"
tar -xzf "${tmp_dir}/${asset}" -C "$tmp_dir"
cp "${tmp_dir}/ultrasql-${version}-${target}/ultrasqld" "$install_dir/"
cp "${tmp_dir}/ultrasql-${version}-${target}/ultrasql" "$install_dir/"
cp "${tmp_dir}/ultrasql-${version}-${target}/ultrasql-local" "$install_dir/"
chmod +x "$install_dir/ultrasqld" "$install_dir/ultrasql" "$install_dir/ultrasql-local"

echo "UltraSQL ${version} installed to ${install_dir}"
echo "Add ${install_dir} to PATH if it is not already present."
