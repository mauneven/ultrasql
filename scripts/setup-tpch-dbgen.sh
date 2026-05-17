#!/usr/bin/env bash
set -euo pipefail

repo_url="${ULTRASQL_TPCH_DBGEN_REPO:-https://github.com/electrum/tpch-dbgen.git}"
repo_rev="${ULTRASQL_TPCH_DBGEN_REV:-32f1c1b92d1664dba542e927d23d86ffa57aa253}"
root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
tool_dir="${ULTRASQL_TPCH_DBGEN_DIR:-${root_dir}/target/tools/tpch-dbgen}"

if [[ ! -d "${tool_dir}/.git" ]]; then
  mkdir -p "$(dirname "${tool_dir}")"
  git clone "${repo_url}" "${tool_dir}"
fi

git -C "${tool_dir}" fetch --tags --quiet origin
git -C "${tool_dir}" checkout --quiet "${repo_rev}"
make -C "${tool_dir}" >/dev/null

cat <<EOF
TPC-H dbgen ready:
  ${tool_dir}/dbgen

Use:
  ULTRASQL_TPCH_DBGEN=${tool_dir}/dbgen \\
  cargo run --release -p ultrasql-bench --features sql-bench --bin tpch -- \\
    gen-data 1 target/tpch-scale1-real
EOF
