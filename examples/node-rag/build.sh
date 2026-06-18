#!/usr/bin/env bash
# Build the UltraSQL Node-API addon and place it next to the demo.
#
# No npm/pnpm install is needed — the demo has zero JavaScript dependencies and
# loads the compiled native addon directly with require("./ultrasql_node.node").
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$HERE/../.." && pwd)"
cd "$REPO_ROOT"

echo "Building ultrasql-node (release)…"
cargo build --release -p ultrasql-node

# The cdylib name and extension are platform-specific; Node loads any of them
# from a file named *.node.
case "$(uname -s)" in
  Darwin) LIB="libultrasql_node.dylib" ;;
  Linux)  LIB="libultrasql_node.so" ;;
  *)      LIB="ultrasql_node.dll" ;;
esac

cp "target/release/$LIB" "$HERE/ultrasql_node.node"
echo "Wrote $HERE/ultrasql_node.node"
echo "Run the demo:  node $HERE/rag-demo.cjs"
