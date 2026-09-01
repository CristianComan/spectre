#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SRC="$ROOT/vendor/SAPIENT-Proto-Files"
OUT="$ROOT/src"

if [[ ! -d "$SRC/bsi_flex_335_v2_0" ]]; then
  echo "Proto source not found. Run ./scripts/fetch_protos.sh first."
  exit 1
fi

mkdir -p "$OUT/sapient_msg/bsi_flex_335_v2_0"
touch "$OUT/sapient_msg/__init__.py"
touch "$OUT/sapient_msg/bsi_flex_335_v2_0/__init__.py"

# Re-create the package layout expected by imports inside the official proto files.
TMP="$ROOT/.proto-build"
rm -rf "$TMP"
mkdir -p "$TMP/sapient_msg/bsi_flex_335_v2_0"

cp "$SRC/proto_options.proto" "$TMP/sapient_msg/proto_options.proto"
cp "$SRC/bsi_flex_335_v2_0/"*.proto "$TMP/sapient_msg/bsi_flex_335_v2_0/"

python -m grpc_tools.protoc \
  -I "$TMP" \
  --python_out="$OUT" \
  "$TMP/sapient_msg/proto_options.proto" \
  "$TMP/sapient_msg/bsi_flex_335_v2_0/"*.proto

rm -rf "$TMP"

echo "Generated Python bindings under src/sapient_msg."
