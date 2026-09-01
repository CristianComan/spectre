#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENDOR="$ROOT/vendor"

mkdir -p "$VENDOR"
rm -rf "$VENDOR/SAPIENT-Proto-Files"

git clone --depth 1 https://github.com/dstl/SAPIENT-Proto-Files.git \
  "$VENDOR/SAPIENT-Proto-Files"

echo "Fetched official Dstl SAPIENT proto files."
