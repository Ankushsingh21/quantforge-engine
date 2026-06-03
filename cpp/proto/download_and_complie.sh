#!/usr/bin/env bash

# cpp/proto/download_and_compile.sh
# Downloads the Upstox V3 MarketDataFeed.proto and compiles it to C++.

set -euo pipefail

PROTO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "[proto] Downloading Upstox V3 MarketDataFeed.proto..."

curl -fsSL \
  "https://assets.upstox.com/feed/market-data-feed/v3/MarketDataFeed.proto" \
  -o "$PROTO_DIR/MarketDataFeed.proto"

echo "[proto] Compiling to C++..."

protoc --cpp_out="$PROTO_DIR" \
       -I"$PROTO_DIR" \
       MarketDataFeed.proto

echo "[proto] Done: MarketDataFeed.pb.h  MarketDataFeed.pb.cc"