#!/usr/bin/env bash
# scripts/build_cpp.sh - Build the QuantForge C++ engine
#
# Usage:
#   ./scripts/build_cpp.sh          # Release build
#   ./scripts/build_cpp.sh debug    # Debug + sanitizers
#   ./scripts/build_cpp.sh clean    # Clean build directory

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
CPP_DIR="$REPO_ROOT/cpp"
BUILD_DIR="$REPO_ROOT/build/cpp"
PROTO_DIR="$CPP_DIR/proto"

BUILD_TYPE="${1:-Release}"

if [[ "$BUILD_TYPE" == "clean" ]]; then
    echo "[build] Cleaning $BUILD_DIR"
    rm -rf "$BUILD_DIR"
    echo "[build] Done."
    exit 0
fi

if [[ "$BUILD_TYPE" == "debug" ]]; then
    CMAKE_BUILD_TYPE="Debug"
else
    CMAKE_BUILD_TYPE="Release"
fi

echo "========================================="
echo " QuantForge C++ Engine Build"
echo " Mode: $CMAKE_BUILD_TYPE"
echo " Root: $REPO_ROOT"
echo "========================================="

# ── Step 1: Download & compile Upstox proto ─────────────────────────────

PROTO_FILE="$PROTO_DIR/MarketDataFeed.proto"

if [[ ! -f "$PROTO_FILE" ]]; then
    echo "[build] Downloading Upstox V3 MarketDataFeed.proto..."
    mkdir -p "$PROTO_DIR"

    curl -fsSL \
        "https://assets.upstox.com/feed/market-data-feed/v3/MarketDataFeed.proto" \
        -o "$PROTO_FILE"
fi

echo "[build] Compiling protobuf files..."
cd "$PROTO_DIR"

protoc --cpp_out=. MarketDataFeed.proto 2>/dev/null || true

# Also compile our quantforge.proto if present
if [[ -f "$REPO_ROOT/proto/quantforge.proto" ]]; then
    protoc \
        --cpp_out="$PROTO_DIR" \
        -I"$REPO_ROOT/proto" \
        quantforge.proto \
        2>/dev/null || true
fi

# ── Step 2: Conan install ───────────────────────────────────────────────

mkdir -p "$BUILD_DIR"
cd "$BUILD_DIR"

if command -v conan &>/dev/null; then
    echo "[build] Running conan install..."

    conan install "$CPP_DIR" \
        --output-folder="$BUILD_DIR" \
        --build=missing \
        --settings="build_type=$CMAKE_BUILD_TYPE"
else
    echo "[build] WARNING: conan not found — assuming dependencies are system-installed"
fi

# ── Step 3: CMake configure ─────────────────────────────────────────────

echo "[build] CMake configure..."

cmake "$CPP_DIR" \
    -B "$BUILD_DIR" \
    -DCMAKE_BUILD_TYPE="$CMAKE_BUILD_TYPE" \
    -DCMAKE_TOOLCHAIN_FILE="$BUILD_DIR/conan_toolchain.cmake" \
    -DCMAKE_EXPORT_COMPILE_COMMANDS=ON

# Copy compile_commands.json to root for clangd
cp -f \
    "$BUILD_DIR/compile_commands.json" \
    "$REPO_ROOT/compile_commands.json" \
    2>/dev/null || true

# ── Step 4: Build ───────────────────────────────────────────────────────

echo "[build] Compiling ($(nproc) cores)..."

cmake \
    --build "$BUILD_DIR" \
    --parallel "$(nproc)"

echo ""
echo "========================================="
echo " Build successful!"
echo " Binary: $BUILD_DIR/quantforge-engine"
echo "========================================="
echo ""

echo "Run with:"
echo " $BUILD_DIR/quantforge-engine config/engine.yaml"