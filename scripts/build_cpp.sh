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


# ── Step 1: Conan install ───────────────────────────────────────────────

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


# ── Step 2: Download & compile Upstox proto ─────────────────────────────
echo "[build] compiling protobuf files..."
mkdir -p "$PROTO_DIR"
cd "$PROTO_DIR"

PROTOC=$(find ~/.conan2 -path "*/bin/protoc" | head -n 1)
if [[ -z "$PROTOC" || ! -x "$PROTOC" ]]; then
    echo "[build] ERROR: Conan protoc not found"
    exit 1
fi

echo "[build] Using protoc: $PROTOC"
"$PROTOC" --version


#2a upstox market-data proto (downliad if missing)
PROTO_FILE="$PROTO_DIR/MarketDataFeed.proto"
if [[ ! -f "$PROTO_FILE" ]]; then
    echo "[build] Downloading Upstox V3 MarketDataFeed.proto..."
    curl -fsSL \
        "https://assets.upstox.com/feed/market-data-feed/v3/MarketDataFeed.proto" \
        -o "$PROTO_FILE"
fi
#protoc -I"$PROTO_DIR" --cpp_out="$PROTO_DIR" MarketDataFeed.proto
"$PROTOC" \
 -I"$PROTO_DIR" \
 --cpp_out="$PROTO_DIR" \
 MarketDataFeed.proto
echo "[build] Genrated: MarketDataFeed.pb.cc / .h"

#2b. QunatForge internal proto (lives in quantforge-engine/proto/)
QF_PROTO_DIR="$REPO_ROOT/proto"
QF_PROTO="$QF_PROTO_DIR/quantforge.proto"
if [[ -f "$QF_PROTO" ]]; then
    #protoc -I"$QF_PROTO_DIR" --cpp_out="$PROTO_DIR" quantforge.proto
    "$PROTOC" \
     -I"$QF_PROTO_DIR" \
     --cpp_out="$PROTO_DIR" \
     "$QF_PROTO"
    echo "[build] Generated: quantforge.pb.cc  ./h"
else
    echo "[build] ERROR: $QF_PROTO not found!"
    echo " Expected: quantforge-engine/proto/quantforge.proto"
    exit 1
fi

# Verify both genrated files exist before proceeding

for f in "$PROTO_DIR/MarketDataFeed.pb.cc" "$PROTO_DIR/quantforge.pb.cc"; do
    if [[ ! -f "$f" ]]; then
        echo "[build] ERROR: protoc did not generate $f"
        echo " try: protoc --version (must be >= 3.21)"
        exit 1    
    fi
done
echo "[build] All proto files complied OK"



# ── Step 3: CMake configure ─────────────────────────────────────────────

echo "[build] CMake configure..."

# Fix: Dynamically match the path layout enforced by Conan 2.x cmake_layout
#CONAN_GENERATORS_DIR="$BUILD_DIR/build/$CMAKE_BUILD_TYPE/generators"
# cmake "$CPP_DIR" \
#     -B "$BUILD_DIR" \
#     -DCMAKE_BUILD_TYPE="$CMAKE_BUILD_TYPE" \
#     -DCMAKE_TOOLCHAIN_FILE="$CONAN_GENERATORS_DIR/conan_toolchain.cmake" \
#     -DCMAKE_EXPORT_COMPILE_COMMANDS=ON

CONAN_GENERATORS_DIR="$BUILD_DIR/build/$CMAKE_BUILD_TYPE/generators"
cmake "$CPP_DIR" \
    -B "$BUILD_DIR" \
    -DCMAKE_TOOLCHAIN_FILE="$CONAN_GENERATORS_DIR/conan_toolchain.cmake" \
    -DCMAKE_BUILD_TYPE="$CMAKE_BUILD_TYPE"
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