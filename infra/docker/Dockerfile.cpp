# QuantForge C++ Engine Dockerfile

FROM ubuntu:24.04 AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    build-essential cmake ninja-build git \
    libboost-all-dev libssl-dev libcurl4-openssl-dev \
    protobuf-compiler libprotobuf-dev \
    libhiredis-dev \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Install Conan
RUN pip3 install conan==2.2.0

# Copy source
WORKDIR /build

COPY cpp/ ./cpp/
COPY proto/ ./proto/

# Install dependencies via Conan
RUN conan profile detect --force

RUN conan install cpp/ --build=missing \
    -s build_type=Release \
    -s compiler.cppstd=23 \
    --output-folder=build/

# Build
RUN cmake -B build/release -S cpp/ -G Ninja \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_TOOLCHAIN_FILE=build/conan_toolchain.cmake \
    -DENABLE_TESTS=OFF \
    -DCMAKE_CXX_FLAGS="-march=native -O3 -ffast-math"

RUN cmake --build build/release --parallel 8

# ── Runtime stage ───────────────────────────────────────────

FROM ubuntu:24.04 AS runtime

RUN apt-get update && apt-get install -y \
    libssl3 libcurl4 libhiredis1.1.0 libprotobuf32 \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -m -u 1001 quantforge

WORKDIR /app

COPY --from=builder /build/build/release/quantforge-engine ./
COPY --from=builder /build/build/release/lib*.so* ./lib/ 2>/dev/null || true

# Config and model directories (mounted at runtime)
RUN mkdir -p config models logs
RUN chown -R quantforge:quantforge /app

USER quantforge

EXPOSE 9101  # Prometheus metrics endpoint

HEALTHCHECK --interval=10s --timeout=5s --start-period=30s --retries=3 \
    CMD curl -sf http://localhost:9101/health/live || exit 1

ENTRYPOINT ["./quantforge-engine"]

CMD ["--config", "config/engine.yaml"]