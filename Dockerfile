# Shopping Sessions: single-binary Rust pipeline.
#
# The scorers are linux/amd64 (Intel Xeon). No FROM line pins a platform, so
# the image builds natively wherever docker builds it.
FROM rust:1.100-slim-bookworm AS build
RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc libc6-dev \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends libgcc-s1 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /build/target/release/shopstats /app
# The scorer runs the container with no arguments, as uid 1000, with
# DATA_DIR/OUTPUT_DIR/SCRATCH_DIR/NCPU/MEM_GB in the environment.
ENTRYPOINT ["/app"]
