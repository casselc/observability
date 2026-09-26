# The Rust publisher image. Build context: otel-chdb/otap-rs.
#   docker build -f ../deploy/images/otap-s3pq.Dockerfile -t REGISTRY/otap-s3pq:TAG .
# The build pins upstream otel-arrow (UPSTREAM) plus this crate's patches
# (scripts/fetch-upstream.sh) and the toolchain in rust-toolchain.toml.
FROM rust:1.98.1-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends protobuf-compiler git && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN scripts/fetch-upstream.sh /upstream && cargo build --profile dist --bin otap-s3pq

# glibc: the binary links it (README §Build and footprint).
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/* \
 && useradd --uid 10001 --no-create-home --shell /usr/sbin/nologin otap
COPY --from=build /src/target/dist/otap-s3pq /usr/local/bin/otap-s3pq
USER 10001
ENTRYPOINT ["/usr/local/bin/otap-s3pq"]
