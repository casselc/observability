# The Rust publisher image. Build context: otel-chdb/otap-rs, plus the
# readiness probe's source as the named context `edgeprobe` (../edgeprobe):
#   docker build -f ../deploy/images/otap-s3pq.Dockerfile --build-context edgeprobe=../deploy/edgeprobe -t REGISTRY/otap-s3pq:TAG .
# The build pins upstream otel-arrow (UPSTREAM) plus this crate's patches
# (scripts/fetch-upstream.sh) and the toolchain in rust-toolchain.toml.
FROM rust:1.98.1-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends protobuf-compiler git && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN scripts/fetch-upstream.sh /upstream && cargo build --profile dist --bin otap-s3pq

# The readiness probe (base/rust/publisher.yaml): static, stdlib only.
FROM golang:1.27-bookworm AS probe
COPY --from=edgeprobe . /src
RUN cd /src && CGO_ENABLED=0 go build -trimpath -ldflags=-s -o /edgeprobe .

# glibc: the binary links it (README §Build and footprint).
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/* \
 && useradd --uid 10001 --no-create-home --shell /usr/sbin/nologin otap
COPY --from=build /src/target/dist/otap-s3pq /usr/local/bin/otap-s3pq
COPY --from=probe /edgeprobe /usr/local/bin/edgeprobe
USER 10001
ENTRYPOINT ["/usr/local/bin/otap-s3pq"]
