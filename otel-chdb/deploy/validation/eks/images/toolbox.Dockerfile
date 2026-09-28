# The validation toolbox: the consumer (built here, like the publisher image,
# so it links this image's glibc) plus static Go tools built on the host by
# eks/images.sh (CGO_ENABLED=0): otlpsend, faultproxy2, mixgen, s3accept,
# entityctl, aggregator, ridcheck.
#   docker build -f ../deploy/validation/eks/images/toolbox.Dockerfile --build-context bin=<dir of Go binaries> -t REPO/toolbox:TAG .
# (context: otel-chdb/otap-rs)
FROM rust:1.98.1-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends protobuf-compiler git && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN scripts/fetch-upstream.sh /upstream && cargo build --profile dist --bin consume

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl python3 procps && rm -rf /var/lib/apt/lists/* \
 && useradd --uid 10001 --create-home --shell /bin/bash tool
COPY --from=build /src/target/dist/consume /usr/local/bin/consume
COPY --from=bin . /usr/local/bin/
USER 10001
WORKDIR /home/tool
ENTRYPOINT []
CMD ["sleep", "infinity"]
