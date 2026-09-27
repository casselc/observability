# A runtime image around a binary built on the host (the otap-s3pq release
# build, telemetrygen): what the kind test loads instead of building
# images/otap-s3pq.Dockerfile's full Rust toolchain stage. Same runtime as
# that image (bookworm-slim: the binaries link glibc; UID 10001).
#   docker build -f deploy/kind/prebuilt.Dockerfile --build-arg BIN=otap-s3pq -t localhost/otap-s3pq:kind <dir holding BIN>
FROM debian:bookworm-slim
ARG BIN
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/* \
 && useradd --uid 10001 --no-create-home --shell /usr/sbin/nologin app
COPY ${BIN} /usr/local/bin/app
USER 10001
ENTRYPOINT ["/usr/local/bin/app"]
