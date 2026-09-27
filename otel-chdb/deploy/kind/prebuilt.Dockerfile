# A runtime image around a binary built on the host (the otap-s3pq release
# build, telemetrygen): what the kind test loads instead of building
# images/otap-s3pq.Dockerfile's full Rust toolchain stage. UID 10001 as in
# that image. The binaries link glibc, so BASE must carry a glibc at least as
# new as the build host's: bookworm-slim (2.36) fails with "GLIBC_2.39 not
# found" for a binary built on Ubuntu 24.04, hence the default below.
# The directory also holds edgeprobe (deploy/edgeprobe, CGO_ENABLED=0 go
# build), the publishers' readiness probe; it is copied into every image.
#   docker build -f deploy/kind/prebuilt.Dockerfile --build-arg BIN=otap-s3pq -t localhost/otap-s3pq:kind <dir holding BIN and edgeprobe>
ARG BASE=ubuntu:24.04
FROM ${BASE}
ARG BIN
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/* \
 && useradd --uid 10001 --no-create-home --shell /usr/sbin/nologin app
COPY ${BIN} /usr/local/bin/app
COPY edgeprobe /usr/local/bin/edgeprobe
USER 10001
ENTRYPOINT ["/usr/local/bin/app"]
