# aws_signing_helper for the Roles Anywhere sidecar
# (../components/roles-anywhere). The release binary is published by AWS at
# rolesanywhere.amazonaws.com; pin the version and its checksum.
#   docker build -f images/aws-signing-helper.Dockerfile --build-arg SHA256=... -t REGISTRY/aws-signing-helper:1.7.1 .
FROM debian:bookworm-slim AS fetch
ARG VERSION=1.7.1
ARG SHA256
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl && rm -rf /var/lib/apt/lists/*
RUN curl -fsSLo /aws_signing_helper "https://rolesanywhere.amazonaws.com/releases/${VERSION}/X86_64/Linux/aws_signing_helper" \
 && echo "${SHA256}  /aws_signing_helper" | sha256sum -c - && chmod 0755 /aws_signing_helper

# distroless/base (glibc), in case the helper is not statically linked.
FROM gcr.io/distroless/base-debian12:nonroot
COPY --from=fetch /aws_signing_helper /aws_signing_helper
USER 10001
ENTRYPOINT ["/aws_signing_helper"]
