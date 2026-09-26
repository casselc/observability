# A Go collector image from an ocb build: the agent/gateway build
# (../collector/build.sh, PATCHED=1 for the gateway) or the Go publisher
# (../../awss3/collector/builder-config.yaml). Build the binary first, then:
#   docker build -f images/otelcol.Dockerfile --build-arg BIN=path/to/otelcol-deploy -t REGISTRY/otelcol-deploy:v0.161.0 .
# The ocb builds use CGO_ENABLED=0 (set it in the environment for build.sh).
FROM gcr.io/distroless/static-debian12:nonroot
ARG BIN
COPY ${BIN} /otelcol
USER 10001
ENTRYPOINT ["/otelcol"]
