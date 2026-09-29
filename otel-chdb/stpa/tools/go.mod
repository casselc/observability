module github.com/casselc/observability/otel-chdb/stpa/tools

go 1.27.0

toolchain go1.27.1

require (
	github.com/casselc/observability/otel-chdb/testgate v0.0.0-00010101000000-000000000000
	gopkg.in/yaml.v3 v3.0.1
)

replace github.com/casselc/observability/otel-chdb/testgate => ../../testgate
