module github.com/casselc/observability/otel-chdb/parquetgo/dst

go 1.27.0

toolchain go1.27.1

replace (
	github.com/casselc/observability/otel-chdb/casreg => ../../casreg
	github.com/casselc/observability/otel-chdb/parquetgo => ../
	github.com/casselc/observability/otel-chdb/testgate => ../../testgate
)

require (
	github.com/anishathalye/porcupine v1.3.1
	github.com/casselc/observability/otel-chdb/casreg v0.0.0-00010101000000-000000000000
	github.com/casselc/observability/otel-chdb/parquetgo v0.0.0-00010101000000-000000000000
)

require (
	github.com/aws/aws-sdk-go-v2 v1.47.1 // indirect
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.7.20 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.5.4 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.8.4 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.5.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.13.19 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.11.5 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.14.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.20.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/s3 v1.113.4 // indirect
	github.com/aws/smithy-go v1.28.1 // indirect
	github.com/klauspost/cpuid/v2 v2.4.0 // indirect
	github.com/zeebo/blake3 v0.2.4 // indirect
	golang.org/x/sys v0.47.0 // indirect
)
