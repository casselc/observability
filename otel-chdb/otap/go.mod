module github.com/chucklehead-dev/oscope/spike/otel-chdb/otap

go 1.26.0

replace (
	github.com/chdb-io/chdb-go/v2 => ../chdb-go
	github.com/chucklehead-dev/oscope/spike/otel-chdb/chdbexporter => ../chdbexporter
	github.com/chucklehead-dev/oscope/spike/otel-chdb/parquetgo => ../parquetgo
	github.com/chucklehead-dev/oscope/spike/otel-chdb/parquetgo/compare => ../parquetgo/compare
)

require (
	github.com/apache/arrow-go/v18 v18.7.0
	github.com/aws/aws-sdk-go-v2 v1.47.1
	github.com/aws/aws-sdk-go-v2/credentials v1.20.6
	github.com/aws/aws-sdk-go-v2/service/s3 v1.113.4
	github.com/chucklehead-dev/oscope/spike/otel-chdb/chdbexporter v0.0.0-00010101000000-000000000000
	github.com/chucklehead-dev/oscope/spike/otel-chdb/parquetgo v0.0.0-00010101000000-000000000000
	github.com/chucklehead-dev/oscope/spike/otel-chdb/parquetgo/compare v0.0.0-00010101000000-000000000000
	github.com/open-telemetry/otel-arrow/go v0.57.0
	go.opentelemetry.io/collector/pdata v1.67.0
	google.golang.org/protobuf v1.36.12
)

require (
	github.com/HdrHistogram/hdrhistogram-go v1.2.0 // indirect
	github.com/andybalholm/brotli v1.2.2 // indirect
	github.com/apache/thrift v0.24.0 // indirect
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.7.20 // indirect
	github.com/aws/aws-sdk-go-v2/config v1.33.6 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.20.1 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.5.4 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.8.4 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.5.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.13.19 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.11.5 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.14.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.20.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/signin v1.10.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.38.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.43.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.51.1 // indirect
	github.com/aws/smithy-go v1.28.1 // indirect
	github.com/axiomhq/hyperloglog v0.2.6 // indirect
	github.com/cenkalti/backoff/v7 v7.0.0 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/chdb-io/chdb-go/v2 v2.0.0-00010101000000-000000000000 // indirect
	github.com/dgryski/go-metro v0.0.0-20250106013310-edb8663e5e33 // indirect
	github.com/ebitengine/purego v0.11.0-alpha.6.0.20260707033313-5f49e7c49322 // indirect
	github.com/fxamacker/cbor/v2 v2.9.4 // indirect
	github.com/go-logr/logr v1.4.4 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-viper/mapstructure/v2 v2.5.0 // indirect
	github.com/gobwas/glob v0.2.3 // indirect
	github.com/goccy/go-json v0.10.6 // indirect
	github.com/google/flatbuffers v25.12.19+incompatible // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/hashicorp/go-version v1.9.0 // indirect
	github.com/hashicorp/golang-lru/v2 v2.0.7 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/kamstrup/intmap v0.5.2 // indirect
	github.com/klauspost/compress v1.19.0 // indirect
	github.com/klauspost/cpuid/v2 v2.4.0 // indirect
	github.com/knadh/koanf/maps v0.1.3 // indirect
	github.com/knadh/koanf/providers/confmap v1.0.1 // indirect
	github.com/knadh/koanf/v2 v2.3.6 // indirect
	github.com/mitchellh/copystructure v1.2.0 // indirect
	github.com/mitchellh/reflectwalk v1.0.2 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.3-0.20250322232337-35a7c28c31ee // indirect
	github.com/parquet-go/bitpack v1.0.0 // indirect
	github.com/parquet-go/jsonlite v1.0.0 // indirect
	github.com/parquet-go/parquet-go v0.32.0 // indirect
	github.com/pierrec/lz4/v4 v4.1.27 // indirect
	github.com/stretchr/testify v1.12.1 // indirect
	github.com/twpayne/go-geom v1.6.1 // indirect
	github.com/x448/float16 v0.8.4 // indirect
	github.com/zeebo/xxh3 v1.1.0 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/collector/client v1.67.0 // indirect
	go.opentelemetry.io/collector/component v1.67.0 // indirect
	go.opentelemetry.io/collector/component/componenttest v0.161.0 // indirect
	go.opentelemetry.io/collector/config/configopaque v1.67.0 // indirect
	go.opentelemetry.io/collector/config/configoptional v1.67.0 // indirect
	go.opentelemetry.io/collector/config/configretry v1.67.0 // indirect
	go.opentelemetry.io/collector/confmap v1.67.0 // indirect
	go.opentelemetry.io/collector/consumer v1.67.0 // indirect
	go.opentelemetry.io/collector/consumer/consumererror v0.161.0 // indirect
	go.opentelemetry.io/collector/consumer/consumertest v0.161.0 // indirect
	go.opentelemetry.io/collector/consumer/xconsumer v0.161.0 // indirect
	go.opentelemetry.io/collector/exporter v1.67.0 // indirect
	go.opentelemetry.io/collector/exporter/exporterhelper v0.161.0 // indirect
	go.opentelemetry.io/collector/exporter/exportertest v0.161.0 // indirect
	go.opentelemetry.io/collector/exporter/xexporter v0.161.0 // indirect
	go.opentelemetry.io/collector/extension v1.67.0 // indirect
	go.opentelemetry.io/collector/extension/xextension v0.161.0 // indirect
	go.opentelemetry.io/collector/featuregate v1.67.0 // indirect
	go.opentelemetry.io/collector/internal/componentalias v0.161.0 // indirect
	go.opentelemetry.io/collector/pdata/pprofile v0.161.0 // indirect
	go.opentelemetry.io/collector/pdata/xpdata v0.161.0 // indirect
	go.opentelemetry.io/collector/pipeline v1.67.0 // indirect
	go.opentelemetry.io/collector/pipeline/xpipeline v0.161.0 // indirect
	go.opentelemetry.io/collector/receiver v1.67.0 // indirect
	go.opentelemetry.io/collector/receiver/receivertest v0.161.0 // indirect
	go.opentelemetry.io/collector/receiver/xreceiver v0.161.0 // indirect
	go.opentelemetry.io/otel v1.46.0 // indirect
	go.opentelemetry.io/otel/metric v1.46.0 // indirect
	go.opentelemetry.io/otel/sdk v1.46.0 // indirect
	go.opentelemetry.io/otel/sdk/metric v1.46.0 // indirect
	go.opentelemetry.io/otel/trace v1.46.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.28.0 // indirect
	go.yaml.in/yaml/v3 v3.0.5 // indirect
	golang.org/x/exp v0.0.0-20260824195058-e88cd73687aa // indirect
	golang.org/x/net v0.58.0 // indirect
	golang.org/x/sync v0.22.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.41.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260526163538-3dc84a4a5aaa // indirect
	google.golang.org/grpc v1.83.2 // indirect
)
