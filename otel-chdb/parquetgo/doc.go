// Package parquetgo publishes OpenTelemetry traces and logs as Parquet
// without chDB: pdata is walked straight into column writers of a Go Parquet
// library (parquet-go/parquet-go, or arrow-go's pqarrow via Arrow builders),
// then uploaded with an S3 client (aws-sdk-go-v2). The schema, the
// envelope columns, the object layout and the manifests are the chdb
// exporter's (../chdbexporter, publish.go), so a consumer cannot tell the two
// producers apart except by the Parquet footer's created_by.
package parquetgo
