// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package awss3exporter // import "github.com/chucklehead-dev/oscope/spike/otel-chdb/awss3/awss3inline"

import (
	"context"
	"fmt"
	"path"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"

	"github.com/chucklehead-dev/oscope/spike/otel-chdb/awss3/awss3inline/internal/upload"
	"github.com/chucklehead-dev/oscope/spike/otel-chdb/awss3/inline"
)

type s3Exporter struct {
	config     *Config
	signalType string
	uploader   upload.Manager
	logger     *zap.Logger
	marshaler  marshaler
	// key_mode sequence
	lanes       *inline.Lanes
	encodingExt any
}

func newS3Exporter(
	config *Config,
	signalType string,
	params exporter.Settings,
) *s3Exporter {
	s3Exporter := &s3Exporter{
		config:     config,
		signalType: signalType,
		logger:     params.Logger,
	}
	return s3Exporter
}

func (e *s3Exporter) getUploadOpts(res pcommon.Resource) *upload.UploadOptions {
	s3Prefix := ""
	s3Bucket := ""
	if s3PrefixKey := e.config.ResourceAttrsToS3.S3Prefix; s3PrefixKey != "" {
		if value, ok := res.Attributes().Get(s3PrefixKey); ok {
			s3Prefix = value.AsString()
		}
	}
	if s3BucketKey := e.config.ResourceAttrsToS3.S3Bucket; s3BucketKey != "" {
		if value, ok := res.Attributes().Get(s3BucketKey); ok {
			s3Bucket = value.AsString()
		}
	}
	uploadOpts := &upload.UploadOptions{
		OverrideBucket: s3Bucket,
		OverridePrefix: s3Prefix,
	}
	return uploadOpts
}

func (e *s3Exporter) start(ctx context.Context, host component.Host) error {
	var m marshaler
	var err error
	if e.config.Encoding != nil {
		if m, err = newMarshalerFromEncoding(e.config.Encoding, e.config.EncodingFileExtension, host, e.logger); err != nil {
			return err
		}
	} else {
		if m, err = newMarshaler(e.config.MarshalerName, e.logger); err != nil {
			return fmt.Errorf("unknown marshaler %q", e.config.MarshalerName)
		}
	}

	e.marshaler = m

	if e.config.S3Uploader.KeyMode == "sequence" {
		client, err := newS3Client(ctx, e.config)
		if err != nil {
			return err
		}
		if e.config.Encoding != nil {
			e.encodingExt = host.GetExtensions()[*e.config.Encoding]
		}
		prefix := path.Join(e.config.S3Uploader.S3BasePrefix, e.config.S3Uploader.S3Prefix, e.signalType)
		e.lanes = inline.NewLanes(&inline.Store{S3: client, Bucket: e.config.S3Uploader.S3Bucket, Prefix: prefix},
			e.config.S3Uploader.FilePrefix, e.config.S3Uploader.Lanes)
		return nil
	}

	up, err := newUploadManager(ctx, e.config, e.logger, e.signalType, m.format(), m.compressed())
	if err != nil {
		return err
	}
	e.uploader = up
	return nil
}

func (*s3Exporter) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (e *s3Exporter) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	buf, err := e.marshaler.MarshalMetrics(md)
	if err != nil {
		return err
	}

	uploadOpts := e.getUploadOpts(md.ResourceMetrics().At(0).Resource())
	return e.uploader.Upload(ctx, buf, uploadOpts)
}

func (e *s3Exporter) ConsumeLogs(ctx context.Context, logs plog.Logs) error {
	if e.lanes != nil {
		h, err := inline.ContentHashLogs(logs)
		if err != nil {
			return err
		}
		return e.appendSlot(ctx, h, func(epoch string, seq uint64) ([]byte, map[string]string, error) {
			if m, ok := e.encodingExt.(interface {
				MarshalLogsSlot(plog.Logs, string, uint64) ([]byte, map[string]string, error)
			}); ok {
				return m.MarshalLogsSlot(logs, epoch, seq)
			}
			b, err := e.marshaler.MarshalLogs(logs)
			return b, nil, err
		})
	}
	buf, err := e.marshaler.MarshalLogs(logs)
	if err != nil {
		return err
	}

	uploadOpts := e.getUploadOpts(logs.ResourceLogs().At(0).Resource())

	return e.uploader.Upload(ctx, buf, uploadOpts)
}

func (e *s3Exporter) ConsumeTraces(ctx context.Context, traces ptrace.Traces) error {
	if e.lanes != nil {
		h, err := inline.ContentHashTraces(traces)
		if err != nil {
			return err
		}
		return e.appendSlot(ctx, h, func(epoch string, seq uint64) ([]byte, map[string]string, error) {
			if m, ok := e.encodingExt.(interface {
				MarshalTracesSlot(ptrace.Traces, string, uint64) ([]byte, map[string]string, error)
			}); ok {
				return m.MarshalTracesSlot(traces, epoch, seq)
			}
			b, err := e.marshaler.MarshalTraces(traces)
			return b, nil, err
		})
	}
	buf, err := e.marshaler.MarshalTraces(traces)
	if err != nil {
		return err
	}

	uploadOpts := e.getUploadOpts(traces.ResourceSpans().At(0).Resource())

	return e.uploader.Upload(ctx, buf, uploadOpts)
}

// appendSlot commits one request as the next slot of a log (key_mode
// sequence). An error means the outcome is not known yet: the queue's retry
// of the same request comes back here with the same content hash and is
// resolved against the slot it may already hold, never committed twice.
func (e *s3Exporter) appendSlot(ctx context.Context, content string,
	marshal func(epoch string, seq uint64) ([]byte, map[string]string, error),
) error {
	contentType := ""
	if ct, ok := e.encodingExt.(interface{ ObjectContentType() string }); ok {
		contentType = ct.ObjectContentType()
	}
	ref, err := e.lanes.Append(ctx, content, func(epoch string, seq uint64) (inline.Object, error) {
		b, meta, err := marshal(epoch, seq)
		if err != nil {
			return inline.Object{}, consumererror.NewPermanent(err)
		}
		return inline.Object{Body: b, ContentType: contentType, Meta: meta}, nil
	})
	if err != nil {
		return err
	}
	e.logger.Debug("committed", zap.String("epoch", ref.Epoch), zap.Uint64("seq", ref.Seq), zap.String("content", content))
	return nil
}
