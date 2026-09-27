// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// splitTraces, splitLogs and splitMetrics are copied unchanged from
// go.opentelemetry.io/collector/processor/batchprocessor@v0.161.0
// (splittraces.go, splitlogs.go, splitmetrics.go): the deterministic
// item-count split the agents' `send_batch_max_size` applied before the
// batch step moved into this exporter (batch.go). Unexported upstream.

package s3pqexporter

import (
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// splitTraces removes spans from the input trace and returns a new trace of the specified size.
func splitTraces(size int, src ptrace.Traces) ptrace.Traces {
	if src.SpanCount() <= size {
		return src
	}
	totalCopiedSpans := 0
	dest := ptrace.NewTraces()

	src.ResourceSpans().RemoveIf(func(srcRs ptrace.ResourceSpans) bool {
		// If we are done skip everything else.
		if totalCopiedSpans == size {
			return false
		}

		// If it fully fits
		srcRsSC := resourceSC(srcRs)
		if (totalCopiedSpans + srcRsSC) <= size {
			totalCopiedSpans += srcRsSC
			srcRs.MoveTo(dest.ResourceSpans().AppendEmpty())
			return true
		}

		destRs := dest.ResourceSpans().AppendEmpty()
		srcRs.Resource().CopyTo(destRs.Resource())
		destRs.SetSchemaUrl(srcRs.SchemaUrl())
		srcRs.ScopeSpans().RemoveIf(func(srcIls ptrace.ScopeSpans) bool {
			// If we are done skip everything else.
			if totalCopiedSpans == size {
				return false
			}

			// If possible to move all metrics do that.
			srcIlsSC := srcIls.Spans().Len()
			if size-totalCopiedSpans >= srcIlsSC {
				totalCopiedSpans += srcIlsSC
				srcIls.MoveTo(destRs.ScopeSpans().AppendEmpty())
				return true
			}

			destIls := destRs.ScopeSpans().AppendEmpty()
			srcIls.Scope().CopyTo(destIls.Scope())
			destIls.SetSchemaUrl(srcIls.SchemaUrl())
			srcIls.Spans().RemoveIf(func(srcSpan ptrace.Span) bool {
				// If we are done skip everything else.
				if totalCopiedSpans == size {
					return false
				}
				srcSpan.MoveTo(destIls.Spans().AppendEmpty())
				totalCopiedSpans++
				return true
			})
			return false
		})
		return srcRs.ScopeSpans().Len() == 0
	})

	return dest
}

// resourceSC calculates the total number of spans in the ptrace.ResourceSpans.
func resourceSC(rs ptrace.ResourceSpans) (count int) {
	for k := 0; k < rs.ScopeSpans().Len(); k++ {
		count += rs.ScopeSpans().At(k).Spans().Len()
	}
	return count
}

// splitLogs removes logrecords from the input data and returns a new data of the specified size.
func splitLogs(size int, src plog.Logs) plog.Logs {
	if src.LogRecordCount() <= size {
		return src
	}
	totalCopiedLogRecords := 0
	dest := plog.NewLogs()

	src.ResourceLogs().RemoveIf(func(srcRl plog.ResourceLogs) bool {
		// If we are done skip everything else.
		if totalCopiedLogRecords == size {
			return false
		}

		// If it fully fits
		srcRlLRC := resourceLRC(srcRl)
		if (totalCopiedLogRecords + srcRlLRC) <= size {
			totalCopiedLogRecords += srcRlLRC
			srcRl.MoveTo(dest.ResourceLogs().AppendEmpty())
			return true
		}

		destRl := dest.ResourceLogs().AppendEmpty()
		srcRl.Resource().CopyTo(destRl.Resource())
		destRl.SetSchemaUrl(srcRl.SchemaUrl())
		srcRl.ScopeLogs().RemoveIf(func(srcIll plog.ScopeLogs) bool {
			// If we are done skip everything else.
			if totalCopiedLogRecords == size {
				return false
			}

			// If possible to move all metrics do that.
			srcIllLRC := srcIll.LogRecords().Len()
			if size >= srcIllLRC+totalCopiedLogRecords {
				totalCopiedLogRecords += srcIllLRC
				srcIll.MoveTo(destRl.ScopeLogs().AppendEmpty())
				return true
			}

			destIll := destRl.ScopeLogs().AppendEmpty()
			srcIll.Scope().CopyTo(destIll.Scope())
			destIll.SetSchemaUrl(srcIll.SchemaUrl())
			srcIll.LogRecords().RemoveIf(func(srcMetric plog.LogRecord) bool {
				// If we are done skip everything else.
				if totalCopiedLogRecords == size {
					return false
				}
				srcMetric.MoveTo(destIll.LogRecords().AppendEmpty())
				totalCopiedLogRecords++
				return true
			})
			return false
		})
		return srcRl.ScopeLogs().Len() == 0
	})

	return dest
}

// resourceLRC calculates the total number of log records in the plog.ResourceLogs.
func resourceLRC(rs plog.ResourceLogs) (count int) {
	for k := 0; k < rs.ScopeLogs().Len(); k++ {
		count += rs.ScopeLogs().At(k).LogRecords().Len()
	}
	return count
}

// splitMetrics removes metrics from the input data and returns a new data of the specified size.
func splitMetrics(size int, src pmetric.Metrics) pmetric.Metrics {
	dataPoints := src.DataPointCount()
	if dataPoints <= size {
		return src
	}
	totalCopiedDataPoints := 0
	dest := pmetric.NewMetrics()

	src.ResourceMetrics().RemoveIf(func(srcRs pmetric.ResourceMetrics) bool {
		// If we are done skip everything else.
		if totalCopiedDataPoints == size {
			return false
		}

		// If it fully fits
		srcRsDataPointCount := resourceMetricsDPC(srcRs)
		if (totalCopiedDataPoints + srcRsDataPointCount) <= size {
			totalCopiedDataPoints += srcRsDataPointCount
			srcRs.MoveTo(dest.ResourceMetrics().AppendEmpty())
			return true
		}

		destRs := dest.ResourceMetrics().AppendEmpty()
		srcRs.Resource().CopyTo(destRs.Resource())
		destRs.SetSchemaUrl(srcRs.SchemaUrl())
		srcRs.ScopeMetrics().RemoveIf(func(srcIlm pmetric.ScopeMetrics) bool {
			// If we are done skip everything else.
			if totalCopiedDataPoints == size {
				return false
			}

			// If possible to move all metrics do that.
			srcIlmDataPointCount := scopeMetricsDPC(srcIlm)
			if srcIlmDataPointCount+totalCopiedDataPoints <= size {
				totalCopiedDataPoints += srcIlmDataPointCount
				srcIlm.MoveTo(destRs.ScopeMetrics().AppendEmpty())
				return true
			}

			destIlm := destRs.ScopeMetrics().AppendEmpty()
			srcIlm.Scope().CopyTo(destIlm.Scope())
			destIlm.SetSchemaUrl(srcIlm.SchemaUrl())
			srcIlm.Metrics().RemoveIf(func(srcMetric pmetric.Metric) bool {
				// If we are done skip everything else.
				if totalCopiedDataPoints == size {
					return false
				}

				// If possible to move all points do that.
				srcMetricPointCount := metricDPC(srcMetric)
				if srcMetricPointCount+totalCopiedDataPoints <= size {
					totalCopiedDataPoints += srcMetricPointCount
					srcMetric.MoveTo(destIlm.Metrics().AppendEmpty())
					return true
				}

				// If the metric has more data points than free slots we should split it.
				copiedDataPoints, remove := splitMetric(srcMetric, destIlm.Metrics().AppendEmpty(), size-totalCopiedDataPoints)
				totalCopiedDataPoints += copiedDataPoints
				return remove
			})
			return false
		})
		return srcRs.ScopeMetrics().Len() == 0
	})

	return dest
}

// resourceMetricsDPC calculates the total number of data points in the pmetric.ResourceMetrics.
func resourceMetricsDPC(rs pmetric.ResourceMetrics) int {
	dataPointCount := 0
	ilms := rs.ScopeMetrics()
	for k := 0; k < ilms.Len(); k++ {
		dataPointCount += scopeMetricsDPC(ilms.At(k))
	}
	return dataPointCount
}

// scopeMetricsDPC calculates the total number of data points in the pmetric.ScopeMetrics.
func scopeMetricsDPC(ilm pmetric.ScopeMetrics) int {
	dataPointCount := 0
	ms := ilm.Metrics()
	for k := 0; k < ms.Len(); k++ {
		dataPointCount += metricDPC(ms.At(k))
	}
	return dataPointCount
}

// metricDPC calculates the total number of data points in the pmetric.Metric.
func metricDPC(ms pmetric.Metric) int {
	switch ms.Type() {
	case pmetric.MetricTypeGauge:
		return ms.Gauge().DataPoints().Len()
	case pmetric.MetricTypeSum:
		return ms.Sum().DataPoints().Len()
	case pmetric.MetricTypeHistogram:
		return ms.Histogram().DataPoints().Len()
	case pmetric.MetricTypeExponentialHistogram:
		return ms.ExponentialHistogram().DataPoints().Len()
	case pmetric.MetricTypeSummary:
		return ms.Summary().DataPoints().Len()
	}
	return 0
}

// splitMetric removes metric points from the input data and moves data of the specified size to destination.
// Returns size of moved data and boolean describing, whether the metric should be removed from original slice.
func splitMetric(ms, dest pmetric.Metric, size int) (int, bool) {
	dest.SetName(ms.Name())
	dest.SetDescription(ms.Description())
	dest.SetUnit(ms.Unit())

	switch ms.Type() {
	case pmetric.MetricTypeGauge:
		return splitNumberDataPoints(ms.Gauge().DataPoints(), dest.SetEmptyGauge().DataPoints(), size)
	case pmetric.MetricTypeSum:
		destSum := dest.SetEmptySum()
		destSum.SetAggregationTemporality(ms.Sum().AggregationTemporality())
		destSum.SetIsMonotonic(ms.Sum().IsMonotonic())
		return splitNumberDataPoints(ms.Sum().DataPoints(), destSum.DataPoints(), size)
	case pmetric.MetricTypeHistogram:
		destHistogram := dest.SetEmptyHistogram()
		destHistogram.SetAggregationTemporality(ms.Histogram().AggregationTemporality())
		return splitHistogramDataPoints(ms.Histogram().DataPoints(), destHistogram.DataPoints(), size)
	case pmetric.MetricTypeExponentialHistogram:
		destHistogram := dest.SetEmptyExponentialHistogram()
		destHistogram.SetAggregationTemporality(ms.ExponentialHistogram().AggregationTemporality())
		return splitExponentialHistogramDataPoints(ms.ExponentialHistogram().DataPoints(), destHistogram.DataPoints(), size)
	case pmetric.MetricTypeSummary:
		return splitSummaryDataPoints(ms.Summary().DataPoints(), dest.SetEmptySummary().DataPoints(), size)
	}
	return size, false
}

func splitNumberDataPoints(src, dst pmetric.NumberDataPointSlice, size int) (int, bool) {
	dst.EnsureCapacity(size)
	i := 0
	src.RemoveIf(func(dp pmetric.NumberDataPoint) bool {
		if i < size {
			dp.MoveTo(dst.AppendEmpty())
			i++
			return true
		}
		return false
	})
	return size, false
}

func splitHistogramDataPoints(src, dst pmetric.HistogramDataPointSlice, size int) (int, bool) {
	dst.EnsureCapacity(size)
	i := 0
	src.RemoveIf(func(dp pmetric.HistogramDataPoint) bool {
		if i < size {
			dp.MoveTo(dst.AppendEmpty())
			i++
			return true
		}
		return false
	})
	return size, false
}

func splitExponentialHistogramDataPoints(src, dst pmetric.ExponentialHistogramDataPointSlice, size int) (int, bool) {
	dst.EnsureCapacity(size)
	i := 0
	src.RemoveIf(func(dp pmetric.ExponentialHistogramDataPoint) bool {
		if i < size {
			dp.MoveTo(dst.AppendEmpty())
			i++
			return true
		}
		return false
	})
	return size, false
}

func splitSummaryDataPoints(src, dst pmetric.SummaryDataPointSlice, size int) (int, bool) {
	dst.EnsureCapacity(size)
	i := 0
	src.RemoveIf(func(dp pmetric.SummaryDataPoint) bool {
		if i < size {
			dp.MoveTo(dst.AppendEmpty())
			i++
			return true
		}
		return false
	})
	return size, false
}
