-- The contrib otel_metrics_* schema presented over layout B, one view per
-- type, with the contrib column names, types and order (the envelope is
-- left out). {db} is B's database, {vdb} the database the views go in (a
-- HyperDX source points its metric tables here).
--
-- ANY LEFT JOIN: the series table can hold one row per (series, cache
-- window, producer) until merges collapse them; ANY keeps one match. (ANY
-- INNER would also keep one row per key of the LEFT side.) A point whose
-- series row has not landed yet reads with empty maps rather than vanishing.
-- The join is on (MetricName, ServiceName, series_id), not series_id alone,
-- so a filter on MetricName or ServiceName (every HyperDX metric query has
-- the first) can reach the series side too.
-- Exemplars.FilteredAttributes is not carried by B's prototype (README,
-- gaps): it reads as one empty map per exemplar.

CREATE VIEW {vdb}.otel_metrics_gauge AS
SELECT s.ResourceAttributes AS ResourceAttributes, s.ResourceSchemaUrl AS ResourceSchemaUrl, s.ScopeName AS ScopeName,
       s.ScopeVersion AS ScopeVersion, s.ScopeAttributes AS ScopeAttributes, s.ScopeDroppedAttrCount AS ScopeDroppedAttrCount,
       s.ScopeSchemaUrl AS ScopeSchemaUrl, ServiceName, MetricName, s.MetricDescription AS MetricDescription,
       s.MetricUnit AS MetricUnit, s.Attributes AS Attributes, p.StartTimeUnix AS StartTimeUnix, p.TimeUnix AS TimeUnix,
       p.Value AS Value, p.Flags AS Flags,
       arrayMap(x -> CAST(map(), 'Map(LowCardinality(String), String)'), p.`Exemplars.TimeUnix`) AS `Exemplars.FilteredAttributes`,
       p.`Exemplars.TimeUnix` AS `Exemplars.TimeUnix`, p.`Exemplars.Value` AS `Exemplars.Value`,
       p.`Exemplars.SpanId` AS `Exemplars.SpanId`, p.`Exemplars.TraceId` AS `Exemplars.TraceId`
FROM {db}.otel_metrics_gauge_points AS p
ANY LEFT JOIN {db}.otel_metrics_series AS s USING (MetricName, ServiceName, series_id);

CREATE VIEW {vdb}.otel_metrics_sum AS
SELECT s.ResourceAttributes AS ResourceAttributes, s.ResourceSchemaUrl AS ResourceSchemaUrl, s.ScopeName AS ScopeName,
       s.ScopeVersion AS ScopeVersion, s.ScopeAttributes AS ScopeAttributes, s.ScopeDroppedAttrCount AS ScopeDroppedAttrCount,
       s.ScopeSchemaUrl AS ScopeSchemaUrl, ServiceName, MetricName, s.MetricDescription AS MetricDescription,
       s.MetricUnit AS MetricUnit, s.Attributes AS Attributes, p.StartTimeUnix AS StartTimeUnix, p.TimeUnix AS TimeUnix,
       p.Value AS Value, p.Flags AS Flags,
       arrayMap(x -> CAST(map(), 'Map(LowCardinality(String), String)'), p.`Exemplars.TimeUnix`) AS `Exemplars.FilteredAttributes`,
       p.`Exemplars.TimeUnix` AS `Exemplars.TimeUnix`, p.`Exemplars.Value` AS `Exemplars.Value`,
       p.`Exemplars.SpanId` AS `Exemplars.SpanId`, p.`Exemplars.TraceId` AS `Exemplars.TraceId`,
       s.AggregationTemporality AS AggregationTemporality, s.IsMonotonic AS IsMonotonic
FROM {db}.otel_metrics_sum_points AS p
ANY LEFT JOIN {db}.otel_metrics_series AS s USING (MetricName, ServiceName, series_id);

CREATE VIEW {vdb}.otel_metrics_histogram AS
SELECT s.ResourceAttributes AS ResourceAttributes, s.ResourceSchemaUrl AS ResourceSchemaUrl, s.ScopeName AS ScopeName,
       s.ScopeVersion AS ScopeVersion, s.ScopeAttributes AS ScopeAttributes, s.ScopeDroppedAttrCount AS ScopeDroppedAttrCount,
       s.ScopeSchemaUrl AS ScopeSchemaUrl, ServiceName, MetricName, s.MetricDescription AS MetricDescription,
       s.MetricUnit AS MetricUnit, s.Attributes AS Attributes, p.StartTimeUnix AS StartTimeUnix, p.TimeUnix AS TimeUnix,
       p.Count AS Count, p.Sum AS Sum, p.BucketCounts AS BucketCounts, s.ExplicitBounds AS ExplicitBounds,
       arrayMap(x -> CAST(map(), 'Map(LowCardinality(String), String)'), p.`Exemplars.TimeUnix`) AS `Exemplars.FilteredAttributes`,
       p.`Exemplars.TimeUnix` AS `Exemplars.TimeUnix`, p.`Exemplars.Value` AS `Exemplars.Value`,
       p.`Exemplars.SpanId` AS `Exemplars.SpanId`, p.`Exemplars.TraceId` AS `Exemplars.TraceId`,
       p.Flags AS Flags, p.Min AS Min, p.Max AS Max, s.AggregationTemporality AS AggregationTemporality
FROM {db}.otel_metrics_histogram_points AS p
ANY LEFT JOIN {db}.otel_metrics_series AS s USING (MetricName, ServiceName, series_id);

CREATE VIEW {vdb}.otel_metrics_exponential_histogram AS
SELECT s.ResourceAttributes AS ResourceAttributes, s.ResourceSchemaUrl AS ResourceSchemaUrl, s.ScopeName AS ScopeName,
       s.ScopeVersion AS ScopeVersion, s.ScopeAttributes AS ScopeAttributes, s.ScopeDroppedAttrCount AS ScopeDroppedAttrCount,
       s.ScopeSchemaUrl AS ScopeSchemaUrl, ServiceName, MetricName, s.MetricDescription AS MetricDescription,
       s.MetricUnit AS MetricUnit, s.Attributes AS Attributes, p.StartTimeUnix AS StartTimeUnix, p.TimeUnix AS TimeUnix,
       p.Count AS Count, p.Sum AS Sum, p.Scale AS Scale, p.ZeroCount AS ZeroCount, p.PositiveOffset AS PositiveOffset,
       p.PositiveBucketCounts AS PositiveBucketCounts, p.NegativeOffset AS NegativeOffset, p.NegativeBucketCounts AS NegativeBucketCounts,
       arrayMap(x -> CAST(map(), 'Map(LowCardinality(String), String)'), p.`Exemplars.TimeUnix`) AS `Exemplars.FilteredAttributes`,
       p.`Exemplars.TimeUnix` AS `Exemplars.TimeUnix`, p.`Exemplars.Value` AS `Exemplars.Value`,
       p.`Exemplars.SpanId` AS `Exemplars.SpanId`, p.`Exemplars.TraceId` AS `Exemplars.TraceId`,
       p.Flags AS Flags, p.Min AS Min, p.Max AS Max, s.AggregationTemporality AS AggregationTemporality
FROM {db}.otel_metrics_exponential_histogram_points AS p
ANY LEFT JOIN {db}.otel_metrics_series AS s USING (MetricName, ServiceName, series_id);

CREATE VIEW {vdb}.otel_metrics_summary AS
SELECT s.ResourceAttributes AS ResourceAttributes, s.ResourceSchemaUrl AS ResourceSchemaUrl, s.ScopeName AS ScopeName,
       s.ScopeVersion AS ScopeVersion, s.ScopeAttributes AS ScopeAttributes, s.ScopeDroppedAttrCount AS ScopeDroppedAttrCount,
       s.ScopeSchemaUrl AS ScopeSchemaUrl, ServiceName, MetricName, s.MetricDescription AS MetricDescription,
       s.MetricUnit AS MetricUnit, s.Attributes AS Attributes, p.StartTimeUnix AS StartTimeUnix, p.TimeUnix AS TimeUnix,
       p.Count AS Count, p.Sum AS Sum, p.`ValueAtQuantiles.Quantile` AS `ValueAtQuantiles.Quantile`,
       p.`ValueAtQuantiles.Value` AS `ValueAtQuantiles.Value`, p.Flags AS Flags
FROM {db}.otel_metrics_summary_points AS p
ANY LEFT JOIN {db}.otel_metrics_series AS s USING (MetricName, ServiceName, series_id);
