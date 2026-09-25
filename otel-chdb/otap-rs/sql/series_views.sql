-- The contrib otel_metrics_* schema over layout B as the Rust edge writes it
-- (src/series.rs): ../../metrics-layout/sql/b_compat_views.sql, with
--  * gauge and sum read from otel_metrics_number_points, split on the point's
--    MetricType (an edge with `series.merge_number_points: false` writes
--    otel_metrics_gauge_points / _sum_points: use the spike's views for those);
--  * `Exemplars.FilteredAttributes` from the points when the object carried
--    them, else one empty map per exemplar (the prototype's objects).
-- {db} is B's database, {vdb} the views' (a HyperDX source points here).
-- ANY LEFT JOIN: a point whose series row hasn't landed yet reads with empty
-- maps rather than vanishing (the lanes are independent).
CREATE VIEW {vdb}.otel_metrics_gauge AS
SELECT s.ResourceAttributes AS ResourceAttributes, s.ResourceSchemaUrl AS ResourceSchemaUrl, s.ScopeName AS ScopeName,
       s.ScopeVersion AS ScopeVersion, s.ScopeAttributes AS ScopeAttributes, s.ScopeDroppedAttrCount AS ScopeDroppedAttrCount,
       s.ScopeSchemaUrl AS ScopeSchemaUrl, ServiceName, MetricName, s.MetricDescription AS MetricDescription,
       s.MetricUnit AS MetricUnit, s.Attributes AS Attributes, p.StartTimeUnix AS StartTimeUnix, p.TimeUnix AS TimeUnix,
       p.Value AS Value, p.Flags AS Flags,
       if(length(p.`Exemplars.FilteredAttributes`) = length(p.`Exemplars.TimeUnix`), p.`Exemplars.FilteredAttributes`,
          arrayMap(x -> CAST(map(), 'Map(LowCardinality(String), String)'), p.`Exemplars.TimeUnix`)) AS `Exemplars.FilteredAttributes`,
       p.`Exemplars.TimeUnix` AS `Exemplars.TimeUnix`, p.`Exemplars.Value` AS `Exemplars.Value`,
       p.`Exemplars.SpanId` AS `Exemplars.SpanId`, p.`Exemplars.TraceId` AS `Exemplars.TraceId`
FROM {db}.otel_metrics_number_points AS p
ANY LEFT JOIN {db}.otel_metrics_series AS s USING (MetricName, ServiceName, series_id)
WHERE p.MetricType = 'gauge';

CREATE VIEW {vdb}.otel_metrics_sum AS
SELECT s.ResourceAttributes AS ResourceAttributes, s.ResourceSchemaUrl AS ResourceSchemaUrl, s.ScopeName AS ScopeName,
       s.ScopeVersion AS ScopeVersion, s.ScopeAttributes AS ScopeAttributes, s.ScopeDroppedAttrCount AS ScopeDroppedAttrCount,
       s.ScopeSchemaUrl AS ScopeSchemaUrl, ServiceName, MetricName, s.MetricDescription AS MetricDescription,
       s.MetricUnit AS MetricUnit, s.Attributes AS Attributes, p.StartTimeUnix AS StartTimeUnix, p.TimeUnix AS TimeUnix,
       p.Value AS Value, p.Flags AS Flags,
       if(length(p.`Exemplars.FilteredAttributes`) = length(p.`Exemplars.TimeUnix`), p.`Exemplars.FilteredAttributes`,
          arrayMap(x -> CAST(map(), 'Map(LowCardinality(String), String)'), p.`Exemplars.TimeUnix`)) AS `Exemplars.FilteredAttributes`,
       p.`Exemplars.TimeUnix` AS `Exemplars.TimeUnix`, p.`Exemplars.Value` AS `Exemplars.Value`,
       p.`Exemplars.SpanId` AS `Exemplars.SpanId`, p.`Exemplars.TraceId` AS `Exemplars.TraceId`,
       s.AggregationTemporality AS AggregationTemporality, s.IsMonotonic AS IsMonotonic
FROM {db}.otel_metrics_number_points AS p
ANY LEFT JOIN {db}.otel_metrics_series AS s USING (MetricName, ServiceName, series_id)
WHERE p.MetricType = 'sum';

CREATE VIEW {vdb}.otel_metrics_histogram AS
SELECT s.ResourceAttributes AS ResourceAttributes, s.ResourceSchemaUrl AS ResourceSchemaUrl, s.ScopeName AS ScopeName,
       s.ScopeVersion AS ScopeVersion, s.ScopeAttributes AS ScopeAttributes, s.ScopeDroppedAttrCount AS ScopeDroppedAttrCount,
       s.ScopeSchemaUrl AS ScopeSchemaUrl, ServiceName, MetricName, s.MetricDescription AS MetricDescription,
       s.MetricUnit AS MetricUnit, s.Attributes AS Attributes, p.StartTimeUnix AS StartTimeUnix, p.TimeUnix AS TimeUnix,
       p.Count AS Count, p.Sum AS Sum, p.BucketCounts AS BucketCounts, s.ExplicitBounds AS ExplicitBounds,
       if(length(p.`Exemplars.FilteredAttributes`) = length(p.`Exemplars.TimeUnix`), p.`Exemplars.FilteredAttributes`,
          arrayMap(x -> CAST(map(), 'Map(LowCardinality(String), String)'), p.`Exemplars.TimeUnix`)) AS `Exemplars.FilteredAttributes`,
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
       if(length(p.`Exemplars.FilteredAttributes`) = length(p.`Exemplars.TimeUnix`), p.`Exemplars.FilteredAttributes`,
          arrayMap(x -> CAST(map(), 'Map(LowCardinality(String), String)'), p.`Exemplars.TimeUnix`)) AS `Exemplars.FilteredAttributes`,
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
