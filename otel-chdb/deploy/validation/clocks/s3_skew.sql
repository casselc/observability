-- Edge clock against the store's clock, from the objects themselves
-- (clocks-and-skew.md §3): a cross-check of the probes that needs no access
-- to the nodes. For every data object still in the bucket, S3's
-- LastModified (`_time`, the store's clock, 1 s resolution) minus the rows'
-- received_at (the edge's clock at custody) is the time the request spent in
-- the edge (batch, buffer, encode, PUT) plus the skew between the two
-- clocks. The time spent is >= 0, so per producer:
--
--   min(delay) < 0   the edge's clock is AHEAD of S3's by at least -min(delay) (minus 1 s of resolution)
--   min(delay)       an upper bound on (edge behind S3) + the fastest custody time
--
-- {s3}: s3() arguments before the format, e.g.
--   'https://s3.us-east-1.amazonaws.com/B/validation/v1/edge/*/*/traces/*/*.parquet', 'KEY', 'SECRET'
-- Run it before GC deletes the objects (consume gc keeps nothing it ingested:
-- stop consume-gc for the window, or read a lane the consumer does not own).
SELECT
    producer_id,
    count() AS objects,
    round(min(delay_ms) / 1000, 3) AS min_delay_s,
    round(quantile(0.5)(delay_ms) / 1000, 3) AS p50_delay_s,
    round(max(delay_ms) / 1000, 3) AS max_delay_s,
    if(min(delay_ms) < -1000, 'edge AHEAD of S3 by >= ' || toString(round((-min(delay_ms) - 1000) / 1000, 3)) || ' s', 'consistent within 1 s') AS verdict
FROM
(
    SELECT producer_id, _path, dateDiff('millisecond', min(received_at), toDateTime64(any(_time), 3)) AS delay_ms
    FROM s3({s3}, 'Parquet')
    GROUP BY producer_id, _path
)
GROUP BY producer_id
ORDER BY min_delay_s
FORMAT PrettyCompactMonoBlock;
