-- ============================================================================
-- APACHE DRUID OPTIMIZED ANALYTICAL SQL QUERIES SHOWCASE
-- ============================================================================
-- These queries target the clickstream datasources modeled in the ingestion specs.
-- They demonstrate key Druid features: __time pruning, rolling roll-up metrics,
-- and high-cardinality approximation using Theta Sketches and HyperUnique.
-- ============================================================================

-- 1. TIME-PRUNED REAL-TIME CLICKSTREAM METRICS (__time Pruning)
-- This query demonstrates Druid's partition pruning using the immutable primary
-- __time column, which guarantees that only relevant segment files are scanned,
-- achieving sub-second response times.
SELECT
  TIME_FLOOR(__time, 'PT15M') AS interval_15m,
  event_type,
  device_type,
  SUM(cnt) AS total_events,
  SUM(purchase_amount) AS total_revenue,
  AVG(duration_ms) AS avg_duration_ms
FROM "clickstream_realtime"
WHERE __time >= CURRENT_TIMESTAMP - INTERVAL '2' HOUR
  AND country = 'BR'
GROUP BY 1, 2, 3
ORDER BY interval_15m DESC, total_revenue DESC;


-- 2. HIGH-CARDINALITY USER ESTIMATION WITH THETA SKETCHES
-- Uses Theta Sketches pre-aggregated during ingestion to perform extremely fast,
-- approximate distinct user count (with standard error < 1%) across dimensions.
SELECT
  country,
  device_type,
  SUM(cnt) as total_events,
  -- THETA_SKETCH_ESTIMATE converts the sketch metric back to a numeric cardinality estimate
  THETA_SKETCH_ESTIMATE(DS_THETA(unique_users_sketch)) AS approximate_unique_users
FROM "clickstream_realtime"
WHERE __time >= TIMESTAMP '2026-07-01 00:00:00'
GROUP BY 1, 2
ORDER BY approximate_unique_users DESC;


-- 3. INTERSECTING AND COMPOSING SETS VIA THETA SKETCHES (A / B Cohorts)
-- Compares unique users who executed dynamic events, demonstrating the powerful
-- post-aggregation capability of Theta Sketches to perform set operations
-- (Union, Intersection, Difference) on distinct values on-the-fly.
WITH cohort_purchasers AS (
  SELECT DS_THETA(unique_users_sketch) AS sketch
  FROM "clickstream_realtime"
  WHERE __time >= CURRENT_TIMESTAMP - INTERVAL '1' DAY
    AND event_type = 'purchase'
),
cohort_add_to_cart AS (
  SELECT DS_THETA(unique_users_sketch) AS sketch
  FROM "clickstream_realtime"
  WHERE __time >= CURRENT_TIMESTAMP - INTERVAL '1' DAY
    AND event_type = 'add_to_cart'
)
SELECT
  -- Users who did both (Intersection)
  THETA_SKETCH_ESTIMATE(
    THETA_SKETCH_INTERSECT(p.sketch, c.sketch)
  ) AS users_who_added_and_purchased,
  -- Users who added to cart but did NOT complete purchase (Difference)
  THETA_SKETCH_ESTIMATE(
    THETA_SKETCH_DIFF(c.sketch, p.sketch)
  ) AS users_abandoned_cart,
  -- Total unique users involved in either action (Union)
  THETA_SKETCH_ESTIMATE(
    THETA_SKETCH_UNION(p.sketch, c.sketch)
  ) AS total_active_users_union
FROM cohort_purchasers p, cohort_add_to_cart c;


-- 4. HYPERUNIQUE DISTINCT SESSIONS AGGREGATION (HLL Based)
-- Computes the estimated count of distinct sessions using HyperLogLog (HyperUnique)
-- structures pre-computed during ingestion. This is even faster and lighter than Theta Sketches
-- but does not support set operations (diff, intersect).
SELECT
  referrer_source,
  APPROX_COUNT_DISTINCT_DS_HLL(unique_sessions_hll) AS approx_unique_sessions
FROM "clickstream_batch"
WHERE __time >= CURRENT_TIMESTAMP - INTERVAL '30' DAY
GROUP BY 1
ORDER BY approx_unique_sessions DESC;
