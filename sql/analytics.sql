-- =============================================================================
-- Weather Pipeline - Analytics Queries
-- =============================================================================
-- Purpose  : Answer real business questions using the data collected by the
--            hourly ETL pipeline.
-- Database : PostgreSQL 15
-- Tables   : weather_records, pipeline_runs, data_quality_logs
--
-- Sections:
--   1. BASIC AGGREGATIONS     - GROUP BY, averages, counts
--   2. CTE QUERIES            - multi-step analysis using WITH clauses
--   3. WINDOW FUNCTIONS       - rankings, moving averages, row comparisons
--   4. PIPELINE HEALTH        - monitoring the pipeline itself
-- =============================================================================


-- =============================================================================
-- SECTION 1 - BASIC AGGREGATIONS
-- "What does the data look like at a high level?"
-- =============================================================================

-- Query 1: Average temperature and humidity per city
-- Use case: Quick city comparison for a travel planning dashboard
-- -------------------------------------------------------
SELECT
    city,
    country,
    ROUND(AVG(temperature)::NUMERIC, 2)   AS avg_temp_c,
    ROUND(MIN(temperature)::NUMERIC, 2)   AS min_temp_c,
    ROUND(MAX(temperature)::NUMERIC, 2)   AS max_temp_c,
    ROUND(AVG(humidity)::NUMERIC, 2)      AS avg_humidity_pct,
    COUNT(*)                               AS total_records
FROM weather_records
WHERE timestamp >= NOW() - INTERVAL '7 days'
GROUP BY city, country
ORDER BY avg_temp_c DESC;


-- Query 2: How many records were collected per city per day?
-- Use case: Data completeness check - should be 24 per city per day (hourly)
-- -------------------------------------------------------
SELECT
    city,
    DATE(timestamp)   AS collection_date,
    COUNT(*)          AS records_collected,
    -- Flag days with missing data (expected = 24 per city per day)
    CASE
        WHEN COUNT(*) < 20 THEN 'INCOMPLETE'
        WHEN COUNT(*) BETWEEN 20 AND 23 THEN 'PARTIAL'
        ELSE 'COMPLETE'
    END AS completeness_status
FROM weather_records
GROUP BY city, DATE(timestamp)
ORDER BY collection_date DESC, city;


-- Query 3: Weather condition breakdown per city
-- Use case: What types of weather does each city experience most?
-- -------------------------------------------------------
SELECT
    city,
    weather_main,
    COUNT(*)                                              AS occurrences,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY city), 1) AS percentage
FROM weather_records
GROUP BY city, weather_main
ORDER BY city, occurrences DESC;


-- =============================================================================
-- SECTION 2 - CTE QUERIES
-- "Break a complex question into readable steps"
-- A CTE (Common Table Expression) is like giving a name to a sub-query
-- so you can reference it cleanly in the main query.
-- =============================================================================

-- Query 4: Which city had the hottest single hour in the past 30 days?
-- CTE Step 1: find the max temp per city
-- CTE Step 2: join back to get the full row (time, description, etc.)
-- -------------------------------------------------------
WITH city_max_temps AS (
    SELECT
        city,
        MAX(temperature) AS max_temp
    FROM weather_records
    WHERE timestamp >= NOW() - INTERVAL '30 days'
    GROUP BY city
)
SELECT
    wr.city,
    wr.timestamp,
    wr.temperature        AS max_temp_c,
    wr.feels_like         AS feels_like_c,
    wr.description        AS weather_description,
    wr.humidity           AS humidity_pct
FROM weather_records wr
JOIN city_max_temps cmt
    ON wr.city = cmt.city
    AND wr.temperature = cmt.max_temp
ORDER BY wr.temperature DESC;


-- Query 5: Daily temperature swing (max - min) per city
-- Use case: Cities with large swings are uncomfortable for travel
-- CTE builds the daily summary, outer query ranks cities by swing
-- -------------------------------------------------------
WITH daily_range AS (
    SELECT
        city,
        DATE(timestamp)                              AS day,
        MAX(temperature)                             AS daily_max,
        MIN(temperature)                             AS daily_min,
        MAX(temperature) - MIN(temperature)          AS temp_swing_c
    FROM weather_records
    GROUP BY city, DATE(timestamp)
),
city_avg_swing AS (
    SELECT
        city,
        ROUND(AVG(temp_swing_c)::NUMERIC, 2)   AS avg_daily_swing_c,
        ROUND(MAX(temp_swing_c)::NUMERIC, 2)   AS max_single_day_swing_c
    FROM daily_range
    GROUP BY city
)
SELECT
    city,
    avg_daily_swing_c,
    max_single_day_swing_c,
    CASE
        WHEN avg_daily_swing_c > 10 THEN 'High variability'
        WHEN avg_daily_swing_c BETWEEN 5 AND 10 THEN 'Moderate variability'
        ELSE 'Stable climate'
    END AS climate_stability
FROM city_avg_swing
ORDER BY avg_daily_swing_c DESC;


-- Query 6: Anomaly detection - hours where temperature was unusually high or low
-- Uses a CTE to compute the mean and standard deviation per city,
-- then flags records that are more than 2 standard deviations from the mean
-- -------------------------------------------------------
WITH city_stats AS (
    SELECT
        city,
        AVG(temperature)        AS mean_temp,
        STDDEV(temperature)     AS stddev_temp
    FROM weather_records
    GROUP BY city
),
anomalies AS (
    SELECT
        wr.city,
        wr.timestamp,
        wr.temperature,
        cs.mean_temp,
        cs.stddev_temp,
        ABS(wr.temperature - cs.mean_temp) / NULLIF(cs.stddev_temp, 0) AS z_score
    FROM weather_records wr
    JOIN city_stats cs ON wr.city = cs.city
)
SELECT
    city,
    timestamp,
    ROUND(temperature::NUMERIC, 2)    AS temp_c,
    ROUND(mean_temp::NUMERIC, 2)      AS city_mean_c,
    ROUND(z_score::NUMERIC, 2)        AS z_score,
    CASE
        WHEN temperature > mean_temp THEN 'Unusually HOT'
        ELSE 'Unusually COLD'
    END AS anomaly_type
FROM anomalies
WHERE z_score > 2
ORDER BY z_score DESC;


-- Query 7: Best time of day to visit each city (lowest humidity + comfortable temp)
-- Multi-step CTE: compute hourly averages → score them → rank per city
-- -------------------------------------------------------
WITH hourly_averages AS (
    SELECT
        city,
        hour,
        ROUND(AVG(temperature)::NUMERIC, 2)  AS avg_temp,
        ROUND(AVG(humidity)::NUMERIC, 2)     AS avg_humidity,
        ROUND(AVG(wind_speed)::NUMERIC, 2)   AS avg_wind_speed
    FROM weather_records
    GROUP BY city, hour
),
comfort_scored AS (
    SELECT
        city,
        hour,
        avg_temp,
        avg_humidity,
        avg_wind_speed,
        -- Comfort score: penalise high humidity and extreme temps
        ROUND((100 - avg_humidity) * 0.5
            + CASE WHEN avg_temp BETWEEN 18 AND 26 THEN 30 ELSE 0 END
            + CASE WHEN avg_wind_speed < 5 THEN 20 ELSE 0 END
        , 2) AS comfort_score
    FROM hourly_averages
)
SELECT
    city,
    hour          AS hour_of_day,
    avg_temp      AS avg_temp_c,
    avg_humidity  AS avg_humidity_pct,
    comfort_score,
    RANK() OVER (PARTITION BY city ORDER BY comfort_score DESC) AS comfort_rank
FROM comfort_scored
ORDER BY city, comfort_rank;


-- =============================================================================
-- SECTION 3 - WINDOW FUNCTIONS
-- "Do calculations across rows without collapsing them"
-- Unlike GROUP BY (which gives one row per group),
-- window functions keep all rows and add a new calculated column.
-- =============================================================================

-- Query 8: Rank cities by average temperature (hottest to coldest)
-- RANK() assigns 1 to the hottest city, 2 to the next, etc.
-- -------------------------------------------------------
SELECT
    city,
    ROUND(AVG(temperature)::NUMERIC, 2)                   AS avg_temp_c,
    RANK() OVER (ORDER BY AVG(temperature) DESC)           AS temp_rank,
    ROUND(AVG(humidity)::NUMERIC, 2)                       AS avg_humidity_pct,
    RANK() OVER (ORDER BY AVG(humidity) ASC)               AS humidity_rank
FROM weather_records
GROUP BY city
ORDER BY temp_rank;


-- Query 9: 6-hour rolling average temperature per city
-- LAG() looks at a previous row. AVG() OVER() computes a moving average.
-- Use case: Smooths out noise to show temperature trends over time
-- -------------------------------------------------------
SELECT
    city,
    timestamp,
    temperature                                                  AS actual_temp_c,
    ROUND(
        AVG(temperature) OVER (
            PARTITION BY city
            ORDER BY timestamp
            ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
        )::NUMERIC, 2
    )                                                            AS rolling_6hr_avg_c
FROM weather_records
ORDER BY city, timestamp;


-- Query 10: Temperature change hour-over-hour per city
-- LAG() fetches the temperature from the previous row (1 hour ago)
-- Use case: Detect rapid temperature drops (cold front arriving)
-- -------------------------------------------------------
SELECT
    city,
    timestamp,
    temperature                                          AS current_temp_c,
    LAG(temperature) OVER (
        PARTITION BY city ORDER BY timestamp
    )                                                    AS prev_hour_temp_c,
    ROUND(
        (temperature - LAG(temperature) OVER (
            PARTITION BY city ORDER BY timestamp
        ))::NUMERIC, 2
    )                                                    AS temp_change_c,
    CASE
        WHEN temperature - LAG(temperature) OVER (
            PARTITION BY city ORDER BY timestamp
        ) > 3 THEN 'Sharp rise'
        WHEN temperature - LAG(temperature) OVER (
            PARTITION BY city ORDER BY timestamp
        ) < -3 THEN 'Sharp drop'
        ELSE 'Stable'
    END AS change_label
FROM weather_records
ORDER BY city, timestamp;


-- Query 11: Cumulative record count per city over time
-- SUM() OVER() with ORDER BY creates a running total
-- Use case: Visualise data collection growth over time
-- -------------------------------------------------------
SELECT
    city,
    DATE(timestamp)                                     AS collection_date,
    COUNT(*)                                            AS daily_records,
    SUM(COUNT(*)) OVER (
        PARTITION BY city
        ORDER BY DATE(timestamp)
    )                                                   AS cumulative_records
FROM weather_records
GROUP BY city, DATE(timestamp)
ORDER BY city, collection_date;


-- Query 12: Percentile rank of each city's temperature reading
-- PERCENT_RANK() tells you: "this reading is hotter than X% of all readings"
-- Use case: Understand where a temperature sits in the full distribution
-- -------------------------------------------------------
SELECT
    city,
    timestamp,
    temperature,
    ROUND(
        PERCENT_RANK() OVER (
            PARTITION BY city ORDER BY temperature
        )::NUMERIC * 100, 1
    ) AS percentile_rank
FROM weather_records
ORDER BY city, percentile_rank DESC;


-- Query 13: First and last recorded temperature per city per day
-- FIRST_VALUE / LAST_VALUE over a daily window
-- Use case: Morning vs night temperature comparison
-- -------------------------------------------------------
SELECT DISTINCT
    city,
    DATE(timestamp)                                AS day,
    FIRST_VALUE(temperature) OVER (
        PARTITION BY city, DATE(timestamp)
        ORDER BY timestamp
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    )                                              AS morning_temp_c,
    LAST_VALUE(temperature) OVER (
        PARTITION BY city, DATE(timestamp)
        ORDER BY timestamp
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    )                                              AS night_temp_c
FROM weather_records
ORDER BY city, day;


-- =============================================================================
-- SECTION 4 - PIPELINE HEALTH QUERIES
-- "Monitor the pipeline itself, not just the data"
-- A DE must maintain and observe the pipeline, not just build it.
-- =============================================================================

-- Query 14: Pipeline run success rate by day
-- Use case: Are there patterns in failures? (e.g., always fails at 3am?)
-- -------------------------------------------------------
WITH daily_runs AS (
    SELECT
        DATE(start_time)                         AS run_date,
        COUNT(*)                                 AS total_runs,
        SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS successful_runs,
        SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END)    AS failed_runs
    FROM pipeline_runs
    GROUP BY DATE(start_time)
)
SELECT
    run_date,
    total_runs,
    successful_runs,
    failed_runs,
    ROUND(successful_runs * 100.0 / NULLIF(total_runs, 0), 1)  AS success_rate_pct
FROM daily_runs
ORDER BY run_date DESC;


-- Query 15: Average records per successful run + data volume trend
-- Use case: Is the pipeline collecting more or less data over time?
-- (Unexpected drops might mean API rate limiting or config changes)
-- -------------------------------------------------------
WITH weekly_volume AS (
    SELECT
        DATE_TRUNC('week', start_time)           AS week_start,
        ROUND(AVG(records_processed)::NUMERIC, 0) AS avg_records_per_run,
        SUM(records_processed)                   AS total_records_week,
        COUNT(*)                                 AS total_runs
    FROM pipeline_runs
    WHERE status = 'completed'
    GROUP BY DATE_TRUNC('week', start_time)
)
SELECT
    week_start,
    avg_records_per_run,
    total_records_week,
    total_runs,
    LAG(total_records_week) OVER (ORDER BY week_start)   AS prev_week_total,
    total_records_week
        - LAG(total_records_week) OVER (ORDER BY week_start) AS week_over_week_change
FROM weekly_volume
ORDER BY week_start DESC;
