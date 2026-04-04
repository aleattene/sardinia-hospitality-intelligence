-- Query: q_top_growth_segments
-- Identifies the fastest-growing province × accommodation_type segments (YoY).
-- Compares the most recent year against the previous one.
-- Only includes segments present in both years (inner join on lag).
-- growth_rank: overall rank by YoY arrivals growth (1 = fastest).

CREATE OR REPLACE TABLE q_top_growth_segments AS
WITH latest_years AS (
    SELECT
        MAX(year)                   AS current_year,
        MAX(year) - 1               AS prior_year
    FROM v_segment_accommodation
),
current AS (
    SELECT province, accommodation_type, total_arrivals, total_nights, avg_stay_length
    FROM v_segment_accommodation
    WHERE year = (SELECT current_year FROM latest_years)
      AND province IS NOT NULL
),
prior AS (
    SELECT province, accommodation_type, total_arrivals AS prev_arrivals, total_nights AS prev_nights
    FROM v_segment_accommodation
    WHERE year = (SELECT prior_year FROM latest_years)
      AND province IS NOT NULL
)
SELECT
    c.province,
    c.accommodation_type,
    c.total_arrivals,
    c.total_nights,
    c.avg_stay_length,
    p.prev_arrivals,
    p.prev_nights,
    ROUND(
        (c.total_arrivals - p.prev_arrivals)::DOUBLE / NULLIF(p.prev_arrivals, 0) * 100,
        2
    ) AS yoy_arrivals_pct,
    ROUND(
        (c.total_nights - p.prev_nights)::DOUBLE / NULLIF(p.prev_nights, 0) * 100,
        2
    ) AS yoy_nights_pct,
    RANK() OVER (
        ORDER BY
            (c.total_arrivals - p.prev_arrivals)::DOUBLE / NULLIF(p.prev_arrivals, 0) DESC NULLS LAST
    ) AS growth_rank
FROM current c
JOIN prior p
    ON c.province = p.province
    AND c.accommodation_type = p.accommodation_type
ORDER BY growth_rank;
