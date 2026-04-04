-- Query: q_priority_score
-- Composite expansion-targeting score per province (most recent year).
-- Score components (each normalized 0-1, equal weight):
--   1. occupancy_proxy_norm  — demand pressure on existing supply
--   2. yoy_growth_norm       — recent growth momentum (arrivals YoY%)
--   3. intl_share_norm       — share of international tourists (premium segment)
-- priority_score: average of the three normalized components (0 = low, 1 = high).
-- Higher score = stronger expansion opportunity.

CREATE OR REPLACE TABLE q_priority_score AS
WITH latest_year AS (
    SELECT MAX(year) AS yr FROM v_supply_demand_gap
),
gap AS (
    SELECT province, occupancy_proxy
    FROM v_supply_demand_gap
    WHERE year = (SELECT yr FROM latest_year)
      AND province IS NOT NULL
      AND occupancy_proxy IS NOT NULL  -- exclude provinces with zero/NULL beds
),
trend AS (
    SELECT province, yoy_arrivals_pct
    FROM v_trend_yoy
    WHERE year = (SELECT yr FROM latest_year)
      AND province IS NOT NULL
      AND yoy_arrivals_pct IS NOT NULL  -- exclude first-year rows (no prior year for LAG)
),
origin AS (
    SELECT
        province,
        SUM(CASE WHEN origin_group = 'Internazionale' THEN total_arrivals ELSE 0 END)::DOUBLE
            / NULLIF(SUM(total_arrivals), 0) AS intl_share
    FROM v_segment_origin
    WHERE year = (SELECT yr FROM latest_year)
      AND province IS NOT NULL
    GROUP BY province
),
-- INNER JOIN: only provinces with complete data across all three components receive a score.
-- Provinces missing supply data (occupancy_proxy NULL) or absent from trend/origin are excluded.
-- This avoids NULL propagation in the final score and ensures all scores are comparable.
combined AS (
    SELECT
        g.province,
        g.occupancy_proxy,
        t.yoy_arrivals_pct,
        o.intl_share
    FROM gap g
    INNER JOIN trend t ON g.province = t.province
    INNER JOIN origin o ON g.province = o.province
),
normalized AS (
    SELECT
        province,
        occupancy_proxy,
        yoy_arrivals_pct,
        intl_share,
        -- Min-max normalization per component
        (occupancy_proxy - MIN(occupancy_proxy) OVER ())
            / NULLIF(MAX(occupancy_proxy) OVER () - MIN(occupancy_proxy) OVER (), 0)
            AS occupancy_proxy_norm,
        (yoy_arrivals_pct - MIN(yoy_arrivals_pct) OVER ())
            / NULLIF(MAX(yoy_arrivals_pct) OVER () - MIN(yoy_arrivals_pct) OVER (), 0)
            AS yoy_growth_norm,
        (intl_share - MIN(intl_share) OVER ())
            / NULLIF(MAX(intl_share) OVER () - MIN(intl_share) OVER (), 0)
            AS intl_share_norm
    FROM combined
)
SELECT
    province,
    ROUND(occupancy_proxy, 2)    AS occupancy_proxy,
    ROUND(yoy_arrivals_pct, 2)   AS yoy_arrivals_pct,
    ROUND(intl_share * 100, 2)   AS intl_share_pct,
    ROUND(occupancy_proxy_norm, 4) AS occupancy_proxy_norm,
    ROUND(yoy_growth_norm, 4)      AS yoy_growth_norm,
    ROUND(intl_share_norm, 4)      AS intl_share_norm,
    ROUND((occupancy_proxy_norm + yoy_growth_norm + intl_share_norm) / 3, 4) AS priority_score
FROM normalized
ORDER BY priority_score DESC NULLS LAST;
