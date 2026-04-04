-- Query: q_seasonality_extremes
-- Identifies provinces with the least and most extreme seasonality (most recent year).
-- peak_month_share: fraction of annual nights in the single busiest month.
-- top3_month_share: combined share of the 3 busiest months.
-- seasonality_index: Herfindahl-style concentration (sum of squared monthly shares) — higher = more seasonal.
-- Lower peak_month_share = more distributed demand = year-round opportunity.

CREATE OR REPLACE TABLE q_seasonality_extremes AS
WITH latest_year AS (
    SELECT MAX(year) AS yr FROM v_seasonality_profile
),
-- Re-aggregate at province × month level (collapsing accommodation_type).
-- v_seasonality_profile.month_share is per (province, accommodation_type):
-- using it directly would produce shares that sum to > 1 per province.
province_monthly AS (
    SELECT
        province,
        month,
        SUM(monthly_nights) AS monthly_nights
    FROM v_seasonality_profile
    WHERE year = (SELECT yr FROM latest_year)
      AND province IS NOT NULL
    GROUP BY province, month
),
province_annual AS (
    SELECT
        province,
        SUM(monthly_nights) AS annual_nights
    FROM province_monthly
    GROUP BY province
),
-- Compute province-level month_share from re-aggregated data
province_month_share AS (
    SELECT
        m.province,
        m.month,
        m.monthly_nights,
        m.monthly_nights::DOUBLE / NULLIF(a.annual_nights, 0) AS month_share
    FROM province_monthly m
    JOIN province_annual a ON m.province = a.province
),
-- peak: busiest month share per province
peak AS (
    SELECT province, MAX(month_share) AS peak_month_share
    FROM province_month_share
    GROUP BY province
),
-- top3: sum of the 3 largest month_share values per province
top3 AS (
    SELECT province, SUM(month_share) AS top3_month_share
    FROM (
        SELECT
            province,
            month_share,
            ROW_NUMBER() OVER (PARTITION BY province ORDER BY month_share DESC) AS rn
        FROM province_month_share
    ) AS ranked
    WHERE rn <= 3
    GROUP BY province
),
-- Seasonality index: sum of squared monthly shares (Herfindahl-style)
-- Ranges from 1/12 (perfectly flat) to 1 (all nights in one month)
concentration AS (
    SELECT
        province,
        SUM(month_share * month_share) AS seasonality_index
    FROM province_month_share
    GROUP BY province
)
SELECT
    a.province,
    ROUND(pk.peak_month_share * 100, 2)  AS peak_month_share_pct,
    ROUND(t3.top3_month_share * 100, 2)  AS top3_month_share_pct,
    ROUND(c.seasonality_index, 4)         AS seasonality_index,
    a.annual_nights
FROM province_annual a
JOIN peak pk ON a.province = pk.province
JOIN top3 t3 ON a.province = t3.province
JOIN concentration c ON a.province = c.province
ORDER BY seasonality_index ASC;
