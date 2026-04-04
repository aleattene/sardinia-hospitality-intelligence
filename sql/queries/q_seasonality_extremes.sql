-- Query: q_seasonality_extremes
-- Identifies provinces with the least and most extreme seasonality (most recent year).
-- peak_month_share: fraction of annual nights in the single busiest month.
-- top3_month_share: combined share of the 3 busiest months.
-- seasonality_index: Gini-like concentration — higher = more seasonal.
-- Lower peak_month_share = more distributed demand = year-round opportunity.

CREATE OR REPLACE TABLE q_seasonality_extremes AS
WITH latest_year AS (
    SELECT MAX(year) AS yr FROM v_seasonality_profile
),
province_totals AS (
    SELECT
        province,
        SUM(monthly_nights) AS annual_nights
    FROM v_seasonality_profile
    WHERE year = (SELECT yr FROM latest_year)
      AND province IS NOT NULL
    GROUP BY province
),
monthly_ranked AS (
    SELECT
        s.province,
        s.month,
        s.monthly_nights,
        s.month_share,
        RANK() OVER (PARTITION BY s.province ORDER BY s.monthly_nights DESC) AS month_rank
    FROM v_seasonality_profile s
    WHERE s.year = (SELECT yr FROM latest_year)
      AND s.province IS NOT NULL
),
peak AS (
    SELECT province, month_share AS peak_month_share
    FROM monthly_ranked
    WHERE month_rank = 1
),
top3 AS (
    SELECT province, SUM(month_share) AS top3_month_share
    FROM monthly_ranked
    WHERE month_rank <= 3
    GROUP BY province
),
-- Seasonality index: sum of squared monthly shares (Herfindahl-style)
-- Ranges from 1/12 (perfectly flat) to 1 (all nights in one month)
concentration AS (
    SELECT
        province,
        SUM(month_share * month_share) AS seasonality_index
    FROM v_seasonality_profile
    WHERE year = (SELECT yr FROM latest_year)
      AND province IS NOT NULL
    GROUP BY province
)
SELECT
    p.province,
    ROUND(pk.peak_month_share * 100, 2)  AS peak_month_share_pct,
    ROUND(t3.top3_month_share * 100, 2)  AS top3_month_share_pct,
    ROUND(c.seasonality_index, 4)         AS seasonality_index,
    p.annual_nights
FROM province_totals p
JOIN peak pk ON p.province = pk.province
JOIN top3 t3 ON p.province = t3.province
JOIN concentration c ON p.province = c.province
ORDER BY seasonality_index ASC;
