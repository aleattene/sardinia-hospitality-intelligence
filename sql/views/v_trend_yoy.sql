-- View: v_trend_yoy
-- Year-over-year growth in arrivals and nights per province.
-- Uses LAG window function to compare each year with the previous one.
-- yoy_arrivals_pct / yoy_nights_pct: percentage change vs. prior year.
-- growth_rank: province rank by YoY arrivals growth within each year (1 = fastest growing).

CREATE OR REPLACE VIEW v_trend_yoy AS
WITH annual AS (
    SELECT
        year,
        province,
        SUM(total_arrivals) AS total_arrivals,
        SUM(total_nights)   AS total_nights
    FROM v_demand_by_province
    GROUP BY year, province
),
with_lag AS (
    SELECT
        year,
        province,
        total_arrivals,
        total_nights,
        LAG(total_arrivals) OVER (PARTITION BY province ORDER BY year) AS prev_arrivals,
        LAG(total_nights)   OVER (PARTITION BY province ORDER BY year) AS prev_nights
    FROM annual
)
SELECT
    year,
    province,
    total_arrivals,
    total_nights,
    prev_arrivals,
    prev_nights,
    ROUND(
        CASE WHEN prev_arrivals > 0
             THEN (total_arrivals - prev_arrivals)::DOUBLE / prev_arrivals * 100
             ELSE NULL END,
        2
    ) AS yoy_arrivals_pct,
    ROUND(
        CASE WHEN prev_nights > 0
             THEN (total_nights - prev_nights)::DOUBLE / prev_nights * 100
             ELSE NULL END,
        2
    ) AS yoy_nights_pct,
    RANK() OVER (
        PARTITION BY year
        ORDER BY
            CASE WHEN prev_arrivals > 0
                 THEN (total_arrivals - prev_arrivals)::DOUBLE / prev_arrivals
                 ELSE NULL END DESC NULLS LAST
    ) AS growth_rank
FROM with_lag;
