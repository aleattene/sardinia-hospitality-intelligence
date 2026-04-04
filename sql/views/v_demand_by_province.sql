-- View: v_demand_by_province
-- Aggregates tourism demand (arrivals + nights) by province, year, and month.
-- Excludes rows with NULL province (3 rows in 2022 with missing source data).

CREATE OR REPLACE VIEW v_demand_by_province AS
SELECT
    year,
    province,
    month,
    SUM(arrivals) AS total_arrivals,
    SUM(nights)   AS total_nights
FROM stg_tourism_flows
WHERE province IS NOT NULL
GROUP BY year, province, month;
