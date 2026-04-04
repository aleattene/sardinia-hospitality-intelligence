-- View: v_supply_demand_gap
-- Joins demand and supply by province and year to compute a gap proxy.
-- occupancy_proxy: average nights per available bed per year.
-- Higher values indicate higher pressure on existing supply.

CREATE OR REPLACE VIEW v_supply_demand_gap AS
WITH demand AS (
    SELECT
        year,
        province,
        SUM(total_arrivals) AS total_arrivals,
        SUM(total_nights)   AS total_nights
    FROM v_demand_by_province
    GROUP BY year, province
),
supply AS (
    SELECT
        year,
        province,
        SUM(total_facilities) AS total_facilities,
        SUM(total_beds)       AS total_beds,
        SUM(total_rooms)      AS total_rooms
    FROM v_supply_by_province
    GROUP BY year, province
)
SELECT
    d.year,
    d.province,
    d.total_arrivals,
    d.total_nights,
    s.total_facilities,
    s.total_beds,
    s.total_rooms,
    -- Nights per bed per year: proxy for occupancy pressure
    ROUND(
        CASE WHEN s.total_beds > 0 THEN d.total_nights::DOUBLE / s.total_beds ELSE NULL END,
        2
    ) AS occupancy_proxy
FROM demand d
LEFT JOIN supply s
    ON d.year = s.year
    AND d.province = s.province;
