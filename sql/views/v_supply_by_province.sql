-- View: v_supply_by_province
-- Aggregates accommodation supply (facilities, beds, rooms) by province and year.
-- Uses annual granularity (month IS NULL) for 2020-2024 source data.
-- Excludes rows with NULL province (2021: column absent in source).

CREATE OR REPLACE VIEW v_supply_by_province AS
SELECT
    year,
    province,
    accommodation_type,
    SUM(facilities) AS total_facilities,
    SUM(beds)       AS total_beds,
    SUM(rooms)      AS total_rooms
FROM stg_accommodation_capacity
WHERE province IS NOT NULL
GROUP BY year, province, accommodation_type;
