-- View: v_segment_origin
-- Demand segmented by tourist origin (domestic vs. international) per province and month.
-- origin_group: coalesces origin_macro (2018-2022) with a derivation from origin (2023-2024).
-- Domestic = "Italia", International = everything else.

CREATE OR REPLACE VIEW v_segment_origin AS
SELECT
    year,
    province,
    month,
    origin,
    -- Derive macro group when origin_macro is absent (2023-2024 source format)
    COALESCE(
        origin_macro,
        CASE WHEN LOWER(origin) = 'italia' THEN 'Domestico' ELSE 'Internazionale' END
    ) AS origin_group,
    SUM(arrivals) AS total_arrivals,
    SUM(nights)   AS total_nights
FROM stg_tourism_flows
WHERE province IS NOT NULL
GROUP BY year, province, month, origin, origin_macro;
