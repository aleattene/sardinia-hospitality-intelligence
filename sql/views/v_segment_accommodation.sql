-- View: v_segment_accommodation
-- Demand segmented by accommodation type per province and year.
-- Useful for comparing hotel vs. complementary structures across provinces.

CREATE OR REPLACE VIEW v_segment_accommodation AS
SELECT
    year,
    province,
    accommodation_type,
    SUM(arrivals) AS total_arrivals,
    SUM(nights)   AS total_nights,
    -- Average length of stay (nights per arrival)
    ROUND(
        CASE WHEN SUM(arrivals) > 0
             THEN SUM(nights)::DOUBLE / SUM(arrivals)
             ELSE NULL END,
        2
    ) AS avg_stay_length
FROM stg_tourism_flows
WHERE province IS NOT NULL
GROUP BY year, province, accommodation_type;
