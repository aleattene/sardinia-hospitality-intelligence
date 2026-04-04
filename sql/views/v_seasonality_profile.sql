-- View: v_seasonality_profile
-- Monthly demand profile by province and accommodation type.
-- month_share: fraction of annual nights concentrated in each month.
-- Used to identify provinces with extreme vs. distributed seasonality.

CREATE OR REPLACE VIEW v_seasonality_profile AS
WITH annual_totals AS (
    SELECT
        year,
        province,
        accommodation_type,
        SUM(total_nights) AS annual_nights
    FROM (
        SELECT
            year,
            province,
            month,
            accommodation_type,
            SUM(nights) AS total_nights
        FROM stg_tourism_flows
        WHERE province IS NOT NULL
        GROUP BY year, province, month, accommodation_type
    )
    GROUP BY year, province, accommodation_type
),
monthly AS (
    SELECT
        year,
        province,
        month,
        accommodation_type,
        SUM(nights) AS monthly_nights
    FROM stg_tourism_flows
    WHERE province IS NOT NULL
    GROUP BY year, province, month, accommodation_type
)
SELECT
    m.year,
    m.province,
    m.month,
    m.accommodation_type,
    m.monthly_nights,
    a.annual_nights,
    ROUND(
        CASE WHEN a.annual_nights > 0
             THEN m.monthly_nights::DOUBLE / a.annual_nights
             ELSE NULL END,
        4
    ) AS month_share
FROM monthly m
JOIN annual_totals a
    ON m.year = a.year
    AND m.province = a.province
    AND m.accommodation_type = a.accommodation_type;
