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
    -- Normalize origin group across schema versions:
    --   2018-2022: origin_macro = 'Italiani' (domestic) or 'Stranieri' (international)
    --   2023-2024: origin_macro is NULL (column exists in stg_tourism_flows but not populated);
    --              domestic is derived from known Italian region names in origin
    -- LOWER(TRIM(...)) guards against trailing spaces from CSV parsing and case variants
    -- across source years (ingest normalizes column names but not cell values).
    CASE
        WHEN LOWER(TRIM(origin_macro)) = 'italiani'
          OR LOWER(TRIM(origin)) IN (
            'piemonte', 'valle d''aosta', 'lombardia', 'veneto',
            'friuli-venezia giulia', 'liguria', 'emilia romagna',
            'toscana', 'umbria', 'marche', 'lazio', 'abruzzo',
            'molise', 'campania', 'puglia', 'basilicata', 'calabria',
            'sicilia', 'sardegna', 'bolzano', 'trento', 'italia'
        )
        THEN 'Domestico'
        ELSE 'Internazionale'
    END AS origin_group,
    SUM(arrivals) AS total_arrivals,
    SUM(nights)   AS total_nights
FROM stg_tourism_flows
WHERE province IS NOT NULL
GROUP BY year, province, month, origin, origin_macro;
