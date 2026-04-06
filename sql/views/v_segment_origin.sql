-- View: v_segment_origin
-- Demand segmented by tourist origin (domestic vs. international) per province and month.
-- origin_group: derived from origin_macro (2018-2022) or from Italian region names in origin (2023-2024).
-- Domestic = origin_macro 'Italiani' OR known Italian region name in origin.
-- International = everything else.

CREATE OR REPLACE VIEW v_segment_origin AS
-- Derive origin_group before aggregating to avoid grouping on raw origin_macro,
-- which may contain case/space variants that would produce duplicate output rows
-- with the same (year, province, month, origin, origin_group) and cause double-counting downstream.
WITH classified AS (
    SELECT
        year,
        province,
        month,
        TRIM(origin) AS origin,
        -- Normalize origin group across schema versions:
        --   2018-2022: origin_macro = 'Italiani' (domestic) or 'Stranieri' (international)
        --   2023-2024: origin_macro is NULL (column exists in stg_tourism_flows but not populated);
        --              domestic is derived from known Italian region names in origin
        -- LOWER(TRIM(...)) guards against trailing spaces from CSV parsing and case variants
        -- across source years (ingest normalizes column names but not cell values).
        -- origin is also TRIM-ped in the SELECT so that grouping and downstream joins
        -- use the clean value rather than the raw cell (which may have trailing spaces).
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
        arrivals,
        nights
    FROM stg_tourism_flows
    WHERE province IS NOT NULL
)
SELECT
    year,
    province,
    month,
    origin,
    origin_group,
    SUM(arrivals) AS total_arrivals,
    SUM(nights)   AS total_nights
FROM classified
GROUP BY year, province, month, origin, origin_group;
