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
    --   2023-2024: no origin_macro column; domestic = known Italian regions listed below
    CASE
        WHEN origin_macro = 'Italiani'
          OR origin IN (
            'Piemonte', 'Valle d''Aosta', 'Lombardia', 'Veneto',
            'Friuli-Venezia Giulia', 'Liguria', 'Emilia Romagna',
            'Toscana', 'Umbria', 'Marche', 'Lazio', 'Abruzzo',
            'Molise', 'Campania', 'Puglia', 'Basilicata', 'Calabria',
            'Sicilia', 'Sardegna', 'Bolzano', 'Trento', 'Italia'
        )
        THEN 'Domestico'
        ELSE 'Internazionale'
    END AS origin_group,
    SUM(arrivals) AS total_arrivals,
    SUM(nights)   AS total_nights
FROM stg_tourism_flows
WHERE province IS NOT NULL
GROUP BY year, province, month, origin, origin_macro;
