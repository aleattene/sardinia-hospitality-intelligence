-- View: v_segment_origin_summary
-- Tourist origin segmentation aggregated to year × province × origin_group.
-- Derived from v_segment_origin; collapses month and individual origin granularity.
-- Use this view for Looker Studio / Google Sheets export: replaces the high-cardinality
-- v_segment_origin (~2M rows) which cannot be loaded directly into a spreadsheet.

CREATE OR REPLACE VIEW v_segment_origin_summary AS
SELECT
    year,
    province,
    origin_group,
    SUM(total_arrivals) AS total_arrivals,
    SUM(total_nights)   AS total_nights
FROM v_segment_origin
GROUP BY year, province, origin_group;
