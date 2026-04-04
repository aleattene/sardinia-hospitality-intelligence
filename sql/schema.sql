-- Staging tables: target schema based on 2024 source format.
-- Recreated on every pipeline run for a clean load.
-- Nullable fields are documented with their source anomaly.
-- See docs/ADR.md — ADR-002 for schema decision rationale.

CREATE OR REPLACE TABLE stg_tourism_flows (
    year               INTEGER  NOT NULL,
    province           VARCHAR,  -- NULL for 3 rows in 2022: missing in source
    month              INTEGER  NOT NULL,
    accommodation_type VARCHAR  NOT NULL,
    origin_macro       VARCHAR,  -- NULL for 2023-2024: removed from source
    origin             VARCHAR  NOT NULL,
    arrivals           INTEGER,  -- nullable: missing data possible (e.g. COVID years)
    nights             INTEGER   -- nullable: missing data possible (e.g. COVID years)
);

CREATE OR REPLACE TABLE stg_accommodation_capacity (
    year               INTEGER  NOT NULL,
    province           VARCHAR,  -- NULL for 2021: column absent in source
    municipality       VARCHAR  NOT NULL,
    month              INTEGER,  -- NULL for 2020-2024: annual granularity
    accommodation_type VARCHAR  NOT NULL,
    category           VARCHAR,  -- NULL where classification absent
    facilities         INTEGER,  -- nullable: missing data possible
    beds               INTEGER,  -- nullable: missing data possible
    rooms              INTEGER   -- nullable: missing data possible
);
