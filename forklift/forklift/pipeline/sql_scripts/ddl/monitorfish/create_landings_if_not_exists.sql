CREATE TABLE IF NOT EXISTS monitorfish.landings (
    report_id String,
    cfr LowCardinality(String),
    flag_state LowCardinality(String),
    trip_number String,
    port_locode LowCardinality(String),
    port_name LowCardinality(Nullable(String)),
    port_latitude Nullable(Float64),
    port_longitude Nullable(Float64),
    country_code_iso2 LowCardinality(Nullable(String)),
    facade LowCardinality(String),
    region LowCardinality(Nullable(String)),
    operation_datetime_utc DateTime,
    landing_datetime_utc DateTime,
    fao_area LowCardinality(String),
    statistical_rectangle LowCardinality(Nullable(String)),
    economic_zone LowCardinality(Nullable(String)),
    species LowCardinality(String),
    weight Float64,
    nb_fish Nullable(Float64),
    freshness LowCardinality(Nullable(String)),
    packaging LowCardinality(Nullable(String)),
    effort_zone LowCardinality(Nullable(String)),
    presentation LowCardinality(Nullable(String)),
    conversion_factor Nullable(Float64),
    preservation_state LowCardinality(Nullable(String))
)
ENGINE MergeTree()
PARTITION BY toYYYYMM(operation_datetime_utc)
PRIMARY KEY (toYear(landing_datetime_utc), cfr)
ORDER BY (toYear(landing_datetime_utc), cfr, landing_datetime_utc)