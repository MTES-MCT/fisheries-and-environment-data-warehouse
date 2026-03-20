CREATE TABLE IF NOT EXISTS monitorfish.cox (
        report_id String,
        cfr LowCardinality(String),
        flag_state LowCardinality(String),
        trip_number String,
        operation_datetime_utc DateTime,
        report_document_type LowCardinality(String),
        exit_datetime_utc DateTime,
        latitude_exited Float64,
        longitude_exited Float64,
        economic_zone_exited LowCardinality(Nullable(String)),
        effort_zone_exited LowCardinality(Nullable(String)),
        fao_area_exited LowCardinality(String),
        statistical_rectangle_exited LowCardinality(Nullable(String)),
        target_species_on_exit LowCardinality(Nullable(String)),
        species LowCardinality(Nullable(String)),
        weight Nullable(Float64),
        nb_fish Nullable(Float64),
        catch_fao_area LowCardinality(Nullable(String)),
        catch_statistical_rectangle LowCardinality(Nullable(String)),
        catch_economic_zone LowCardinality(Nullable(String)),
        catch_effort_zone LowCardinality(Nullable(String)),
        presentation LowCardinality(Nullable(String)),
        packaging LowCardinality(Nullable(String)),
        freshness LowCardinality(Nullable(String)),
        preservation_state LowCardinality(Nullable(String)),
        conversion_factor Nullable(Float64)

)
ENGINE MergeTree()
PARTITION BY toYYYYMM(operation_datetime_utc)
PRIMARY KEY (toYear(exit_datetime_utc), cfr)
ORDER BY (toYear(exit_datetime_utc), cfr, exit_datetime_utc)