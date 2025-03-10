CREATE TABLE {database:Identifier}.{table:Identifier} (
    id UUID,
    mission_id Int32,
    action_start_datetime_utc DateTime,
    action_end_datetime_utc Nullable(DateTime),
    year Int32,
    mission_start_datetime_utc DateTime,
    mission_end_datetime_utc DateTime,
    mission_type LowCardinality(String),
    action_type LowCardinality(String),
    mission_facade LowCardinality(String),
    control_unit_id Int32,
    control_unit LowCardinality(String),
    administration LowCardinality(String),
    is_aff_mar UInt8,
    is_aem UInt8,
    administration_aem LowCardinality(String),
    action_facade LowCardinality(String),
    action_department LowCardinality(String),
    theme_level_1 LowCardinality(String),
    theme_level_2 LowCardinality(String),
    plan LowCardinality(String),
    longitude Nullable(Float64),
    latitude Nullable(Float64),
    infraction Nullable(UInt8),
    number_of_controls Nullable(Float64),
    surveillance_duration Nullable(Float64),
    observations_cacem Nullable(String)
)
ENGINE MergeTree
ORDER BY action_start_datetime_utc
PARTITION BY year