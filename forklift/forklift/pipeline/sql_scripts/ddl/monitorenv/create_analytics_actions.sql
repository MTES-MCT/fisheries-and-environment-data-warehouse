CREATE TABLE {database:Identifier}.{table:Identifier} (
    id UUID,
    mission_id Int32,
    action_start_datetime_utc DateTime,
    action_end_datetime_utc DateTime,
    year Int32,
    mission_start_datetime_utc DateTime,
    mission_end_datetime_utc DateTime,
    mission_type LowCardinality(String),
    action_type LowCardinality(String),
    mission_facade LowCardinality(String),
    control_unit LowCardinality(String),
    administration LowCardinality(String),
    is_aff_mar UInt8,
    is_aem UInt8,
    administration_aem LowCardinality(String),
    action_facade LowCardinality(String),
    action_department LowCardinality(String),
    theme_level_1 LowCardinality(String),
    theme_level_2 LowCardinality(String),
    longitude Float64,
    latitude Float64,
    infraction UInt8,
    number_of_controls Float64,
    surveillance_duration Float64,
    observations_cacem Nullable(String)
)
ENGINE MergeTree
ORDER BY action_start_datetime_utc
PARTITION BY year