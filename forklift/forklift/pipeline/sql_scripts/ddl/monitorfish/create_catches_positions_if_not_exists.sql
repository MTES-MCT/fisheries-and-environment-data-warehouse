CREATE TABLE IF NOT EXISTS monitorfish.catches_positions (
    catch_year Integer,
    report_id String,
    far_latitude Nullable(Float64),
    far_longitude Nullable(Float64),
    vms_latitude Nullable(Float64),
    vms_longitude Nullable(Float64),
    facade LowCardinality(Nullable(String))
)
ENGINE MergeTree()
PARTITION BY catch_year
ORDER BY report_id;