CREATE TABLE IF NOT EXISTS rapportnav.aem (
    id Int32,
    idUUID Nullable(String),
    serviceId Nullable(Int32),
    missionTypes Array(String),
    facade String,
    startDateTimeUtc DateTime,
    endDateTimeUtc DateTime
)
ENGINE = MergeTree()
ORDER BY id