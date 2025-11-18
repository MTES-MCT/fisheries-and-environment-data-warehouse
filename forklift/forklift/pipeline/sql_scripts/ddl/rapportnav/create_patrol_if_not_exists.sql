CREATE TABLE IF NOT EXISTS rapportnav.patrol (
    id Int32,
    startDateTimeUtc DateTime
)
ENGINE MergeTree()
ORDER BY id