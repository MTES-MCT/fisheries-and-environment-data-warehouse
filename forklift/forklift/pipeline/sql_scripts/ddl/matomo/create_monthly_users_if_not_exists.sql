CREATE TABLE IF NOT EXISTS matomo.monthly_users (
    application LowCardinality(String),
    month DateTime,
    users Integer
)
ENGINE MergeTree()
PARTITION BY application
PRIMARY KEY (application, month)