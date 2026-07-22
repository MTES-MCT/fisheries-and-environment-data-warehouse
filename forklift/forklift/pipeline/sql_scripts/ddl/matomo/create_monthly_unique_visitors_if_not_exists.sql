CREATE TABLE IF NOT EXISTS matomo.monthly_unique_visitors (
    application LowCardinality(String),
    month DateTime,
    unique_visitors Integer
)
ENGINE MergeTree()
PARTITION BY application
PRIMARY KEY (application, month)