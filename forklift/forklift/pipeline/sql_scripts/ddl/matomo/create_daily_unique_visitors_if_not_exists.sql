CREATE TABLE IF NOT EXISTS matomo.daily_unique_visitors (
    application LowCardinality(String),
    day DateTime,
    unique_visitors Integer
)
ENGINE MergeTree()
PARTITION BY application
PRIMARY KEY (application, day)