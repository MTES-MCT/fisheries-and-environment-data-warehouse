CREATE TABLE IF NOT EXISTS sacrois.segmented_fishing_activity (
    PROCESSING_DATE Date,
    ID Int64,
    VESSEL_ID LowCardinality(String),
    TRIP_ID Integer,
    CATCH_DATE Date,
    LANDING_DATETIME DateTime,
    GEAR LowCardinality(String),
    MESH Nullable(Float64),
    AREA LowCardinality(Nullable(String)),
    ICES_SR LowCardinality(Nullable(String)),
    EU_nEU LowCardinality(Nullable(String)),
    ZPT_COD LowCardinality(Nullable(String)),
    ZEE_COD LowCardinality(Nullable(String)),
    SPECIES LowCardinality(String),
    PORT_COUNTRY LowCardinality(Nullable(String)),
    LANDING_PORT_NAME LowCardinality(Nullable(String)),
    LANDING_PORT LowCardinality(Nullable(String)),
    QUANTITY DOUBLE,
    CATEGORY LowCardinality(String),
    TRIP_MAIN_CATCH_TYPE Enum('DEMERSAL' = 1, 'PELAGIC' = 2),
    FLEET LowCardinality(String)
)
ENGINE MergeTree()
PARTITION BY toYYYYMM(PROCESSING_DATE)
PRIMARY KEY (toStartOfMonth(CATCH_DATE), VESSEL_ID)
ORDER BY (toStartOfMonth(CATCH_DATE), VESSEL_ID, CATCH_DATE, GEAR)