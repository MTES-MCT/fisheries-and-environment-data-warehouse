CREATE TABLE IF NOT EXISTS sacrois.fishing_activity (
    date_traitement_sacrois DateTime,
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
)
ENGINE MergeTree()
PARTITION BY toYYYYMM(date_traitement_sacrois)
PRIMARY KEY (toStartOfMonth(CATCH_DATE), VESSEL_ID)
ORDER BY (toStartOfMonth(CATCH_DATE), VESSEL_ID, CATCH_DATE, GEAR)