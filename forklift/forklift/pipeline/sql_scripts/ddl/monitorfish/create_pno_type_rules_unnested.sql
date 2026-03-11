CREATE TABLE IF NOT EXISTS monitorfish.pno_type_rules_unnested (
    pno_type_id UInt8,
    pno_type_name LowCardinality(String),
    minimum_notification_period Float32,
    has_designated_ports bool,
    pno_type_rule_id UInt16,
    species LowCardinality(Nullable(String)),
    fao_area LowCardinality(Nullable(String)),
    gear LowCardinality(Nullable(String)),
    flag_state LowCardinality(Nullable(String)),
    minimum_quantity_kg Float32
)
ENGINE MergeTree()
PRIMARY KEY pno_type_id
ORDER BY pno_type_id