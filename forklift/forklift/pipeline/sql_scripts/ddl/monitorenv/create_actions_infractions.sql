CREATE TABLE {database:Identifier}.{table:Identifier} (
    env_action_id UUID,
    infraction_id UUID,
    natinf Array(Integer),
    administrative_response LowCardinality(Nullable(String)),
    company_name Nullable(String),
    controlled_person_identity Nullable(String),
    formal_notice LowCardinality(Nullable(String)),
    imo Nullable(String),
    infraction_type LowCardinality(Nullable(String)),
    mmsi Nullable(String),
    nb_target Nullable(Int32),
    observations Nullable(String),
    registration_number Nullable(String),
    relevant_court LowCardinality(Nullable(String)),
    seizure LowCardinality(Nullable(String)),
    to_process Nullable(bool),
    vessel_batch_id Nullable(Int32),
    vessel_name Nullable(String),
    vessel_row_number Nullable(Int32),
    vessel_ship_id Nullable(Int32),
    vessel_size Nullable(Float64),
    vessel_type LowCardinality(Nullable(String))
)
ENGINE MergeTree
ORDER BY env_action_id
