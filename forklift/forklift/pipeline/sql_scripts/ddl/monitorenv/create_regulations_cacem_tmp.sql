CREATE TABLE {database:Identifier}.{table:Identifier} (
    id Integer,
    geom String,
    entity_name String,
    url Nullable(String),
    layer_name String,
    facade Nullable(String),
    ref_reg Nullable(String),
    edition Nullable(String),
    editeur Nullable(String),
    source Nullable(String),
    observation Nullable(String),
    thematique String,
    date Nullable(String),
    duree_validite Nullable(String),
    date_fin Nullable(String),
    temporalite Nullable(String),
    type Nullable(String)
)
ENGINE MergeTree
ORDER BY id
