CREATE TABLE IF NOT EXISTS sacrois.navires_mois_marees_jour (
    date_traitement_sacrois DateTime,
    datetime_ref DateTime,
    cfr_cod LowCardinality(String),
    maree_id INTEGER,
    datetime_deb DateTime,
    date_seq DateTime,
    pavillon LowCardinality(Nullable(String)),
    origine LowCardinality(String),
    navs_cod LowCardinality(Nullable(String)),
    lieu_ref LowCardinality(String),
    lieu_lib_ref LowCardinality(String),
    departement_cod_ref LowCardinality(String),
    region_lib_ref LowCardinality(String),
    jour_seq_origine LowCardinality(Nullable(String)),
    engin_cod LowCardinality(String),
    engin_lib LowCardinality(String),
    engin_origine LowCardinality(Nullable(String)),
    maillage Nullable(Float64),
    maillage_origine LowCardinality(Nullable(String)),
    dimension LowCardinality(Nullable(String)),
    dimension_origine LowCardinality(Nullable(String)),
    metier_cod LowCardinality(Nullable(String)),
    metier_lib LowCardinality(Nullable(String)),
    metier_qualite LowCardinality(Nullable(String)),
    metier_dcf_5_cod LowCardinality(Nullable(String)),
    metier_dcf_5_lib LowCardinality(Nullable(String)),
    metier_dcf_6_cod LowCardinality(Nullable(String)),
    metier_dcf_6_lib LowCardinality(Nullable(String)),
    zone_cod_sipa LowCardinality(Nullable(String)),
    div_ciem_cod_sipa LowCardinality(Nullable(String)),
    sous_div_ciem_cod LowCardinality(Nullable(String)),
    rect_cod_sipa LowCardinality(Nullable(String)),
    zone_sipa_origine LowCardinality(Nullable(String)),
    zone_regl_cod LowCardinality(Nullable(String)),
    zee_cod LowCardinality(Nullable(String)),
    gradient LowCardinality(Nullable(String)),
    zpt_cod LowCardinality(Nullable(String)),
    zpt_cod_origine LowCardinality(Nullable(String)),
    tp_navire_vms Nullable(Float64),
    tp_navire_ref_vms Nullable(Float64),
    tp_navire_ref_sipa Nullable(Float64),
    tp_navire_ref_sacrois Nullable(Float64),
    tp_engin_sipa Nullable(Float64),
    origine_esp_cod_fao LowCardinality(Nullable(String)),
    esp_cod_fao LowCardinality(Nullable(String)),
    esp_lib_fao_francais LowCardinality(Nullable(String)),
    etat_cod LowCardinality(Nullable(String)),
    presentation_cod LowCardinality(Nullable(String)),
    coeff_conv Nullable(Float64),
    hors_criee LowCardinality(String),
    origine_quant_poids_vif LowCardinality(Nullable(String)),
    quant_poids_vif_ref_vms Nullable(Float64),
    quant_poids_vif_ref_sipa Nullable(Float64),
    quant_poids_vif_ref_sacrois Nullable(Float64),
    origine_montant_euros LowCardinality(Nullable(String)),
    montant_euros_ref_vms Nullable(Float64),
    montant_euros_ref_sipa Nullable(Float64),
    montant_euros_ref_sacrois Nullable(Float64),
    maree_validee_patterns INTEGER,
)
ENGINE MergeTree()
PARTITION BY toYYYYMM(date_traitement_sacrois)
PRIMARY KEY (toStartOfMonth(datetime_ref), cfr_cod)
ORDER BY (toStartOfMonth(datetime_ref), cfr_cod, datetime_ref, date_seq, engin_cod)