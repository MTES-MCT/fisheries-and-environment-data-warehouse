CREATE TABLE IF NOT EXISTS rapportnav.aem (
    id Int32,
    idUUID String,
    serviceId Int32,
    missionTypes Array(String),
    facade String,
    annee Int32,
    1_1_1_nombre_d_heures_de_mer Int32
)
ENGINE = MergeTree()
ORDER BY id