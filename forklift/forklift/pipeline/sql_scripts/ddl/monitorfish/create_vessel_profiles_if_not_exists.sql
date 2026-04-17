CREATE TABLE IF NOT EXISTS monitorfish.vessel_profiles (
    cfr String,
    gears Map(String, Float),
    species Map(String, Float),
    fao_areas Map(String, Float),
    segments Map(String, Float),
    landing_ports Map(String, Float),
    recent_gears Map(String, Float),
    recent_species Map(String, Float),
    recent_fao_areas Map(String, Float),
    recent_segments Map(String, Float),
    recent_landing_ports Map(String, Float),
    latest_landing_port LowCardinality(Nullable(String)),
    latest_landing_facade LowCardinality(Nullable(String)),
    recent_gear_onboard Array(Map(String, Dynamic)),
    gear_onboard Array(Map(String, Dynamic))
)
ENGINE = MergeTree()
ORDER BY cfr 