SELECT
    cfr,
    COALESCE(NULLIF(gears, 'null'), '{}'::JSONB) AS gears,
    COALESCE(NULLIF(species, 'null'), '{}'::JSONB) AS species,
    COALESCE(NULLIF(fao_areas, 'null'), '{}'::JSONB) AS fao_areas,
    COALESCE(NULLIF(segments, 'null'), '{}'::JSONB) AS segments,
    COALESCE(NULLIF(landing_ports, 'null'), '{}'::JSONB) AS landing_ports,
    COALESCE(NULLIF(recent_gears, 'null'), '{}'::JSONB) AS recent_gears,
    COALESCE(NULLIF(recent_species, 'null'), '{}'::JSONB) AS recent_species,
    COALESCE(NULLIF(recent_fao_areas, 'null'), '{}'::JSONB) AS recent_fao_areas,
    COALESCE(NULLIF(recent_segments, 'null'), '{}'::JSONB) AS recent_segments,
    COALESCE(NULLIF(recent_landing_ports, 'null'), '{}'::JSONB) AS recent_landing_ports,
    latest_landing_port,
    latest_landing_facade,
    COALESCE(NULLIF(recent_gear_onboard, 'null'), '[]'::JSONB) AS recent_gear_onboard,
    COALESCE(NULLIF(gear_onboard, 'null'), '[]'::JSONB) AS gear_onboard
FROM vessel_profiles