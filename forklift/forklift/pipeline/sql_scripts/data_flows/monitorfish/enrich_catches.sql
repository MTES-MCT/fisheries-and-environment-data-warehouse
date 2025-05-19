INSERT INTO monitorfish.enriched_catches

WITH catches_main_type AS (
    SELECT
        c.id AS id,
        c.report_id,
        c.cfr AS cfr,
        c.flag_state AS flag_state,
        c.trip_number,
        c.latitude,
        c.longitude,
        c.far_datetime_utc,
        c.fao_area,
        c.statistical_rectangle,
        c.economic_zone,
        c.gear,
        c.mesh,
        c.species,
        c.weight,
        v.vessel_type,
        CASE
            WHEN (
                SUM(CASE WHEN s.scip_species_type = 'PELAGIC' THEN weight ELSE 0 END) OVER (PARTITION BY c.cfr, trip_number) >
                SUM(CASE WHEN s.scip_species_type = 'DEMERSAL' THEN weight ELSE 0 END) OVER (PARTITION BY c.cfr, trip_number)
            ) THEN 'PELAGIC'
            ELSE 'DEMERSAL'
        END AS main_scip_species_type
    FROM monitorfish.catches c
    LEFT JOIN monitorfish.vessels v ON v.cfr = c.cfr
    LEFT JOIN monitorfish.species s ON s.species_code = c.species
    WHERE
        toISOYear(far_datetime_utc) = {catch_year:Integer} AND
        c.cfr >= {cfr_start:String} AND
        c.cfr <= {cfr_end:String}
),

trips_landings AS (
    SELECT DISTINCT ON (cfr, trip_number)
        cfr,
        trip_number,
        port_locode,
        port_name,
        port_latitude,
        port_longitude,
        nearby_position_at_sea_longitude,
        nearby_position_at_sea_latitude,
        p.country_code_iso2 AS landing_country_code_iso2,
        p.facade AS landing_facade,
        p.region AS landing_region,
        landing_datetime_utc
    FROM monitorfish.landings l
    LEFT JOIN monitorfish.ports p
    ON p.locode = l.port_locode
    WHERE
        toISOYear(landing_datetime_utc) = {catch_year:Integer} AND
        cfr >= {cfr_start:String} AND
        cfr <= {cfr_end:String}
),

segments AS (
    SELECT
        segment,
        gears,
        arrayJoin(CASE WHEN empty(fao_areas) OR fao_areas IS NULL THEN [''] ELSE fao_areas END) as fao_area,
        target_species,
        min_share_of_target_species,
        main_scip_species_type,
        COALESCE(min_mesh, -1.0) AS min_mesh,
        COALESCE(max_mesh, 1000.0) AS max_mesh,
        priority,
        vessel_types
    FROM monitorfish.fleet_segments
    WHERE
        year = {catch_year:Integer}
),

segments_current_year AS (
    SELECT
        segment,
        gears,
        arrayJoin(CASE WHEN empty(fao_areas) OR fao_areas IS NULL THEN [''] ELSE fao_areas END) as fao_area,
        target_species,
        min_share_of_target_species,
        main_scip_species_type,
        COALESCE(min_mesh, -1.0) AS min_mesh,
        COALESCE(max_mesh, 1000.0) AS max_mesh,
        priority,
        vessel_types
    FROM monitorfish.fleet_segments
    WHERE
        year = {current_year:Integer}
),

segmented_catches AS (
    SELECT
        id,
        s.segment,
        s.priority AS priority,
        COALESCE(
            (
                SUM(
                    CASE WHEN
                        has(s.target_species, c.species) OR
                        s.target_species = []
                    THEN
                        weight
                    ELSE
                        0
                    END
                )
                OVER (PARTITION BY cfr, trip_number, s.segment)
            ) / nullIf(
                SUM(weight) OVER (PARTITION BY cfr, trip_number, s.segment),
                0
            ),
            0
        ) AS share_of_target_species,
        (
            SUM(CASE WHEN has(s.target_species, c.species) THEN 1 ELSE 0 END)
            OVER (PARTITION BY cfr, trip_number, s.segment)
        ) > 0 AS has_target_species
    FROM catches_main_type c
    JOIN segments s
    ON
        (s.main_scip_species_type = c.main_scip_species_type OR s.main_scip_species_type IS NULL)
        AND startsWith(c.fao_area, s.fao_area)
        AND COALESCE(c.mesh, 0) >= s.min_mesh
        AND COALESCE(c.mesh, 0) < s.max_mesh
    WHERE
        (has(s.gears, c.gear) OR s.gears = '[]')
        AND (has(s.vessel_types, c.vessel_type) OR s.vessel_types = [])
    QUALIFY (
        (
            has_target_species AND
            share_of_target_species >= s.min_share_of_target_species
        ) OR
        s.min_share_of_target_species IS NULL OR
        s.target_species = []
    )
),

segmented_catches_current_year AS (
    SELECT
        id,
        s.segment,
        s.priority AS priority,
        COALESCE(
            (
                SUM(
                    CASE WHEN
                        has(s.target_species, c.species) OR
                        s.target_species = []
                    THEN
                        weight
                    ELSE
                        0
                    END
                )
                OVER (PARTITION BY cfr, trip_number, s.segment)
            ) / nullIf(
                SUM(weight) OVER (PARTITION BY cfr, trip_number, s.segment),
                0
            ),
            0
        ) AS share_of_target_species,
        (
            SUM(CASE WHEN has(s.target_species, c.species) THEN 1 ELSE 0 END)
            OVER (PARTITION BY cfr, trip_number, s.segment)
        ) > 0 AS has_target_species
    FROM catches_main_type c
    JOIN segments_current_year s
    ON
        (s.main_scip_species_type = c.main_scip_species_type OR s.main_scip_species_type IS NULL)
        AND startsWith(c.fao_area, s.fao_area)
        AND COALESCE(c.mesh, 0) >= s.min_mesh
        AND COALESCE(c.mesh, 0) < s.max_mesh
    WHERE
        (has(s.gears, c.gear) OR s.gears = '[]')
        AND (has(s.vessel_types, c.vessel_type) OR s.vessel_types = [])
    QUALIFY (
        (
            has_target_species AND
            share_of_target_species >= s.min_share_of_target_species
        ) OR
        s.min_share_of_target_species IS NULL OR
        s.target_species = []
    )
),

catches_top_priority_segment AS (
    SELECT DISTINCT ON (id)
        id,
        segment,
        priority
    FROM segmented_catches c
    ORDER BY id, priority DESC
),

catches_top_priority_segment_current_year AS (
    SELECT DISTINCT ON (id)
        id,
        segment,
        priority
    FROM segmented_catches_current_year c
    ORDER BY id, priority DESC
)

SELECT
    c.id,
    c.report_id,
    c.cfr,
    c.flag_state,
    c.trip_number,
    COALESCE(
        c.latitude,
        p.vms_latitude,
        (r.north + r.south) / 2,
        l.nearby_position_at_sea_latitude
    ) AS latitude,
    COALESCE(
        c.longitude,
        p.vms_longitude,
        (r.east + r.west) / 2,
        l.nearby_position_at_sea_longitude
    ) AS longitude,
    CASE
        WHEN c.latitude IS NOT NULL AND c.longitude IS NOT NULL THEN 'LOGBOOK'
        WHEN p.vms_latitude IS NOT NULL AND p.vms_longitude IS NOT NULL THEN 'VMS'
        WHEN r.north IS NOT NULL AND r.south IS NOT NULL THEN 'ICES_SR'
        WHEN l.nearby_position_at_sea_latitude IS NOT NULL AND l.nearby_position_at_sea_longitude IS NOT NULL THEN 'PORT'
        ELSE 'NONE'
    END AS position_source,
    c.far_datetime_utc,
    COALESCE(
        CASE
            WHEN c.latitude IS NOT NULL AND c.longitude IS NOT NULL THEN dictGetOrNull(monitorfish.facade_areas_dict, 'facade', (c.longitude, c.latitude)::Point)
            WHEN p.vms_latitude IS NOT NULL AND p.vms_longitude IS NOT NULL THEN dictGetOrNull(monitorfish.facade_areas_dict, 'facade', (p.vms_longitude, p.vms_latitude)::Point)
            WHEN r.facade IS NOT NULL THEN r.facade
            WHEN l.landing_facade IS NOT NULL THEN l.landing_facade
            ELSE 'Hors façade'
        END,
        'Hors façade'
    ) AS facade,
    c.fao_area,
    c.statistical_rectangle,
    c.economic_zone,
    c.gear,
    c.mesh,
    c.species,
    c.weight,
    main_scip_species_type AS trip_main_catch_type,
    COALESCE(s.segment, 'NO_SEGMENT') AS segment,
    COALESCE(scy.segment, 'NO_SEGMENT') AS segment_current_year,
    landing_datetime_utc,
    l.port_locode AS landing_port_locode,
    l.port_name AS landing_port_name,
    l.port_latitude AS landing_port_latitude,
    l.port_longitude AS landing_port_longitude,
    landing_country_code_iso2,
    COALESCE(landing_facade, 'Hors façade') AS landing_facade,
    landing_region
FROM catches_main_type c
LEFT JOIN catches_top_priority_segment s
ON c.id = s.id
LEFT JOIN catches_top_priority_segment_current_year scy
ON c.id = scy.id
LEFT JOIN monitorfish.catches_positions p
ON
    p.catch_year = {catch_year:Integer} AND
    p.report_id = c.report_id
LEFT JOIN monitorfish.rectangles_stat_areas r
ON r.icesname = c.statistical_rectangle
LEFT JOIN trips_landings l
ON
    l.cfr = c.cfr AND
    l.trip_number = c.trip_number

SETTINGS join_use_nulls=1