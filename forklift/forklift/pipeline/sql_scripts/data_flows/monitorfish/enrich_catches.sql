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
        toISOYear(far_datetime_utc) = {far_datetime_year:Integer} AND
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
        landing_datetime_utc
    FROM monitorfish.landings
    WHERE
        toISOYear(landing_datetime_utc) = {far_datetime_year:Integer} AND
        cfr >= {cfr_start:String} AND
        cfr <= {cfr_end:String}
),

segments AS (
    SELECT
        segment,
        gears,
        arrayJoin(CASE WHEN empty(fao_areas) THEN [NULL] ELSE fao_areas END) as fao_area,
        target_species,
        min_share_of_target_species,
        main_scip_species_type,
        min_mesh,
        max_mesh,
        priority,
        vessel_types
    FROM monitorfish.fleet_segments
    WHERE
        year = {far_datetime_year:Integer}
),

segments_current_year AS (
    SELECT
        segment,
        gears,
        arrayJoin(CASE WHEN empty(fao_areas) THEN [NULL] ELSE fao_areas END) as fao_area,
        target_species,
        min_share_of_target_species,
        main_scip_species_type,
        min_mesh,
        max_mesh,
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
        (has(s.gears, c.gear) OR s.gears = '[]')
        AND (s.main_scip_species_type = c.main_scip_species_type OR s.main_scip_species_type IS NULL)
        AND (c.mesh >= s.min_mesh OR s.min_mesh IS NULL)
        AND (c.mesh < s.max_mesh OR s.max_mesh IS NULL)
        AND (startsWith(c.fao_area, s.fao_area) OR s.fao_area IS NULL)
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
        (has(s.gears, c.gear) OR s.gears = '[]')
        AND (s.main_scip_species_type = c.main_scip_species_type OR s.main_scip_species_type IS NULL)
        AND (c.mesh >= s.min_mesh OR s.min_mesh IS NULL)
        AND (c.mesh < s.max_mesh OR s.max_mesh IS NULL)
        AND (startsWith(c.fao_area, s.fao_area) OR s.fao_area IS NULL)
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
    c.* EXCEPT (vessel_type, main_scip_species_type),
    main_scip_species_type AS trip_main_catch_type,
    COALESCE(s.segment, 'NO_SEGMENT') AS segment,
    COALESCE(scy.segment, 'NO_SEGMENT') AS segment_current_year,
    landing_datetime_utc,
    l.port_locode AS landing_port_locode,
    l.port_name AS landing_port_name,
    l.port_latitude AS landing_port_latitude,
    l.port_longitude AS landing_port_longitude
FROM catches_main_type c
LEFT JOIN catches_top_priority_segment s
ON c.id = s.id
LEFT JOIN catches_top_priority_segment_current_year scy
ON c.id = scy.id
LEFT JOIN trips_landings l
ON
    l.cfr = c.cfr AND
    l.trip_number = c.trip_number