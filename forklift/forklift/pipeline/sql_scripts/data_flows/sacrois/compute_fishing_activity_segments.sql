INSERT INTO sacrois.segmented_fishing_activity

WITH catches_main_type AS (
    SELECT
        a.*,
        v.vessel_type AS VESSEL_TYPE,
        CASE
            WHEN (
                SUM(CASE WHEN s.scip_species_type = 'PELAGIC' THEN QUANTITY ELSE 0 END) OVER (PARTITION BY TRIP_ID) >
                SUM(CASE WHEN s.scip_species_type = 'DEMERSAL' THEN QUANTITY ELSE 0 END) OVER (PARTITION BY TRIP_ID)
            ) THEN 'PELAGIC'
            ELSE 'DEMERSAL'
        END AS MAIN_SCIP_SPECIES_TYPE
    FROM sacrois.fishing_activity a
    LEFT JOIN monitorfish.vessels v ON v.cfr = a.VESSEL_ID
    LEFT JOIN monitorfish.species s ON s.species_code = a.SPECIES
    WHERE
        PROCESSING_DATE = {processing_date:Date} AND
        TRIP_ID BETWEEN {id_min:Integer} AND {id_max:Integer}
),

efca_segments AS (
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
        year = {segments_year:Integer}
),

segmented_catches AS (
    SELECT
        ID,
        s.segment,
        s.priority AS priority,
        COALESCE(
            (
                SUM(
                    CASE WHEN
                        has(s.target_species, c.SPECIES) OR
                        s.target_species = []
                    THEN
                        QUANTITY
                    ELSE
                        0
                    END
                )
                OVER (PARTITION BY TRIP_ID, s.segment)
            ) / nullIf(
                SUM(QUANTITY) OVER (PARTITION BY TRIP_ID, s.segment),
                0
            ),
            0
        ) AS share_of_target_species,
        (
            SUM(CASE WHEN has(s.target_species, c.SPECIES) THEN 1 ELSE 0 END)
            OVER (PARTITION BY TRIP_ID, s.segment)
        ) > 0 AS has_target_species
    FROM catches_main_type c
    JOIN efca_segments s
    ON
        (has(s.gears, c.GEAR) OR s.gears = '[]')
        AND (s.main_scip_species_type = c.MAIN_SCIP_SPECIES_TYPE OR s.main_scip_species_type IS NULL)
        AND (c.MESH >= s.min_mesh OR s.min_mesh IS NULL)
        AND (c.MESH < s.max_mesh OR s.max_mesh IS NULL)
        AND (startsWith(c.AREA, s.fao_area) OR s.fao_area IS NULL)
        AND (has(s.vessel_types, c.VESSEL_TYPE) OR s.vessel_types = [])
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
    SELECT DISTINCT ON (ID)
        ID,
        segment,
        priority
    FROM segmented_catches c
    ORDER BY ID, priority DESC
)

SELECT
    c.* EXCEPT (VESSEL_TYPE, MAIN_SCIP_SPECIES_TYPE),
    COALESCE(s.segment, 'NO_SEGMENT') AS FLEET
FROM catches_main_type c
LEFT JOIN catches_top_priority_segment s
ON c.ID = s.ID
ORDER BY c.ID