-- =====================================================================
-- Alimente rapportnav.fact_action_ulam (query_filepath pour la ligne
-- "fact_action_ulam" de sync_table_from_db_connection.csv).
-- =====================================================================
WITH
-- Référentiel unité VALIDÉ AEM (idem requête 2) -- pas rapportnav_proxy.service.
mission_units AS (
    SELECT
        mcu.mission_id,
        arrayStringConcat(groupArray(cu.name), ', ') AS unit_names
    FROM monitorenv_proxy.missions_control_units mcu
    INNER JOIN monitorenv_proxy.control_units cu ON cu.id = mcu.control_unit_id
    GROUP BY mcu.mission_id
),
resource_dim AS (
    SELECT
        id AS resource_id,
        type AS resource_type_raw,
        multiIf(
            type IN ('AIRPLANE', 'HELICOPTER', 'DRONE'), 'AIR',
            type IN ('CAR', 'MOTORCYCLE', 'PEDESTRIAN', 'EQUESTRIAN'), 'TERRE',
            type IN (
                'BARGE', 'FAST_BOAT', 'FRIGATE', 'HYDROGRAPHIC_SHIP', 'KAYAK',
                'LIGHT_FAST_BOAT', 'MINE_DIVER', 'NET_LIFTER', 'PATROL_BOAT',
                'PIROGUE', 'RIGID_HULL', 'SEA_SCOOTER', 'SEMI_RIGID',
                'SUPPORT_SHIP', 'TRAINING_SHIP', 'TUGBOAT'
            ), 'MER',
            'AUTRE'
        ) AS terrain_category
    FROM monitorenv_proxy.control_unit_resources
),
-- Un moyen (ou plusieurs) par action -> agrégés en tableau, une ligne par action.
action_resources AS (
    SELECT
        toString(mar.action_id) AS action_id,
        groupArray(mar.resource_id) AS resource_ids,
        groupArray(toString(rd.resource_type_raw)) AS resource_types,
        -- ⚠️ approximation : si l'action mobilise des moyens de catégories
        -- différentes (ex: un bateau + un véhicule sur la même sortie),
        -- on ne garde que le 1er trouvé. À signaler si ça arrive en pratique
        -- (cf. GROUP BY ci-dessous, arrayElement sur un groupUniqArray).
        arrayElement(groupUniqArray(rd.terrain_category), 1) AS terrain_type_first
    FROM rapportnav_proxy.mission_action_resource mar
    LEFT JOIN resource_dim rd ON rd.resource_id = mar.resource_id
    GROUP BY mar.action_id
)

SELECT
    toString(ma.id) AS action_id,
    ma.mission_id AS mission_id,
    coalesce(mu.unit_names, '') AS unit_names,
    toString(ma.action_type) AS action_type,
    toString(multiIf(
        ma.action_type = 'TRAINING', coalesce(ma.training_type, ''),
        ma.action_type = 'UNIT_MANAGEMENT_TRAINING', coalesce(ma.unit_management_training_type, ''),
        ma.action_type = 'RESOURCES_MAINTENANCE', coalesce(ma.resource_type, ''),
        coalesce(ma.reason, '')
    )) AS action_subtype,
    ma.resource_type AS resource_type_declared,
    toDateTime64(ma.start_datetime_utc, 6) AS start_datetime_utc,
    toDateTime64(ma.end_datetime_utc, 6) AS end_datetime_utc,
    toFloat64(if(
        ma.end_datetime_utc IS NOT NULL AND ma.end_datetime_utc >= ma.start_datetime_utc,
        dateDiff('second', ma.start_datetime_utc, ma.end_datetime_utc) / 3600.0,
        coalesce(toFloat64(ma.nbr_of_hours), 0)
    )) AS duration_h,
    ma.nbr_of_hours AS nbr_of_hours_declared,
    toUInt8(coalesce(ma.is_complete_for_stats, 0)) AS is_complete_for_stats,
    toUInt16(length(coalesce(ar.resource_ids, []))) AS nb_resources_linked,
    coalesce(ar.resource_ids, []) AS resource_ids,
    coalesce(ar.resource_types, []) AS resource_types,
    toString(coalesce(ar.terrain_type_first, 'INDETERMINE')) AS terrain_type,
    now() AS updated_at
FROM rapportnav_proxy.mission_action ma
LEFT JOIN mission_units mu ON mu.mission_id = ma.mission_id
LEFT JOIN action_resources ar ON ar.action_id = toString(ma.id)
-- STATUS = marqueurs de changement d'état nav (ANCHORED/NAVIGATING/...),
-- déjà exploités dans fact_mission_ulam.computed_hours_at_sea -- pas une
-- "activité" au sens métier du rapport ULAM.
WHERE ma.action_type != 'STATUS';
