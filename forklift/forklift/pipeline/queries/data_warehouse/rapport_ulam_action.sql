-- =====================================================================
-- Alimente rapportnav.fact_action_ulam (query_filepath pour la ligne
-- "fact_action_ulam" de sync_table_from_db_connection.csv).
-- =====================================================================
WITH
-- Référentiel façade par unité (monitorenv, control_unit_id) -- fourni
-- manuellement (pas une vraie table monitorenv), repris de la CTE
-- dim_unit_reference_by_id de missions_aem.sql. Dupliqué ici pour la
-- même raison que les autres référentiels de ce fichier (requêtes
-- indépendantes, pas de vue/macro partagée possible dans ce repo) --
-- si la liste évolue, penser à la répercuter dans les 4 fichiers
-- (missions_aem.sql + les 3 requêtes ULAM).
dim_unit_reference_by_id AS (
    SELECT 10194 AS control_unit_id, 'MED' AS facade_ref
    UNION ALL SELECT 10039, 'MED'
    UNION ALL SELECT 10452, 'MEMN'
    UNION ALL SELECT 10204, 'NAMO'
    UNION ALL SELECT 10457, 'NAMO'  -- Brest
    UNION ALL SELECT 10288, 'NAMO'  -- Douarnenez
    UNION ALL SELECT 10074, 'MED'   -- 2A
    UNION ALL SELECT 10192, 'MED'   -- 2B
    UNION ALL SELECT 10225, 'SA'
    UNION ALL SELECT 10255, 'SA'
    UNION ALL SELECT 10420, 'MED'
    UNION ALL SELECT 10176, 'NAMO'
    UNION ALL SELECT 10428, 'NAMO'
    UNION ALL SELECT 10210, 'MEMN'
    UNION ALL SELECT 10449, 'NAMO'
    UNION ALL SELECT 10050, 'MEMN'
    UNION ALL SELECT 10318, 'MEMN'
    UNION ALL SELECT 10364, 'SA'
    UNION ALL SELECT 10303, 'MED'
    UNION ALL SELECT 10423, 'MEMN'
    UNION ALL SELECT 10166, 'MED'
    UNION ALL SELECT 10171, 'NAMO'
    UNION ALL SELECT 10169, 'Antilles'
    UNION ALL SELECT 10327, 'Antilles'
    UNION ALL SELECT 10265, 'Guyane'
    UNION ALL SELECT 10183, 'Sud de l''Océan indien'
    UNION ALL SELECT 10430, 'Saint-Pierre et Miquelon'
    UNION ALL SELECT 10047, 'Sud de l''Océan indien'
    UNION ALL SELECT 10080, 'NAMO'                    -- PAM Themis
    UNION ALL SELECT 10121, 'MEMN'                    -- PAM Jeanne Barret
    UNION ALL SELECT 10141, 'MED'                     -- PAM Gyptis
    UNION ALL SELECT 10404, 'SA'                      -- PAM Iris
    UNION ALL SELECT 10345, 'Sud de l''Océan indien'  -- PAM Osiris II
    UNION ALL SELECT 10519, 'Guyane'                  -- PAM Cayenne
),
-- Filtre unités ULAM (cf. requête 2, même logique) : service_type via
-- service_control_unit, repli sur le nom si le lien n'est pas renseigné.
ulam_control_units AS (
    SELECT DISTINCT cu.id AS control_unit_id
    FROM monitorenv_proxy.control_units cu
    LEFT JOIN rapportnav_proxy.service_control_unit scu ON scu.control_unit_id = cu.id
    LEFT JOIN rapportnav_proxy.service s ON s.id = scu.service_id AND s.deleted_at IS NULL
    WHERE s.service_type = 'ULAM' OR startsWith(upper(cu.name), 'ULAM')
),
-- Référentiel unité VALIDÉ AEM (idem requête 2) -- rapportnav_proxy.service
-- sert uniquement au filtre ULAM ci-dessus, pas de référentiel concurrent.
-- INNER JOIN sur ulam_control_units : filtre les actions dont la mission
-- n'a aucune unité ULAM associée.
mission_units AS (
    SELECT
        mcu.mission_id,
        arrayStringConcat(groupArray(cu.name), ', ') AS unit_names,
        -- Approximation : mission conjointe entre unités de façades
        -- différentes -> on ne garde que la 1ère façade trouvée (même
        -- limitation que terrain_type_first plus bas).
        arrayElement(groupUniqArray(uref.facade_ref), 1) AS facade
    FROM monitorenv_proxy.missions_control_units mcu
    INNER JOIN monitorenv_proxy.control_units cu ON cu.id = mcu.control_unit_id
    INNER JOIN ulam_control_units uu ON uu.control_unit_id = cu.id
    LEFT JOIN dim_unit_reference_by_id uref ON uref.control_unit_id = cu.id
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
    coalesce(mu.facade, '') AS facade,
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
-- INNER JOIN (pas LEFT) : filtre aux actions dont la mission a au moins
-- une unité ULAM (cf. ulam_control_units plus haut).
INNER JOIN mission_units mu ON mu.mission_id = ma.mission_id
LEFT JOIN action_resources ar ON ar.action_id = toString(ma.id)
-- STATUS = marqueurs de changement d'état nav (ANCHORED/NAVIGATING/...),
-- déjà exploités dans fact_mission_ulam.computed_hours_at_sea -- pas une
-- "activité" au sens métier du rapport ULAM.
WHERE ma.action_type != 'STATUS';
