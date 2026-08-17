-- =====================================================================
-- Alimente rapportnav.fact_moyen_ulam (query_filepath pour la ligne
-- "fact_moyen_ulam" de sync_table_from_db_connection.csv).
-- Grain : 1 ligne par (mission, action, moyen). Voir avertissement
-- double comptage dans les commentaires ci-dessous.
-- =====================================================================
-- table unique déjà dénormalisée (unité, façade, date de début de
-- mission, type de moyen, indicateurs de durée) pour être droppable
-- directement en Metabase sans jointure.
--
-- ⚠️ PIÈGE DOUBLE COMPTAGE : si une action mobilise plusieurs moyens
-- (ex: 2 véhicules sur une même sortie), elle apparaît sur autant de
-- lignes que de moyens, chacune portant la DURÉE COMPLÈTE de l'action
-- (pas divisée). C'est le comportement correct pour "heures d'entretien
-- PAR moyen" (chaque moyen a bien mobilisé cette durée), mais un SUM(action_duration_h)
-- sur cette table sans GROUP BY resource_id ou sans filtrer nb_resources_on_action=1
-- surcompte le temps mission/action total. Pour un total d'heures par
-- action (sans double compte), utiliser fact_action_ulam.duration_h,
-- pas cette table. nb_resources_on_action est fourni pour permettre de
-- répartir la durée au prorata si besoin (action_duration_h / nb_resources_on_action).
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
mission_units AS (
    SELECT
        mcu.mission_id,
        arrayStringConcat(groupArray(cu.name), ', ') AS unit_names,
        -- Approximation : mission conjointe entre unités de façades
        -- différentes -> on ne garde que la 1ère façade trouvée (même
        -- limitation que terrain_category plus bas, résolu par moyen).
        arrayElement(groupUniqArray(uref.facade_ref), 1) AS facade
    FROM monitorenv_proxy.missions_control_units mcu
    INNER JOIN monitorenv_proxy.control_units cu ON cu.id = mcu.control_unit_id
    LEFT JOIN dim_unit_reference_by_id uref ON uref.control_unit_id = cu.id
    GROUP BY mcu.mission_id
),
-- Nb de moyens mobilisés par action, pour permettre une répartition au
-- prorata côté Metabase si besoin (cf. avertissement ci-dessus).
action_resource_count AS (
    SELECT
        action_id,
        COUNT(*) AS nb_resources_on_action
    FROM rapportnav_proxy.mission_action_resource
    GROUP BY action_id
)

SELECT
    ma.mission_id AS mission_id,
    coalesce(mu.unit_names, '') AS unit_names,
    coalesce(mu.facade, '') AS facade,
    toDateTime64(envm.start_datetime_utc, 6) AS mission_start_datetime_utc,
    toString(ma.id) AS action_id,
    toString(ma.action_type) AS action_type,
    toString(multiIf(
        ma.action_type = 'TRAINING', coalesce(ma.training_type, ''),
        ma.action_type = 'UNIT_MANAGEMENT_TRAINING', coalesce(ma.unit_management_training_type, ''),
        ma.action_type = 'RESOURCES_MAINTENANCE', coalesce(ma.resource_type, ''),
        coalesce(ma.reason, '')
    )) AS action_subtype,
    toDateTime64(ma.start_datetime_utc, 6) AS action_start_datetime_utc,
    toDateTime64(ma.end_datetime_utc, 6) AS action_end_datetime_utc,
    toFloat64(if(
        ma.end_datetime_utc IS NOT NULL AND ma.end_datetime_utc >= ma.start_datetime_utc,
        dateDiff('second', ma.start_datetime_utc, ma.end_datetime_utc) / 3600.0,
        coalesce(toFloat64(ma.nbr_of_hours), 0)
    )) AS action_duration_h,
    toUInt16(coalesce(arc.nb_resources_on_action, 0)) AS nb_resources_on_action,
    mar.resource_id AS resource_id,
    coalesce(cur.name, '') AS resource_name,
    toString(coalesce(cur.type, '')) AS resource_type_raw,
    toString(multiIf(
        cur.type IN ('AIRPLANE', 'HELICOPTER', 'DRONE'), 'AIR',
        cur.type IN ('CAR', 'MOTORCYCLE', 'PEDESTRIAN', 'EQUESTRIAN'), 'TERRE',
        cur.type IN (
            'BARGE', 'FAST_BOAT', 'FRIGATE', 'HYDROGRAPHIC_SHIP', 'KAYAK',
            'LIGHT_FAST_BOAT', 'MINE_DIVER', 'NET_LIFTER', 'PATROL_BOAT',
            'PIROGUE', 'RIGID_HULL', 'SEA_SCOOTER', 'SEMI_RIGID',
            'SUPPORT_SHIP', 'TRAINING_SHIP', 'TUGBOAT'
        ), 'MER',
        'AUTRE'
    )) AS terrain_category,
    toUInt8(ma.action_type = 'RESOURCES_MAINTENANCE') AS is_maintenance,
    toUInt8(ma.action_type = 'TRAINING') AS is_training,
    now() AS updated_at
FROM rapportnav_proxy.mission_action_resource mar
INNER JOIN rapportnav_proxy.mission_action ma ON ma.id = mar.action_id
INNER JOIN monitorenv_proxy.missions envm      ON envm.id = ma.mission_id
LEFT JOIN mission_units mu                     ON mu.mission_id = ma.mission_id
LEFT JOIN monitorenv_proxy.control_unit_resources cur ON cur.id = mar.resource_id
LEFT JOIN action_resource_count arc            ON arc.action_id = mar.action_id
-- STATUS n'a jamais de resource_id associé (marqueurs nav ANCHORED/NAVIGATING),
-- donc ce filtre est surtout défensif / documentaire ici.
WHERE ma.action_type != 'STATUS';
