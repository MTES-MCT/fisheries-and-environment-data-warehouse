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
mission_units AS (
    SELECT
        mcu.mission_id,
        arrayStringConcat(groupArray(cu.name), ', ') AS unit_names
    FROM monitorenv_proxy.missions_control_units mcu
    INNER JOIN monitorenv_proxy.control_units cu ON cu.id = mcu.control_unit_id
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
    -- TODO même limitation que fact_mission_ulam : à rebrancher sur
    -- dim_unit_reference_by_id/_by_name une fois le schéma confirmé.
    '' AS facade,
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
