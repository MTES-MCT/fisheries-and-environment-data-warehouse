-- =====================================================================
-- Alimente rapportnav.fact_moyen_pam_ulam (query_filepath pour la ligne
-- "fact_moyen_pam_ulam" de sync_table_from_db_connection.csv).
-- Grain : 1 ligne par (mission, action, moyen). Voir avertissement
-- double comptage dans les commentaires ci-dessous.
-- Couvre les unités PAM ET ULAM dans une seule table (cf.
-- pam_ulam_control_units plus bas) -- unit_type distingue les deux, cf.
-- discussion en chat (une table partagée plutôt que 2 jeux dupliqués).
-- ⚠️ Ce fichier DOIT tourner après dim_unit_reference.sql dans
-- sync_table_from_db_connection.csv (aucune dépendance native entre
-- lignes de ce flow -- cf. commentaire détaillé dans dim_unit_reference.sql).
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
-- action (sans double compte), utiliser fact_action_pam_ulam.duration_h,
-- pas cette table. nb_resources_on_action est fourni pour permettre de
-- répartir la durée au prorata si besoin (action_duration_h / nb_resources_on_action).
-- =====================================================================
WITH
-- Filtre unités PAM + ULAM (même logique que les 2 autres requêtes) :
-- service_type via service_control_unit, repli sur le nom si le lien
-- n'est pas renseigné -- constaté non peuplé en pratique (aucune fixture
-- de test ne renseigne service_control_unit), donc le repli par nom est
-- la voie principale, pas un simple filet de sécurité.
pam_ulam_control_units AS (
    SELECT DISTINCT cu.id AS control_unit_id
    FROM monitorenv_proxy.control_units cu
    LEFT JOIN rapportnav_proxy.service_control_unit scu ON scu.control_unit_id = cu.id
    LEFT JOIN rapportnav_proxy.service s ON s.id = scu.service_id AND s.deleted_at IS NULL
    WHERE s.service_type IN ('PAM', 'ULAM')
       OR startsWith(upper(cu.name), 'ULAM')
       OR startsWith(upper(cu.name), 'PAM')
),
-- INNER JOIN sur pam_ulam_control_units : filtre les moyens dont la
-- mission n'a aucune unité PAM ni ULAM associée.
mission_units AS (
    SELECT
        mcu.mission_id,
        arrayStringConcat(groupArray(cu.name), ', ') AS unit_names,
        -- Approximation : mission conjointe entre unités de façades
        -- différentes -> on ne garde que la 1ère façade trouvée (même
        -- limitation que terrain_category plus bas, résolu par moyen).
        arrayElement(groupUniqArray(uref.facade_ref), 1) AS facade,
        -- unit_type : priorité à rapportnav.dim_unit_reference (référentiel
        -- unique), repli sur le nom en direct pour toute unité PAM/ULAM pas
        -- encore ajoutée à ce référentiel manuel (cf. pam_ulam_control_units
        -- ci-dessus, même filtre par nom). Même approximation "1er trouvé"
        -- que facade.
        arrayElement(groupUniqArray(coalesce(nullIf(uref.unit_type, ''), multiIf(
            startsWith(upper(cu.name), 'PAM'), 'PAM',
            startsWith(upper(cu.name), 'ULAM'), 'ULAM',
            'AUTRE'
        ))), 1) AS unit_type
    FROM monitorenv_proxy.missions_control_units mcu
    INNER JOIN monitorenv_proxy.control_units cu ON cu.id = mcu.control_unit_id
    INNER JOIN pam_ulam_control_units uu ON uu.control_unit_id = cu.id
    LEFT JOIN rapportnav.dim_unit_reference uref ON uref.control_unit_id = cu.id
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
    toString(coalesce(mu.unit_type, '')) AS unit_type,
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
-- INNER JOIN (pas LEFT) : filtre aux moyens dont la mission a au moins
-- une unité PAM ou ULAM (cf. pam_ulam_control_units plus haut).
INNER JOIN mission_units mu                    ON mu.mission_id = ma.mission_id
LEFT JOIN monitorenv_proxy.control_unit_resources cur ON cur.id = mar.resource_id
LEFT JOIN action_resource_count arc            ON arc.action_id = mar.action_id
-- STATUS n'a jamais de resource_id associé (marqueurs nav ANCHORED/NAVIGATING),
-- donc ce filtre est surtout défensif / documentaire ici.
WHERE ma.action_type != 'STATUS';
