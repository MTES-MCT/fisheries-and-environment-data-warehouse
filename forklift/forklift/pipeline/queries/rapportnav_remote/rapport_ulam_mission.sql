-- =====================================================================
-- Alimente rapportnav.fact_mission_ulam (query_filepath pour la ligne
-- "fact_mission_ulam" de sync_table_from_db_connection.csv).
-- SELECT pur : le flow générique fait CREATE TABLE ... AS <cette requête>
-- (ddl_script_path laissé vide -> schéma inféré, cf. discussion en chat).
-- =====================================================================
WITH
-- Référentiel "unité" VALIDÉ sur le rapport AEM : monitorenv_proxy
-- missions_control_units + control_units (PAS rapportnav_proxy.service --
-- la fiche ULAM d'origine a été écrite avant le travail AEM et n'en tenait
-- pas compte). Repris tel quel de la CTE mission_units de
-- query_aem_par_mission_3_bases_clickhouse.sql.
mission_units AS (
    SELECT
        mcu.mission_id,
        arrayStringConcat(groupArray(cu.name), ', ') AS unit_names,
        COUNT(DISTINCT cu.id) AS nb_unites_distinctes,
        groupArray(cu.id) AS control_unit_ids
    FROM monitorenv_proxy.missions_control_units mcu
    INNER JOIN monitorenv_proxy.control_units cu ON cu.id = mcu.control_unit_id
    GROUP BY mcu.mission_id
),
-- ⚠️ Classification PAM/ULAM + façade : à rebrancher sur les tables
-- dim_unit_reference_by_id (ULAM, jointure sur control_unit_id) et
-- dim_unit_reference_by_name (PAM, jointure sur le nom en attendant
-- l'export control_unit_id) déjà utilisées côté AEM -- je n'ai pas pu
-- localiser ces tables dans le checkout forklift actuel pour en reprendre
-- le schéma exact ici : à COMPLÉTER avec le nom/schéma réel avant
-- d'activer cette requête (placeholder ci-dessous à remplacer).
-- unit_reference AS (
--     SELECT control_unit_id, unit_type, facade FROM rapportnav.dim_unit_reference_by_id
--     UNION ALL
--     SELECT ... FROM rapportnav.dim_unit_reference_by_name ...
-- ),

-- Heures de mer recalculées (méthode déjà validée sur le rapport AEM,
-- cf. status_actions / heures_de_mer_nav dans
-- query_aem_par_mission_3_bases_clickhouse.sql) -- à préférer à
-- mission_general_info.nb_hour_at_sea qui est une saisie manuelle agent.
status_actions AS (
    SELECT
        ma.mission_id,
        ma.status,
        ma.start_datetime_utc,
        leadInFrame(
            ma.start_datetime_utc,
            1,
            ifNull(envm.end_datetime_utc, ma.start_datetime_utc)
        ) OVER (
            PARTITION BY ma.mission_id ORDER BY ma.start_datetime_utc
        ) AS corrected_end_datetime_utc
    FROM rapportnav_proxy.mission_action ma
    INNER JOIN monitorenv_proxy.missions envm ON envm.id = ma.mission_id
    WHERE ma.action_type = 'STATUS'
),
heures_de_mer AS (
    SELECT
        mission_id,
        sumIf(dateDiff('second', start_datetime_utc, corrected_end_datetime_utc) / 3600.0,
              status IN ('ANCHORED', 'NAVIGATING')) AS computed_hours_at_sea,
        -- ⚠️ HYPOTHÈSE À VÉRIFIER : "heures moteur" ≈ heures où le statut
        -- mission est NAVIGATING seul (bateau qui avance), à l'exclusion
        -- d'ANCHORED (mouillage, moteur probablement coupé ou au ralenti).
        -- Personne n'a confirmé ce mapping côté métier -- à valider avant
        -- toute utilisation en dashboard. Ne peut pas être ventilé par
        -- moyen (les actions STATUS n'ont pas de resource_id).
        sumIf(dateDiff('second', start_datetime_utc, corrected_end_datetime_utc) / 3600.0,
              status = 'NAVIGATING') AS heures_navigation_hypothese_moteur
    FROM status_actions
    GROUP BY mission_id
),

-- Missions conjointes / administrations concourantes
intermin AS (
    SELECT
        mission_general_info_id,
        groupArray(administration_id) AS administration_ids,
        groupArray(control_unit_id)   AS control_unit_ids,
        COUNT(DISTINCT administration_id) AS nb_administrations
    FROM rapportnav_proxy.inter_ministerial_service
    GROUP BY mission_general_info_id
),

-- Classification mer/terre/air des moyens (même mapping que la requête 1,
-- dupliqué ici pour ne pas dépendre de l'ordre de sync entre les 2 tables)
resource_dim AS (
    SELECT
        id AS resource_id,
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
mission_resources AS (
    SELECT
        ma.mission_id,
        groupUniqArray(toString(rd.terrain_category)) AS mission_terrain_types,
        uniqExact(mar.resource_id) AS nb_resources_used
    FROM rapportnav_proxy.mission_action_resource mar
    INNER JOIN rapportnav_proxy.mission_action ma ON ma.id = mar.action_id
    LEFT JOIN resource_dim rd ON rd.resource_id = mar.resource_id
    GROUP BY ma.mission_id
),

-- Complétude nav (brique partielle, cf. avertissement déjà documenté
-- dans query_aem_par_mission_3_bases_clickhouse.sql)
nav_completeness AS (
    SELECT
        mission_id,
        countIf(coalesce(is_complete_for_stats, 0) = 0) = 0 AS toutes_actions_nav_completes
    FROM rapportnav_proxy.mission_action
    GROUP BY mission_id
)

SELECT
    toInt32(mgi.mission_id) AS mission_id,
    toString(mgi.mission_id_uuid) AS mission_id_uuid,
    coalesce(mu.unit_names, '') AS unit_names,
    toUInt16(coalesce(mu.nb_unites_distinctes, 0)) AS nb_unites_distinctes,
    coalesce(mu.control_unit_ids, []) AS control_unit_ids,
    -- TODO : rebrancher sur dim_unit_reference_by_id / _by_name (cf. CTE
    -- commentée plus haut) une fois le schéma exact confirmé -- laissé
    -- vide plutôt que de réintroduire l'heuristique service.name.
    '' AS unit_type,
    '' AS facade,
    toInt32(coalesce(mgi.service_id, 0)) AS service_id,
    toDateTime64(envm.start_datetime_utc, 6) AS start_datetime_utc,
    toDateTime64(envm.end_datetime_utc, 6) AS end_datetime_utc,
    toString(multiIf(
        envm.end_datetime_utc IS NULL OR envm.start_datetime_utc IS NULL, 'UNAVAILABLE',
        envm.start_datetime_utc < now() AND envm.end_datetime_utc > now(), 'IN_PROGRESS',
        envm.end_datetime_utc <= now(), 'ENDED',
        envm.start_datetime_utc >= now(), 'UPCOMING',
        'UNAVAILABLE'
    )) AS mission_status,
    toString(coalesce(mgi.mission_report_type, '')) AS mission_report_type,
    toUInt8(mgi.mission_report_type = 'FIELD_REPORT') AS is_field_mission,
    toUInt8(mgi.mission_report_type = 'EXTERNAL_REINFORCEMENT_TIME_REPORT') AS is_external_reinforcement,
    toString(coalesce(mgi.reinforcement_type, '')) AS reinforcement_type,
    toString(coalesce(mgi.jdp_type, '')) AS jdp_type,
    toUInt8(mgi.reinforcement_type = 'JDP' OR mgi.jdp_type IS NOT NULL) AS is_jdp,
    toUInt8(coalesce(mgi.is_mission_armed, 0)) AS is_mission_armed,
    toUInt8(coalesce(mgi.is_with_interministerial_service, 0)) AS is_with_interministerial_service,
    toUInt16(coalesce(im.nb_administrations, 0)) AS nb_intermin_administrations,
    coalesce(im.administration_ids, []) AS intermin_administration_ids,
    coalesce(im.control_unit_ids, []) AS intermin_control_unit_ids,
    toFloat64(coalesce(mgi.nb_hour_at_sea, 0)) AS declared_hours_at_sea,
    toFloat64(coalesce(hm.computed_hours_at_sea, 0)) AS computed_hours_at_sea,
    toFloat64(coalesce(hm.heures_navigation_hypothese_moteur, 0)) AS heures_navigation_hypothese_moteur,
    toFloat64(coalesce(mgi.distance_in_nautical_miles, 0)) AS distance_nm,
    toFloat64(coalesce(mgi.consumed_fuel_in_liters, 0)) AS consumed_fuel_liters,
    toFloat64(coalesce(mgi.consumed_go_in_liters, 0)) AS consumed_go_liters,
    toUInt16(coalesce(mr.nb_resources_used, 0)) AS nb_resources_used,
    coalesce(mr.mission_terrain_types, []) AS mission_terrain_types,
    toUInt8(envm.end_datetime_utc IS NOT NULL AND envm.end_datetime_utc < now()) AS is_mission_finished,
    toUInt8(coalesce(nc.toutes_actions_nav_completes, 0)) AS nav_toutes_actions_completes,
    'rapportnav' AS source_system,
    now() AS updated_at
FROM rapportnav_proxy.mission_general_info mgi
INNER JOIN monitorenv_proxy.missions envm ON envm.id = mgi.mission_id
LEFT JOIN mission_units mu      ON mu.mission_id = mgi.mission_id
LEFT JOIN intermin im           ON im.mission_general_info_id = mgi.id
LEFT JOIN heures_de_mer hm      ON hm.mission_id = mgi.mission_id
LEFT JOIN mission_resources mr  ON mr.mission_id = mgi.mission_id
LEFT JOIN nav_completeness nc   ON nc.mission_id = mgi.mission_id
-- ⚠️ même filtre de date codé en dur que query_aem_par_mission_3_bases_clickhouse.sql
-- (portée jamais expliquée dans le code source) -- à confirmer avec Alexandre,
-- ou à retirer si le rapport ULAM doit couvrir tout l'historique.
WHERE envm.start_datetime_utc >= toDateTime('2025-01-01 00:00:00');


