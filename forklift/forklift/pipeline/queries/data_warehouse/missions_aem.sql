-- =====================================================================
-- Une ligne par mission ET par unité rattachée = TOUS les indicateurs AEM.
-- ⚠️ mission_id n'est plus unique dans le résultat : une mission
-- inter-services (plusieurs unités MonitorEnv rattachées) apparaît une
-- fois par unité, avec les mêmes indicateurs dupliqués sur chaque ligne
-- (heures de mer, contrôles...) -- seule la ligne d'unité change.
--
-- Bases/schemas :
--   rapportnav_proxy  (mission_action, target_2, mission_general_info, mission)
--   monitorenv_proxy  (missions, env_actions, themes, themes_env_actions,
--                       missions_control_units, control_units)
--   monitorfish_proxy (mission_actions)
--
-- Points de vigilance :
-- 4) [tech: env/nav] harcodé à 0, vérifier avec ENV
--    envTraffic.nbrOfRedirectShip (3.3.3), envTraffic.nbrOfSeizure (3.3.4),
--    7.5/7.6 piraterie-brigandage
-- 6) [métier] Filtre rm.is_complete_for_stats = 1 (rapportnav_proxy.mission,
--    Boolean) : ne garder que les missions complètes pour les stats.
--    Jointure via rm.external_id = id monitorenv (String vs Int, cast requis).
-- 7) [tech: env] Filtre sur les missions supprimées à confirmer
--    monitorenv_proxy.mission / env_actions
--    monitorfish_proxy.mission_actions
-- 8) [métier] Référentiel unité -> nom d'origine / façade / zone maritime,
--    fourni manuellement (pas sourcé dans le code) : clé = control_unit_id
--    (monitorenv_proxy.control_units.id), pour les ~27 ULAM et les 6 PAM
--    connues. Pas de bordée A/B côté MonitorEnv : une seule entrée par
--    navire. facade provient exclusivement de rapportnav.dim_unit_reference
--    (référentiel unité, dérivé du control_unit_id) -- aucune référence à
--    monitorenv_proxy.missions.facade.
--    Ce référentiel vit maintenant dans sa propre table
--    (rapportnav.dim_unit_reference, alimentée par dim_unit_reference.sql)
--    plutôt que dupliqué en CTE ici -- source unique partagée avec les 3
--    requêtes rapport_pam_ulam_*.sql (cf. discussion en chat). ⚠️ Ce
--    fichier DOIT continuer à tourner après dim_unit_reference.sql dans
--    sync_table_from_db_connection.csv (aucune dépendance native entre
--    lignes de ce flow -- cf. commentaire détaillé dans dim_unit_reference.sql).
-- =====================================================================

WITH
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
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS corrected_end_datetime_utc
    FROM rapportnav_proxy.mission_action ma
    INNER JOIN monitorenv_proxy.missions envm ON envm.id = ma.mission_id
    WHERE ma.action_type = 'STATUS'
),
heures_de_mer_nav AS (
    SELECT
        mission_id,
        SUM(dateDiff('second', start_datetime_utc, corrected_end_datetime_utc) / 3600.0)
            AS heures_de_mer_nav_ancrage_navigation,
        sumIf(dateDiff('second', start_datetime_utc, corrected_end_datetime_utc) / 3600.0,
              status = 'NAVIGATING')
            AS heures_de_mer_nav_navigation_seule
    FROM status_actions
    WHERE status IN ('ANCHORED', 'NAVIGATING')
      AND corrected_end_datetime_utc >= start_datetime_utc
    GROUP BY mission_id
),
control_targets_nav AS (
    SELECT
        ma.mission_id,
        countIf(toString(t.id) != '00000000-0000-0000-0000-000000000000') AS nb_targets_control_nav
    FROM rapportnav_proxy.mission_action ma
    LEFT JOIN rapportnav_proxy.target_2 t ON toString(t.action_id) = toString(ma.id)
    WHERE ma.action_type = 'CONTROL'
    GROUP BY ma.mission_id
),

mission_units AS (
    SELECT
        mcu.mission_id,
        COUNT(DISTINCT cu.id) AS nb_unites_distinctes
    FROM monitorenv_proxy.missions_control_units mcu
    INNER JOIN monitorenv_proxy.control_units cu ON cu.id = mcu.control_unit_id
    GROUP BY mcu.mission_id
),

-- Unité de la mission : source MonitorEnv (missions_control_units +
-- control_units), pas rapportnav_proxy.mission_general_info.service_id
-- (souvent vide côté rapportnav pour ces mêmes missions).
-- Une ligne par mission ET par unité rattachée : une mission
-- inter-services ressort donc sur plusieurs lignes ici. Conséquence en
-- aval : les indicateurs joints par mission_id (nav_agg, env_agg,
-- fish_agg...) sont dupliqués à l'identique sur chaque ligne d'unité de
-- la même mission -- assumé, pas un bug.
mission_service AS (
    SELECT
        mcu.mission_id,
        cu.name AS unit_name,
        multiIf(
            startsWith(upper(cu.name), 'PAM'), 'PAM',
            startsWith(upper(cu.name), 'ULAM'), 'ULAM',
            'AUTRE'
        ) AS service_type,
        cu.id AS control_unit_id
    FROM monitorenv_proxy.missions_control_units mcu
    LEFT JOIN monitorenv_proxy.control_units cu ON cu.id = mcu.control_unit_id
),

nav_completeness AS (
    SELECT
        mission_id,
        countIf(coalesce(toUInt8(is_complete_for_stats), 0) = 0) AS nb_actions_nav_incompletes,
        countIf(coalesce(toUInt8(is_complete_for_stats), 0) = 0) = 0 AS toutes_actions_nav_completes
    FROM rapportnav_proxy.mission_action
    GROUP BY mission_id
),

nav_agg AS (
    SELECT
        ma.mission_id,

        sumIf(dateDiff('second', ma.start_datetime_utc, ma.end_datetime_utc) / 3600.0,
              ma.action_type = 'RESCUE' AND coalesce(toUInt8(ma.is_migration_rescue), 0) = 0)
            AS n1_1_1_nb_heures_de_mer,
        countIf(ma.action_type = 'RESCUE' AND coalesce(toUInt8(ma.is_migration_rescue), 0) = 0)
            AS n1_1_3_nb_operations_conduites,
        sumIf(ma.number_persons_rescued, ma.action_type = 'RESCUE' AND coalesce(toUInt8(ma.is_migration_rescue), 0) = 0)
            AS n1_1_4_nb_personnes_secourues,

        sumIf(dateDiff('second', ma.start_datetime_utc, ma.end_datetime_utc) / 3600.0,
              ma.action_type = 'RESCUE' AND toUInt8(ma.is_migration_rescue) = 1)
            AS n1_2_1_nb_heures_de_mer,
        countIf(ma.action_type = 'RESCUE' AND toUInt8(ma.is_migration_rescue) = 1)
            AS n1_2_3_nb_operations_conduites,
        sumIf(ma.nb_vessels_tracked_without_intervention, ma.action_type = 'RESCUE' AND toUInt8(ma.is_migration_rescue) = 1)
            AS n1_2_4_nb_embarcations_suivies_sans_intervention,
        sumIf(ma.nb_assisted_vessels_returning_to_shore, ma.action_type = 'RESCUE' AND toUInt8(ma.is_migration_rescue) = 1)
            AS n1_2_5_nb_embarcations_assistees_retour_terre,
        countIf(ma.action_type = 'RESCUE' AND toUInt8(ma.is_migration_rescue) = 1)
            AS n1_2_6_nb_operations_sauvetage,
        sumIf(ma.number_persons_rescued, ma.action_type = 'RESCUE' AND toUInt8(ma.is_migration_rescue) = 1)
            AS n1_2_7_nb_personnes_secourues,

        sumIf(dateDiff('second', ma.start_datetime_utc, ma.end_datetime_utc) / 3600.0,
              ma.action_type = 'RESCUE' AND toUInt8(ma.is_vessel_rescue) = 1)
            AS n2_1_nb_heures_de_mer,
        countIf(ma.action_type = 'RESCUE' AND toUInt8(ma.is_vessel_rescue) = 1)
            AS n2_3_nb_operations,
        countIf(ma.action_type = 'RESCUE' AND toUInt8(ma.is_vessel_rescue) = 1 AND toUInt8(ma.is_vessel_noticed) = 1)
            AS n2_4_nb_mise_en_demeure,
        countIf(ma.action_type = 'RESCUE' AND toUInt8(ma.is_vessel_rescue) = 1 AND toUInt8(ma.is_vessel_towed) = 1)
            AS n2_7_nb_remorquages,

        sumIf(dateDiff('second', ma.start_datetime_utc, ma.end_datetime_utc) / 3600.0,
              ma.action_type = 'ILLEGAL_IMMIGRATION')
            AS n3_4_1_nb_heures_de_mer,
        sumIf(ma.nb_of_intercepted_vessels, ma.action_type = 'ILLEGAL_IMMIGRATION')
            AS n3_4_3_nb_navires_interceptes,
        sumIf(ma.nb_of_intercepted_migrants, ma.action_type = 'ILLEGAL_IMMIGRATION')
            AS n3_4_4_nb_migrants_interceptes,
        sumIf(ma.nb_of_suspected_smugglers, ma.action_type = 'ILLEGAL_IMMIGRATION')
            AS n3_4_5_nb_passeurs_presumes_interceptes,

        sumIf(dateDiff('second', ma.start_datetime_utc, ma.end_datetime_utc) / 3600.0,
              ma.action_type = 'ANTI_POLLUTION')
            AS pollution_heures_de_mer_nav,
        countIf(ma.action_type = 'ANTI_POLLUTION' AND toUInt8(ma.simple_brewing_operation) = 1)
            AS n4_2_3_participation_brassage_simple,
        countIf(ma.action_type = 'ANTI_POLLUTION' AND toUInt8(ma.anti_pol_device_deployed) = 1)
            AS n4_2_4_nb_dispositifs_deployes,
        countIf(ma.action_type = 'ANTI_POLLUTION' AND toUInt8(ma.diversion_carried_out) = 1)
            AS n4_2_7_nb_deroutements_nav,
        countIf(ma.action_type = 'ANTI_POLLUTION' AND toUInt8(ma.pollution_observed_by_authorized_agent) = 1)
            AS n4_2_8_nb_pollutions_detectees_nav,

        (
            sumIf(dateDiff('second', ma.start_datetime_utc, ma.end_datetime_utc) / 3600.0,
                  ma.action_type IN ('VIGIMER', 'BAAEM_PERMANENCE'))
        ) AS n5_1_nb_heures_de_mer_surete_maritime,
        sumIf(dateDiff('second', ma.start_datetime_utc, ma.end_datetime_utc) / 3600.0,
              ma.action_type IN ('PUBLIC_ORDER', 'NAUTICAL_EVENT'))
            AS n5_3_nb_heures_de_mer_ordre_public,
        countIf(ma.action_type IN ('PUBLIC_ORDER', 'NAUTICAL_EVENT'))
            AS n5_4_nb_operations_ordre_public

    FROM rapportnav_proxy.mission_action ma
    GROUP BY ma.mission_id
),

env_action_theme_ids AS (
    SELECT
        env_actions_id AS action_id,
        groupArray(themes_id) AS theme_ids
    FROM monitorenv_proxy.themes_env_actions
    GROUP BY env_actions_id
),
env_agg AS (
    SELECT
        ea.mission_id AS mission_id,

        sumIf(
            dateDiff('second', ea.action_start_datetime_utc, ea.action_end_datetime_utc) / 3600.0,
            has(ifNull(et.theme_ids, []), 103)
        ) AS n3_3_1_nb_heures_de_mer,

        sumIf(
            dateDiff('second', ea.action_start_datetime_utc, ea.action_end_datetime_utc) / 3600.0,
            ea.action_type IN ('CONTROL', 'SURVEILLANCE') AND NOT hasAny(ifNull(et.theme_ids, []), [19, 102])
        ) AS n4_1_1_nb_heures_de_mer,
        -- nb_controls par défaut à 1 (pas 0) quand actionNumberOfControls
        -- n'est pas renseigné sur une action CONTROL : le contrôle a bien
        -- eu lieu (action_type='CONTROL'), seul le décompte détaillé
        -- manque -- cas notamment des contrôles ciblant un établissement
        -- plutôt qu'un navire, où ce champ n'est pas systématiquement
        -- saisi. SURVEILLANCE compte 1 par ligne (countIf) pour la même
        -- raison qu'une action = une opération -- PAS parce qu'elle
        -- cacherait des contrôles non détaillés : vérifié dans le backend
        -- monitorenv (EnvActionSurveillanceProperties.kt), une SURVEILLANCE
        -- n'a aucun champ de décompte de contrôles (observations/awareness
        -- seulement), donc rien à défaut-1 ici, cf. même correction sur
        -- nb_controls dans rapport_pam_ulam_action.sql.
        sumIf(
            if(JSONHas(ea.value, 'actionNumberOfControls'), JSONExtractInt(ea.value, 'actionNumberOfControls'), 1),
            ea.action_type = 'CONTROL'AND NOT hasAny(ifNull(et.theme_ids, []), [19, 102])
        )
        + countIf(
            ea.action_type = 'SURVEILLANCE' AND NOT hasAny(ifNull(et.theme_ids, []), [19, 102])
        )
            AS n4_1_3_nb_operations,
        sumIf(
            length(arrayFilter(x -> length(JSONExtractArrayRaw(x, 'natinf')) > 0,
                                JSONExtractArrayRaw(ifNull(ea.value, ''), 'infractions'))),
            ea.action_type IN ('CONTROL', 'SURVEILLANCE') AND NOT hasAny(ifNull(et.theme_ids, []), [19, 102])
        ) AS n4_1_4_nb_infractions,
        sumIf(
            length(arrayFilter(x -> JSONExtractString(x, 'infractionType') = 'WITH_REPORT',
                                JSONExtractArrayRaw(ifNull(ea.value, ''), 'infractions'))),
            ea.action_type IN ('CONTROL', 'SURVEILLANCE') AND NOT hasAny(ifNull(et.theme_ids, []), [19, 102])
        ) AS n4_1_5_nb_pv,

        sumIf(
            dateDiff('second', ea.action_start_datetime_utc, ea.action_end_datetime_utc) / 3600.0,
            hasAny(ifNull(et.theme_ids, []), [19, 102])
        ) AS pollution_heures_de_mer_env,
        sumIf(
            arraySum(arrayMap(x -> length(JSONExtractArrayRaw(x, 'natinf')),
                               JSONExtractArrayRaw(ifNull(ea.value, ''), 'infractions'))),
            hasAny(ifNull(et.theme_ids, []), [19, 102])
        ) AS n4_2_5_nb_infractions,
        sumIf(
            length(arrayFilter(x -> JSONExtractString(x, 'infractionType') = 'WITH_REPORT',
                                JSONExtractArrayRaw(ifNull(ea.value, ''), 'infractions'))),
            hasAny(ifNull(et.theme_ids, []), [19, 102])
        ) AS n4_2_6_nb_pv,

        countIf(has(ifNull(et.theme_ids, []), 165))
            AS n4_4_2_nb_operations_scientifiques,
        countIf(has(ifNull(et.theme_ids, []), 104))
            AS n4_4_3_nb_operations_police_bcm,
        sumIf(
            dateDiff('second', ea.action_start_datetime_utc, ea.action_end_datetime_utc) / 3600.0,
            has(ifNull(et.theme_ids, []), 104)
        ) AS n4_4_1_nb_heures_de_mer,

        -- Même défaut à 1 (pas 0) que n4_1_3_nb_operations ci-dessus.
        sumIf(
            if(JSONHas(ea.value, 'actionNumberOfControls'), JSONExtractInt(ea.value, 'actionNumberOfControls'), 1),
            JSONExtractString(ea.value, 'vehicleType') = 'VESSEL' AND ea.action_type = 'CONTROL'
        )
        + countIf(JSONExtractString(ea.value, 'vehicleType') = 'VESSEL' AND ea.action_type = 'SURVEILLANCE')
            AS nb_targets_control_env

    FROM monitorenv_proxy.env_actions ea
    LEFT JOIN env_action_theme_ids et ON toString(et.action_id) = toString(ea.id)
    GROUP BY ea.mission_id
),

fish_agg AS (
    SELECT
        fa.mission_id,
        sumIf(
            dateDiff('second', fa.action_datetime_utc, fa.action_end_datetime_utc) / 3600.0,
            fa.action_type IN ('SEA_CONTROL', 'LAND_CONTROL')
        ) AS n4_3_1_nb_heures_de_mer_fish,
        countIf(fa.action_type IN ('SEA_CONTROL', 'LAND_CONTROL'))
            AS n4_3_3_nb_operations_polpeche,
        countIf(fa.action_type = 'SEA_CONTROL')
            AS n4_3_5_nb_navires_inspectes,
        sumIf(
            length(arrayFilter(x -> JSONExtractString(x, 'infractionType') = 'WITH_RECORD',
                                JSONExtractArrayRaw(ifNull(fa.infractions, '')))),
            fa.action_type = 'SEA_CONTROL'
        ) AS n4_3_6_nb_pv_mer,
        sumIf(
            length(arrayFilter(x -> JSONHas(x, 'natinf'), JSONExtractArrayRaw(ifNull(fa.infractions, '')))),
            fa.action_type = 'SEA_CONTROL'
        ) AS n4_3_7_nb_infractions_mer,
        countIf(fa.action_type = 'SEA_CONTROL' AND toUInt8(fa.seizure_and_diversion) = 1)
            AS n4_3_8_nb_navires_accompagnes_deroutes,
        sumIf(fa.species_quantity_seized, fa.action_type = 'SEA_CONTROL')
            AS n4_3_9_quantite_kg_saisie
    FROM monitorfish_proxy.mission_actions fa
    WHERE fa.completion = 'COMPLETED'
    GROUP BY fa.mission_id
),
fish_control_targets AS (
    SELECT mission_id, countIf(action_type = 'SEA_CONTROL') AS nb_targets_control_fish
    FROM monitorfish_proxy.mission_actions
    WHERE completion = 'COMPLETED'
    GROUP BY mission_id
)

SELECT
    toInt32(m.id) AS mission_id,
    -- assumeNotNull() : sert de sorting key ClickHouse (order_by du CSV
    -- de sync), qui refuse les colonnes Nullable par défaut.
    assumeNotNull(toDateTime(m.start_datetime_utc)) AS mission_date_debut,
    toDateTime(m.end_datetime_utc)   AS mission_date_fin,
    toString(ms.unit_name)              AS unite_nom,
    toInt32(coalesce(mu.nb_unites_distinctes, 0)) AS unite_nb_distinctes,

    -- Référence ajoutée via rapportnav.dim_unit_reference (clé control_unit_id) :
    toString(coalesce(nullIf(urid.nom_ou_ville_origine, ''), ms.unit_name)) AS nom_ou_ville_origine,
    toString(coalesce(nullIf(urid.zone_maritime, ''), '')) AS zone_maritime,

    toString(gi.mission_id_uuid)          AS mission_id_uuid,
    toInt32(coalesce(gi.service_id, 0))   AS service_id,
    -- facade : uniquement via le référentiel unité (facade_ref, dérivé
    -- du control_unit_id) -- aucune référence à
    -- monitorenv_proxy.missions.facade.
    toString(coalesce(nullIf(urid.facade_ref, ''), '')) AS facade,
    arrayStringConcat(coalesce(m.mission_types, []), ', ') AS mission_types,
    toString(m.mission_source)            AS mission_source,
    toUInt8(m.deleted)                    AS is_deleted,

    toString(multiIf(
        m.end_datetime_utc IS NULL OR m.start_datetime_utc IS NULL, 'UNAVAILABLE',
        m.start_datetime_utc < now() AND m.end_datetime_utc > now(), 'IN_PROGRESS',
        m.end_datetime_utc <= now(), 'ENDED',
        m.start_datetime_utc >= now(), 'UPCOMING',
        'UNAVAILABLE'
    )) AS mission_status,
    toUInt8(m.end_datetime_utc IS NOT NULL AND m.end_datetime_utc < now()) AS is_mission_finished,
    toUInt8(coalesce(nc.toutes_actions_nav_completes, 0)) AS nav_toutes_actions_completes,
    toInt32(coalesce(nc.nb_actions_nav_incompletes, 0))   AS nav_nb_actions_incompletes,

    -- 1.1
    round(toFloat64(coalesce(n.n1_1_1_nb_heures_de_mer, 0)), 1)              AS n1_1_1_nb_heures_de_mer,
    toInt64(coalesce(n.n1_1_3_nb_operations_conduites, 0))         AS n1_1_3_nb_operations_conduites,
    toInt64(coalesce(n.n1_1_4_nb_personnes_secourues, 0))          AS n1_1_4_nb_personnes_secourues,
    -- 1.2
    round(toFloat64(coalesce(n.n1_2_1_nb_heures_de_mer, 0)), 1)                          AS n1_2_1_nb_heures_de_mer,
    toInt64(coalesce(n.n1_2_3_nb_operations_conduites, 0))                     AS n1_2_3_nb_operations_conduites,
    toInt64(coalesce(n.n1_2_4_nb_embarcations_suivies_sans_intervention, 0))   AS n1_2_4_nb_embarcations_suivies_sans_intervention,
    toInt64(coalesce(n.n1_2_5_nb_embarcations_assistees_retour_terre, 0))      AS n1_2_5_nb_embarcations_assistees_retour_terre,
    toInt64(coalesce(n.n1_2_6_nb_operations_sauvetage, 0))                     AS n1_2_6_nb_operations_sauvetage,
    toInt64(coalesce(n.n1_2_7_nb_personnes_secourues, 0))                      AS n1_2_7_nb_personnes_secourues,
    -- 2
    round(toFloat64(coalesce(n.n2_1_nb_heures_de_mer, 0)), 1)          AS n2_1_nb_heures_de_mer,
    toInt64(coalesce(n.n2_3_nb_operations, 0))                AS n2_3_nb_operations,
    toInt64(coalesce(n.n2_4_nb_mise_en_demeure, 0))            AS n2_4_nb_mise_en_demeure,
    toInt64(coalesce(n.n2_7_nb_remorquages, 0))                 AS n2_7_nb_remorquages,

    -- 3.3
    round(toFloat64(coalesce(e.n3_3_1_nb_heures_de_mer, 0)), 1)   AS n3_3_1_nb_heures_de_mer,
    toInt64(0)                                            AS n3_3_3_nb_navires_deroutes_ou_saisis,
    toInt64(0)                                             AS n3_3_4_nb_saisies,

    -- 3.4
    round(toFloat64(coalesce(n.n3_4_1_nb_heures_de_mer, 0)), 1)                  AS n3_4_1_nb_heures_de_mer,
    toInt64(coalesce(n.n3_4_3_nb_navires_interceptes, 0))               AS n3_4_3_nb_navires_interceptes,
    toInt64(coalesce(n.n3_4_4_nb_migrants_interceptes, 0))               AS n3_4_4_nb_migrants_interceptes,
    toInt64(coalesce(n.n3_4_5_nb_passeurs_presumes_interceptes, 0))       AS n3_4_5_nb_passeurs_presumes_interceptes,

    -- 4.1
    round(toFloat64(coalesce(e.n4_1_1_nb_heures_de_mer, 0)), 1)  AS n4_1_1_nb_heures_de_mer,
    toInt64(coalesce(e.n4_1_3_nb_operations, 0))        AS n4_1_3_nb_operations,
    toInt64(coalesce(e.n4_1_4_nb_infractions, 0))        AS n4_1_4_nb_infractions,
    toInt64(coalesce(e.n4_1_5_nb_pv, 0))                  AS n4_1_5_nb_pv,

    -- 4.2 (nav + env réunis pour 4.2.1)
    round(toFloat64(coalesce(n.pollution_heures_de_mer_nav, 0) + coalesce(e.pollution_heures_de_mer_env, 0)), 1) AS n4_2_1_nb_heures_de_mer,
    toInt64(coalesce(n.n4_2_3_participation_brassage_simple, 0))  AS n4_2_3_participation_brassage_simple,
    toInt64(coalesce(n.n4_2_4_nb_dispositifs_deployes, 0))         AS n4_2_4_nb_dispositifs_deployes,
    toInt64(coalesce(e.n4_2_5_nb_infractions, 0))                   AS n4_2_5_nb_infractions,
    toInt64(coalesce(e.n4_2_6_nb_pv, 0))                             AS n4_2_6_nb_pv,
    toInt64(coalesce(n.n4_2_7_nb_deroutements_nav, 0))                AS n4_2_7_nb_deroutements,
    toInt64(coalesce(n.n4_2_8_nb_pollutions_detectees_nav, 0))         AS n4_2_8_nb_pollutions_detectees,

    -- 4.3 (nav [statuts navigation seuls] + fish [durée des contrôles] pour 4.3.1)
    round(toFloat64(coalesce(hm.heures_de_mer_nav_navigation_seule, 0) + coalesce(f.n4_3_1_nb_heures_de_mer_fish, 0)), 1) AS n4_3_1_nb_heures_de_mer,
    toInt64(coalesce(f.n4_3_3_nb_operations_polpeche, 0))           AS n4_3_3_nb_operations_polpeche,
    toInt64(coalesce(f.n4_3_5_nb_navires_inspectes, 0))              AS n4_3_5_nb_navires_inspectes,
    toInt64(coalesce(f.n4_3_6_nb_pv_mer, 0))                          AS n4_3_6_nb_pv_mer,
    toInt64(coalesce(f.n4_3_7_nb_infractions_mer, 0))                  AS n4_3_7_nb_infractions_mer,
    toInt64(coalesce(f.n4_3_8_nb_navires_accompagnes_deroutes, 0))     AS n4_3_8_nb_navires_accompagnes_deroutes,
    toFloat64(coalesce(f.n4_3_9_quantite_kg_saisie, 0))                 AS n4_3_9_quantite_kg_saisie,

    -- 4.4
    round(toFloat64(coalesce(e.n4_4_1_nb_heures_de_mer, 0)), 1)          AS n4_4_1_nb_heures_de_mer,
    toInt64(coalesce(e.n4_4_2_nb_operations_scientifiques, 0))  AS n4_4_2_nb_operations_scientifiques,
    toInt64(coalesce(e.n4_4_3_nb_operations_police_bcm, 0))      AS n4_4_3_nb_operations_police_bcm,

    -- 5
    round(toFloat64(coalesce(n.n5_1_nb_heures_de_mer_surete_maritime, 0)), 1) AS n5_1_nb_heures_de_mer_surete_maritime,
    round(toFloat64(coalesce(n.n5_3_nb_heures_de_mer_ordre_public, 0)), 1)     AS n5_3_nb_heures_de_mer_ordre_public,
    toInt64(coalesce(n.n5_4_nb_operations_ordre_public, 0))           AS n5_4_nb_operations_ordre_public,

    -- 7 (navigation + mouillage pour 7.1)
    round(toFloat64(coalesce(hm.heures_de_mer_nav_ancrage_navigation, 0)), 1) AS n7_1_nb_heures_de_mer,
    toInt32(coalesce(gi.nbr_of_recognized_vessel, 0))                AS n7_3_nb_navires_reconnus,
    toInt64(
        coalesce(ct.nb_targets_control_nav, 0) + coalesce(e.nb_targets_control_env, 0) + coalesce(fct.nb_targets_control_fish, 0)
    ) AS n7_4_nb_controles_en_mer,
    toFloat64(0) AS n7_5_nb_heures_de_mer_piraterie_brigandage,
    toFloat64(0) AS n7_6_nb_heures_de_vol_piraterie_brigandage

FROM monitorenv_proxy.missions m
LEFT JOIN nav_agg n                             ON n.mission_id = m.id
LEFT JOIN heures_de_mer_nav hm                  ON hm.mission_id = m.id
LEFT JOIN control_targets_nav ct                ON ct.mission_id = m.id
LEFT JOIN mission_units mu                      ON mu.mission_id = m.id
LEFT JOIN mission_service ms                    ON ms.mission_id = m.id
LEFT JOIN rapportnav.dim_unit_reference urid     ON urid.control_unit_id = ms.control_unit_id
LEFT JOIN nav_completeness nc                   ON nc.mission_id = m.id
LEFT JOIN rapportnav_proxy.mission_general_info gi ON gi.mission_id = m.id
-- rm : table mission (UUID) côté rapportnav, jointe via
-- external_id = id monitorenv (String vs Int, cast requis).
-- is_complete_for_stats est un Boolean.
LEFT JOIN rapportnav_proxy.mission rm            ON rm.external_id = toString(m.id)
LEFT JOIN env_agg e                             ON e.mission_id = m.id
LEFT JOIN fish_agg f                             ON f.mission_id = m.id
LEFT JOIN fish_control_targets fct               ON fct.mission_id = m.id

WHERE toDateTime(m.start_datetime_utc) >= toDateTime('2025-01-01 00:00:00')
  AND toUInt8(m.deleted) = 0
  AND ms.service_type IN ('PAM', 'ULAM')
  AND rm.is_complete_for_stats = 1
ORDER BY m.start_datetime_utc DESC
;
