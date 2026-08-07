-- =====================================================================
-- Rapport AEM agrégé par "Nom ou Ville d'origine" et par mois.
-- Colonnes : Mois / Zone Maritime de déploiement / Administration / Type
-- de moyen / Famille / Nom ou Ville d'origine, suivi des indicateurs AEM
-- sommés.
--
-- ⚠️ `base` est en grain mission x unité (une mission inter-services y
-- apparaît une fois par unité rattachée). Les indicateurs par mission
-- (heures de mer, contrôles...) sont donc dupliqués sur chaque ligne
-- d'unité d'une même mission -- assumé, pas un bug. nb_missions_agregees
-- compte les missions distinctes (pas les lignes) pour rester lisible ;
-- mission_ids/unites_noms_brutes/facades_brutes listent le détail agrégé
-- sur chaque ligne de sortie.
-- =====================================================================

WITH
base AS (
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
    -- Unité de la mission : source MonitorEnv (missions_control_units +
    -- control_units), pas rapportnav_proxy.mission_general_info.service_id
    -- (souvent vide côté rapportnav pour ces mêmes missions).
    -- Une ligne par mission ET par unité rattachée : une mission
    -- inter-services ressort donc sur plusieurs lignes ici. Conséquence
    -- en aval : les indicateurs joints par mission_id (nav_agg, env_agg,
    -- fish_agg...) sont dupliqués à l'identique sur chaque ligne d'unité
    -- de la même mission -- assumé, pas un bug.
    mission_service AS (
        SELECT
            mcu.mission_id,
            cu.name AS unit_name,
            -- service_type déduit du préfixe du nom d'unité MonitorEnv
            -- (pas de colonne catégorielle PAM/ULAM native).
            multiIf(
                startsWith(upper(cu.name), 'PAM'), 'PAM',
                startsWith(upper(cu.name), 'ULAM'), 'ULAM',
                'AUTRE'
            ) AS service_type,
            cu.id AS control_unit_id
        FROM monitorenv_proxy.missions_control_units mcu
        LEFT JOIN monitorenv_proxy.control_units cu ON cu.id = mcu.control_unit_id
    ),
    -- ULAM : clé = control_unit_id (monitorenv_proxy.control_units.id).
    -- Unité ULAM 29 Nord = Brest, ULAM 29 Sud = Douarnenez (nom d'origine en DDTM).
    dim_unit_reference_by_id AS (
        SELECT 10194 AS control_unit_id, 'DDTM 06' AS nom_ou_ville_origine, 'MED' AS facade_ref, 'Méditerranée' AS zone_maritime
        UNION ALL SELECT 10039, 'DDTM 13',              'MED',  'Méditerranée'
        UNION ALL SELECT 10452, 'DDTM 14',              'MEMN', 'Manche-Mer du Nord'
        UNION ALL SELECT 10204, 'DDTM 22',              'NAMO', 'Atlantique'
        UNION ALL SELECT 10457, 'DDTM 29 Nord',         'NAMO', 'Atlantique'  -- Brest
        UNION ALL SELECT 10288, 'DDTM 29 Sud',          'NAMO', 'Atlantique'  -- Douarnenez
        UNION ALL SELECT 10074, 'DMLC',                 'MED',  'Méditerranée' -- 2A
        UNION ALL SELECT 10192, 'DMLC',                 'MED',  'Méditerranée' -- 2B
        UNION ALL SELECT 10225, 'DDTM 33',              'SA',   'Atlantique'
        UNION ALL SELECT 10255, 'DDTM 17',              'SA',   'Atlantique'
        UNION ALL SELECT 10420, 'DDTM 34/30',           'MED',  'Méditerranée'
        UNION ALL SELECT 10176, 'DDTM 35',              'NAMO', 'Atlantique'
        UNION ALL SELECT 10428, 'DDTM 44',              'NAMO', 'Atlantique'
        UNION ALL SELECT 10210, 'DDTM 50',              'MEMN', 'Manche-Mer du Nord'
        UNION ALL SELECT 10449, 'DDTM 56',              'NAMO', 'Atlantique'
        UNION ALL SELECT 10050, 'DDTM 59',              'MEMN', 'Manche-Mer du Nord'
        UNION ALL SELECT 10318, 'DDTM 62/80',           'MEMN', 'Manche-Mer du Nord'
        UNION ALL SELECT 10364, 'DDTM 64/40',           'SA',   'Atlantique'
        UNION ALL SELECT 10303, 'DDTM 66/11',           'MED',  'Méditerranée'
        UNION ALL SELECT 10423, 'DDTM 76/27',           'MEMN', 'Manche-Mer du Nord'
        UNION ALL SELECT 10166, 'DDTM 83',              'MED',  'Méditerranée'
        UNION ALL SELECT 10171, 'DDTM 85',              'NAMO', 'Atlantique'
        UNION ALL SELECT 10169, 'DM Guadeloupe (971)',  'Guadeloupe', 'Antilles'
        UNION ALL SELECT 10327, 'DM Martinique (972)',  'Martinique', 'Antilles'
        UNION ALL SELECT 10265, 'DGTM Guyane (973)',    'Guyane', 'Guyane'
        UNION ALL SELECT 10183, 'DM SOI (974)',         'La Réunion', 'Sud de l''Océan indien'
        UNION ALL SELECT 10430, 'DTAM St Pierre et Miquelon (975)', 'Saint-Pierre et Miquelon', 'Saint-Pierre et Miquelon'
        UNION ALL SELECT 10047, 'DEALM Mayotte (976)',  'Mayotte', 'Sud de l''Océan indien'
        -- PAM (confirmé via export monitorenv_proxy.control_units,
        -- name LIKE 'PAM%', archived=0 -- pas de bordée A/B côté
        -- MonitorEnv, une seule entrée par navire) :
        UNION ALL SELECT 10080, 'DIRM NAMO',            'NAMO', 'Atlantique'          -- PAM Themis
        UNION ALL SELECT 10121, 'DIRM MEMN',            'MEMN', 'Manche-Mer du Nord'  -- PAM Jeanne Barret
        UNION ALL SELECT 10141, 'DIRM MED',             'MED',  'Méditerranée'        -- PAM Gyptis
        UNION ALL SELECT 10404, 'DIRM SA',              'SA',   'Atlantique'          -- PAM Iris
        -- ⚠️ Ces deux PAM n'existent pas dans rapportnav -- attention,
        -- les indicateurs PAM et ULAM seront agrégés ensemble pour ces
        -- deux directions (même nom_ou_ville_origine que ULAM 974/973) :
        UNION ALL SELECT 10345, 'DM SOI (974)',        'La Réunion', 'Sud de l''Océan indien'  -- PAM Osiris II
        UNION ALL SELECT 10519, 'DGTM Guyane (973)',    'Guyane', 'Guyane'                       -- PAM Cayenne
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

            sumIf(dateDiff('second', ma.start_datetime_utc, ma.end_datetime_utc) / 3600.0,
                  ma.action_type IN ('VIGIMER', 'BAAEM_PERMANENCE'))
                AS n5_1_nb_heures_de_mer_surete_maritime,
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
            sumIf(
                if(JSONHas(ea.value, 'actionNumberOfControls'), JSONExtractInt(ea.value, 'actionNumberOfControls'), 0),
                ea.action_type = 'CONTROL' AND NOT hasAny(ifNull(et.theme_ids, []), [19, 102])
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
            sumIf(
                if(JSONHas(ea.value, 'actionNumberOfControls'), JSONExtractInt(ea.value, 'actionNumberOfControls'), 0),
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
        toDateTime(m.start_datetime_utc) AS mission_date_debut,
        toDateTime(m.end_datetime_utc)   AS mission_date_fin,
        toString(ms.unit_name)              AS unite_nom,
        toString(ms.service_type)           AS service_type,
        -- facade : uniquement via le référentiel unité (facade_ref,
        -- dérivé du control_unit_id) -- aucune référence à
        -- monitorenv_proxy.missions.facade.
        toString(coalesce(nullIf(urid.facade_ref, ''), '')) AS facade,
        toString(coalesce(nullIf(urid.nom_ou_ville_origine, ''), ms.unit_name)) AS nom_ou_ville_origine,
        toString(coalesce(nullIf(urid.zone_maritime, ''), '')) AS zone_maritime,

        toFloat64(coalesce(n.n1_1_1_nb_heures_de_mer, 0))              AS n1_1_1_nb_heures_de_mer,
        toInt64(coalesce(n.n1_1_3_nb_operations_conduites, 0))         AS n1_1_3_nb_operations_conduites,
        toInt64(coalesce(n.n1_1_4_nb_personnes_secourues, 0))          AS n1_1_4_nb_personnes_secourues,
        toFloat64(coalesce(n.n1_2_1_nb_heures_de_mer, 0))                          AS n1_2_1_nb_heures_de_mer,
        toInt64(coalesce(n.n1_2_3_nb_operations_conduites, 0))                     AS n1_2_3_nb_operations_conduites,
        toInt64(coalesce(n.n1_2_4_nb_embarcations_suivies_sans_intervention, 0))   AS n1_2_4_nb_embarcations_suivies_sans_intervention,
        toInt64(coalesce(n.n1_2_5_nb_embarcations_assistees_retour_terre, 0))      AS n1_2_5_nb_embarcations_assistees_retour_terre,
        toInt64(coalesce(n.n1_2_6_nb_operations_sauvetage, 0))                     AS n1_2_6_nb_operations_sauvetage,
        toInt64(coalesce(n.n1_2_7_nb_personnes_secourues, 0))                      AS n1_2_7_nb_personnes_secourues,
        toFloat64(coalesce(n.n2_1_nb_heures_de_mer, 0))          AS n2_1_nb_heures_de_mer,
        toInt64(coalesce(n.n2_3_nb_operations, 0))                AS n2_3_nb_operations,
        toInt64(coalesce(n.n2_4_nb_mise_en_demeure, 0))            AS n2_4_nb_mise_en_demeure,
        toInt64(coalesce(n.n2_7_nb_remorquages, 0))                 AS n2_7_nb_remorquages,
        toFloat64(coalesce(e.n3_3_1_nb_heures_de_mer, 0))   AS n3_3_1_nb_heures_de_mer,
        toInt64(0)                                            AS n3_3_3_nb_navires_deroutes_ou_saisis,
        toInt64(0)                                             AS n3_3_4_nb_saisies,
        toFloat64(coalesce(n.n3_4_1_nb_heures_de_mer, 0))                  AS n3_4_1_nb_heures_de_mer,
        toInt64(coalesce(n.n3_4_3_nb_navires_interceptes, 0))               AS n3_4_3_nb_navires_interceptes,
        toInt64(coalesce(n.n3_4_4_nb_migrants_interceptes, 0))               AS n3_4_4_nb_migrants_interceptes,
        toInt64(coalesce(n.n3_4_5_nb_passeurs_presumes_interceptes, 0))       AS n3_4_5_nb_passeurs_presumes_interceptes,
        toFloat64(coalesce(e.n4_1_1_nb_heures_de_mer, 0))  AS n4_1_1_nb_heures_de_mer,
        toInt64(coalesce(e.n4_1_3_nb_operations, 0))        AS n4_1_3_nb_operations,
        toInt64(coalesce(e.n4_1_4_nb_infractions, 0))        AS n4_1_4_nb_infractions,
        toInt64(coalesce(e.n4_1_5_nb_pv, 0))                  AS n4_1_5_nb_pv,
        toFloat64(coalesce(n.pollution_heures_de_mer_nav, 0) + coalesce(e.pollution_heures_de_mer_env, 0)) AS n4_2_1_nb_heures_de_mer,
        toInt64(coalesce(n.n4_2_3_participation_brassage_simple, 0))  AS n4_2_3_participation_brassage_simple,
        toInt64(coalesce(n.n4_2_4_nb_dispositifs_deployes, 0))         AS n4_2_4_nb_dispositifs_deployes,
        toInt64(coalesce(e.n4_2_5_nb_infractions, 0))                   AS n4_2_5_nb_infractions,
        toInt64(coalesce(e.n4_2_6_nb_pv, 0))                             AS n4_2_6_nb_pv,
        toInt64(coalesce(n.n4_2_7_nb_deroutements_nav, 0))                AS n4_2_7_nb_deroutements,
        toInt64(coalesce(n.n4_2_8_nb_pollutions_detectees_nav, 0))         AS n4_2_8_nb_pollutions_detectees,
        toFloat64(coalesce(hm.heures_de_mer_nav_navigation_seule, 0) + coalesce(f.n4_3_1_nb_heures_de_mer_fish, 0)) AS n4_3_1_nb_heures_de_mer,
        toInt64(coalesce(f.n4_3_3_nb_operations_polpeche, 0))           AS n4_3_3_nb_operations_polpeche,
        toInt64(coalesce(f.n4_3_5_nb_navires_inspectes, 0))              AS n4_3_5_nb_navires_inspectes,
        toInt64(coalesce(f.n4_3_6_nb_pv_mer, 0))                          AS n4_3_6_nb_pv_mer,
        toInt64(coalesce(f.n4_3_7_nb_infractions_mer, 0))                  AS n4_3_7_nb_infractions_mer,
        toInt64(coalesce(f.n4_3_8_nb_navires_accompagnes_deroutes, 0))     AS n4_3_8_nb_navires_accompagnes_deroutes,
        toFloat64(coalesce(f.n4_3_9_quantite_kg_saisie, 0))                 AS n4_3_9_quantite_kg_saisie,
        toFloat64(coalesce(e.n4_4_1_nb_heures_de_mer, 0))          AS n4_4_1_nb_heures_de_mer,
        toInt64(coalesce(e.n4_4_2_nb_operations_scientifiques, 0))  AS n4_4_2_nb_operations_scientifiques,
        toInt64(coalesce(e.n4_4_3_nb_operations_police_bcm, 0))      AS n4_4_3_nb_operations_police_bcm,
        toFloat64(coalesce(n.n5_1_nb_heures_de_mer_surete_maritime, 0)) AS n5_1_nb_heures_de_mer_surete_maritime,
        toFloat64(coalesce(n.n5_3_nb_heures_de_mer_ordre_public, 0))     AS n5_3_nb_heures_de_mer_ordre_public,
        toInt64(coalesce(n.n5_4_nb_operations_ordre_public, 0))           AS n5_4_nb_operations_ordre_public,
        toFloat64(coalesce(hm.heures_de_mer_nav_ancrage_navigation, 0)) AS n7_1_nb_heures_de_mer,
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
    LEFT JOIN mission_service ms                    ON ms.mission_id = m.id
    LEFT JOIN dim_unit_reference_by_id urid          ON urid.control_unit_id = ms.control_unit_id
    LEFT JOIN rapportnav_proxy.mission_general_info gi ON gi.mission_id = m.id
    LEFT JOIN env_agg e                             ON e.mission_id = m.id
    LEFT JOIN fish_agg f                             ON f.mission_id = m.id
    LEFT JOIN fish_control_targets fct               ON fct.mission_id = m.id
    WHERE toDateTime(m.start_datetime_utc) >= toDateTime('2025-01-01 00:00:00')
      AND toUInt8(m.deleted) = 0
      AND ms.service_type IN ('PAM', 'ULAM')
)

-- =====================================================================
-- ASSEMBLAGE FINAL — une ligne par unité et par mois.
--
-- Ordre des colonnes : d'abord toutes les colonnes bonus/QA (Mois,
-- comptages, listes brutes), PUIS le gabarit officiel export mensuel
-- AEM dans son ordre exact -- pour permettre de sélectionner uniquement
-- le bloc final et le coller tel quel dans le gabarit.
--
-- ⚠️ Le gabarit officiel comporte des colonnes SANS AUCUNE source de
-- données dans AEMTableExport.kt/ComputeAEMData.kt (backend rapportnav2),
-- posées ici à 0 avec commentaire explicite, pas un oubli :
--   - TOUTES les colonnes "Nombre d'heures de vol" (1.1.2, 1.2.2, 2.2,
--     3.1.2, 3.2.2, 3.3.2, 3.4.2, 4.1.2, 4.2.2, 4.3.2, 5.2, 7.2) : le
--     pipeline ne trace que des moyens nautiques, aucune donnée aérienne.
--   - Domaine 3.1 entier (stupéfiants), domaine 3.2 entier (armes),
--     domaine 6 entier (police douanière/fiscale).
--   - 2.5, 2.6, 2.8, 2.9, 2.10, 2.11, 3.4.4bis/4.3.4 (VHF), 5.5, 5.6.
-- Les colonnes "N)"/"N.N)" (ex. "1) Sauvegarde de la vie humaine") sont
-- des en-têtes de catégorie du gabarit Excel, sans donnée propre :
-- posées à '' (texte vide).
-- =====================================================================
SELECT
    -- --- Colonnes bonus / QA, à retirer avant collage dans le gabarit ---
    -- assumeNotNull() : mission_date_debut remonte Nullable(DateTime)
    -- depuis Postgres (colonne source nullable), même si le WHERE de
    -- `base` exclut déjà toute valeur NULL en pratique. Nécessaire ici
    -- car "Mois" sert de sorting key ClickHouse (order_by du CSV de
    -- sync), qui refuse les colonnes Nullable par défaut
    -- (allow_nullable_key désactivé).
    assumeNotNull(toDateTime(toStartOfMonth(b.mission_date_debut), 'UTC')) AS "Mois",
    formatDateTime(toStartOfMonth(b.mission_date_debut), '%Y-%m') AS "Mois (texte)",
    toInt32(uniqExact(b.mission_id)) AS nb_missions_agregees,  -- missions distinctes, pas nb de lignes
    MIN(b.mission_date_debut) AS mission_date_debut_min,       -- vérif. manuelle : doit tomber dans le mois affiché par "Mois"
    arrayStringConcat(groupUniqArray(b.facade), ', ')          AS facades_brutes,
    arrayStringConcat(groupUniqArray(toString(b.mission_id)), ', ') AS mission_ids,
    arrayStringConcat(groupUniqArray(b.unite_nom), ', ')       AS unites_noms_brutes,

    -- --- Gabarit officiel export mensuel AEM (ordre exact) ---
    b.zone_maritime                        AS "Zone Maritime de déploiement",
    'Affaires maritimes'                   AS "Administration",
    'Maritime'                             AS "Type de moyen",
    ''                                      AS "Famille",
    b.nom_ou_ville_origine                 AS "Nom ou Ville d'origine",

    CAST(NULL AS Nullable(String))                                      AS "1) Sauvegarde de la vie humaine",
    CAST(NULL AS Nullable(String))                                      AS "1.1) Sauvegarde de la vie humaine hors cadre d'un phénomène migratoire",
    SUM(b.n1_1_1_nb_heures_de_mer)         AS "1.1.1 Nombre d'heures de mer",
    toFloat64(0)                           AS "1.1.2 Nombre d'heures de vol",  -- aucune source (pas de moyen aérien tracé)
    SUM(b.n1_1_3_nb_operations_conduites)  AS "1.1.3 Nombre d'opérations conduites",
    SUM(b.n1_1_4_nb_personnes_secourues)   AS "1.1.4 Nombre de personnes secourues",
    CAST(NULL AS Nullable(String))                                      AS "1.2) Sauvegarde de la vie humaine dans le cadre d'un phénomène migratoire",
    SUM(b.n1_2_1_nb_heures_de_mer)         AS "1.2.1 Nombre d'heures de mer",
    toFloat64(0)                           AS "1.2.2 Nombre d'heures de vol",  -- aucune source
    SUM(b.n1_2_3_nb_operations_conduites)  AS "1.2.3 Nombre d'opérations conduites",
    SUM(b.n1_2_4_nb_embarcations_suivies_sans_intervention) AS "1.2.4  (SAR migrants) Nombre d'embarcations suivies sans nécessité d'intervention",
    SUM(b.n1_2_5_nb_embarcations_assistees_retour_terre)    AS "1.2.5 (SAR migrants)Nombre d'embarcations assistées pour un retour à terre",
    SUM(b.n1_2_6_nb_operations_sauvetage)  AS "1.2.6 (SAR migrants) Nombre d'opérations de sauvetage conduites ",
    SUM(b.n1_2_7_nb_personnes_secourues)   AS "1.2.7 (SAR migrants)Nombre de personnes secourues",

    CAST(NULL AS Nullable(String))                                      AS "2) Assistance aux navires en difficulté et sécurité maritime",
    SUM(b.n2_1_nb_heures_de_mer)           AS "2.1 Nombre d'heures de mer",
    toFloat64(0)                           AS "2.2 Nombre d'heures de vol",  -- aucune source
    SUM(b.n2_3_nb_operations)              AS "2.3 Nombre d'opérations ANED (mise en œuvre de moyens nautique ou aérien)",
    SUM(b.n2_4_nb_mise_en_demeure)         AS "2.4 Nombre d'intervention faisant suite à une mise en demeure",
    toInt64(0)                             AS "2.5 Nombre de mises en œuvre de l'équipe d'évaluation et d'intervention",  -- aucune source
    toInt64(0)                             AS "2.6 Nombre de mise en oeuvre de la CAPINAV",  -- aucune source
    SUM(b.n2_7_nb_remorquages)             AS "2.7 Nombre de remorquages ",
    toInt64(0)                             AS "2.8 Nombre d'opération de maintenance des systèmes de signalisation maritime",  -- aucune source
    toInt64(0)                             AS "2.9 Nombre d'opérations de déminage ",  -- aucune source
    toInt64(0)                             AS "2.10 Nombre de munitions détruites",  -- aucune source
    toFloat64(0)                           AS "2.11 Poids de la matière active correspondante (en kg)",  -- aucune source

    CAST(NULL AS Nullable(String))                                      AS "3) Lutte contre les trafics illicites par voie maritime",
    CAST(NULL AS Nullable(String))                                      AS "3.1) Lutte contre le trafic en mer de produits stupéfiants",
    toFloat64(0)                           AS "3.1.1 Nombre d’heures de mer",  -- domaine 3.1 entier absent du backend
    toFloat64(0)                           AS "3.1.2 Nombre d’heures de vol",
    toInt64(0)                             AS "3.1.3 Nombre d'opérations NARCO en mer",
    toInt64(0)                             AS "3.1.4 Nombre d’inspections en mer",
    toInt64(0)                             AS "3.1.5 Nombre de navires ou embarcations déroutés ou saisis en mer",
    toFloat64(0)                           AS "3.1.6 Quantité saisie en kg suite à ces opérations",
    CAST(NULL AS Nullable(String))                                      AS "3.2) Lutte contre le trafic en mer d’armes, de munitions ou d’explosifs",
    toFloat64(0)                           AS "3.2.1  Nombre d'heures de mer",  -- domaine 3.2 entier absent du backend
    toFloat64(0)                           AS "3.2.2 Nombre d'heures de vol",
    toInt64(0)                             AS "3.2.3 Nombre d'opérations en mer",
    toInt64(0)                             AS "3.2.4 Nombre d’inspections en mer",
    toInt64(0)                             AS "3.2.5 Nombre de navires ou embarcations déroutés ou saisis en mer",
    toInt64(0)                             AS "3.2.6 Nombre d'armes et de munitions saisies suite à ces opérations",
    CAST(NULL AS Nullable(String))                                      AS "3.3) Lutte contre le trafic en mer d’espèces protégées",
    SUM(b.n3_3_1_nb_heures_de_mer)         AS "3.3.1  Nombre d'heures de mer",
    toFloat64(0)                           AS "3.3.2 Nombre d'heures de vol",  -- aucune source
    SUM(b.n3_3_3_nb_navires_deroutes_ou_saisis) AS "3.3.3 Nombre de navires ou embarcations déroutés ou saisis en mer",  -- TODO backend, toujours 0
    SUM(b.n3_3_4_nb_saisies)               AS "3.3.4 Nombre de saisies",  -- TODO backend, toujours 0
    CAST(NULL AS Nullable(String))                                      AS "3.4) lutte contre l’immigration illégale par voie maritime",
    SUM(b.n3_4_1_nb_heures_de_mer)         AS "3.4.1 Nombre d’heures de mer",
    toFloat64(0)                           AS "3.4.2 Nombre d’heures de vol ",  -- aucune source
    SUM(b.n3_4_3_nb_navires_interceptes)   AS "3.4.3 Nombre de navires/embarcations interceptés ",
    SUM(b.n3_4_4_nb_migrants_interceptes)  AS "3.4.4 Nombre de migrants interceptés ",
    SUM(b.n3_4_5_nb_passeurs_presumes_interceptes) AS "3.4.5 Nombre de passeurs présumés interceptés",

    CAST(NULL AS Nullable(String))                                      AS "4) Protection de l’environnement, gestion du patrimoine marin et des ressources publiques marines, surveillance des espaces protégés ",
    CAST(NULL AS Nullable(String))                                      AS "4.1) Surveillance et contrôles pour la protection de l'environnement (hors rejets illicites)",
    SUM(b.n4_1_1_nb_heures_de_mer)         AS "4.1.1 Nombre d'heures de mer de surveillance ou de contrôle pour la protection de l'environnement (hors rejets illicites)",
    toFloat64(0)                           AS "4.1.2 Nombre d’heures de vol de surveillance ou de contrôle pour la protection de l'environnement (hors rejets illicites)",  -- aucune source
    SUM(b.n4_1_3_nb_operations)            AS "4.1.3 Nombre d'opérations de surveillance ou de contrôles (hors rejets illicites)",
    SUM(b.n4_1_4_nb_infractions)           AS "4.1.4 Nombre d’infractions à la réglementation relative à la protection de l'environnement en mer (hors rejets illicites)",
    SUM(b.n4_1_5_nb_pv)                    AS "4.1.5 Nombre de Procès-Verbaux dressés en mer (hors rejets illicites)",
    CAST(NULL AS Nullable(String))                                      AS "4.2) Répression contre les rejets illicites, lutte contre les pollutions",
    SUM(b.n4_2_1_nb_heures_de_mer)         AS "4.2.1 Nombre d'heures de mer (surveillance et lutte)",
    toFloat64(0)                           AS "4.2.2 Nombre d'heures de vol  (surveillance et lutte)",  -- aucune source
    SUM(b.n4_2_3_participation_brassage_simple) AS "4.2.3 Participation à une opération de lutte ANTIPOL en mer (simple brassage)",
    SUM(b.n4_2_4_nb_dispositifs_deployes)  AS "4.2.4 Déploiement d’un dispositif de lutte anti-pollution en mer (dispersant, barrage, etc…)",
    SUM(b.n4_2_5_nb_infractions)           AS "4.2.5 Nombre d'infractions constatées",
    SUM(b.n4_2_6_nb_pv)                    AS "4.2.6 Nombre de procès-verbaux dressés",
    SUM(b.n4_2_7_nb_deroutements)          AS "4.2.7 Nombre de déroutements effectués",
    SUM(b.n4_2_8_nb_pollutions_detectees)  AS "4.2.8 Nombre de pollutions détectées et/ou constatées par un agent habilité",
    CAST(NULL AS Nullable(String))                                      AS "4.3) Lutte contre les activités de pêche illégale",
    SUM(b.n4_3_1_nb_heures_de_mer)         AS "4.3.1 Nombre d’heures de mer (surveillance/police des pêches) :",
    toFloat64(0)                           AS "4.3.2 Nombre d’heures de vol (surveillance/police des pêches) :",  -- aucune source
    SUM(b.n4_3_3_nb_operations_polpeche)   AS "4.3.3 nombre d'opérations POLPECHE",
    toInt64(0)                             AS "4.3.4 Nombre de navires de pêche contrôlés par VHF en mer ",  -- aucune source
    SUM(b.n4_3_5_nb_navires_inspectes)     AS "4.3.5 Nombre de navires inspectés en mer (montée à bord)",
    SUM(b.n4_3_6_nb_pv_mer)                AS "4.3.6 Nombre de procès-verbaux dressés en mer (législation pêche)",
    SUM(b.n4_3_7_nb_infractions_mer)       AS "4.3.7 Nombre d'infractions constatées en mer",
    SUM(b.n4_3_8_nb_navires_accompagnes_deroutes) AS "4.3.8 Nombre de navires accompagnés ou déroutés",
    SUM(b.n4_3_9_quantite_kg_saisie)       AS "4.3.9 quantitée de produits de la pêche saisis/rejetés en mer(en kg)",
    CAST(NULL AS Nullable(String))                                      AS "4.4) Protection des biens culturels maritimes",
    SUM(b.n4_4_1_nb_heures_de_mer)         AS "4.4.1 Nombre d'heures de mer",
    SUM(b.n4_4_2_nb_operations_scientifiques) AS "4.4.2 Nombre d'opérations scientifiques",
    SUM(b.n4_4_3_nb_operations_police_bcm) AS "4.4.3 Nombre d'opération de police des BCM",

    CAST(NULL AS Nullable(String))                                      AS "5) Sûreté maritime et maintien de l'ordre public en mer",
    SUM(b.n5_1_nb_heures_de_mer_surete_maritime) AS "5.1  Nombre d'heures de mer sureté maritime (y compris Vigipirate-mer)    ",
    toFloat64(0)                           AS "5.2 Nombre d'heures de vol sureté maritime (y compris Vigipirate-mer)  ",  -- aucune source
    SUM(b.n5_3_nb_heures_de_mer_ordre_public) AS "5.3 Nombre d'heures de mer de maintien de l'ordre public en mer",
    SUM(b.n5_4_nb_operations_ordre_public) AS "5.4 Nombre d'opérations de maintien de l'ordre public en mer",
    toInt64(0)                             AS "5.5 Nombre de traversées protégées par des équipes d'agents de l'Etat,",  -- aucune source
    toInt64(0)                             AS "5.6 Nombre de contrôles sûreté sur navire",  -- aucune source

    CAST(NULL AS Nullable(String))                                      AS "6) Police douanière, fiscale et économique en mer (hors stupéfiants)",
    toFloat64(0)                           AS "6.1  Nombre d'heures de mer",  -- domaine 6 entier absent du backend
    toFloat64(0)                           AS "6.2 Nombre d'heures de vol",
    toInt64(0)                             AS "6.3 Nombre d'opérations",
    toInt64(0)                             AS "6.4 Nombre de navires contrôlés en mer au titre de la police douanière ou fiscale",
    toInt64(0)                             AS "6.5 Nombre d'infractions douanières et fiscales constatées en mer",
    toFloat64(0)                           AS "6.6 Montant des droits et taxes redressés",
    toInt64(0)                             AS "6.7 Nombre de constatations en matière de tabac et cigarettes",
    toInt64(0)                             AS "6.8 Nombre de manquement à l'obligation déclarative et de blanchiments douaniers relevés",

    CAST(NULL AS Nullable(String))                                      AS "7) Souveraineté et protection des intérêts nationaux ",
    SUM(b.n7_1_nb_heures_de_mer)           AS "7.1 Nombre d’heures de mer de surveillance générale des approches maritimes (ZEE)",
    toFloat64(0)                           AS "7.2 Nombre d’heures de vol de surveillance générale des approches maritimes (ZEE)",  -- aucune source
    SUM(b.n7_3_nb_navires_reconnus)        AS "7.3 Nombre total de navires reconnus dans les approches maritimes (ZEE)",
    SUM(b.n7_4_nb_controles_en_mer)        AS "7.4 Nombre de contrôles en mer de navires (toutes zones)",
    SUM(b.n7_5_nb_heures_de_mer_piraterie_brigandage) AS "7.5 Nombre d’heures de mer dédiées à un évènement de piraterie ou brigandage",
    SUM(b.n7_6_nb_heures_de_vol_piraterie_brigandage) AS "7.6 Nombre d’heures de vol dédiées à un évènement de piraterie ou brigandage"

FROM base b

GROUP BY
    assumeNotNull(toDateTime(toStartOfMonth(b.mission_date_debut), 'UTC')),
    formatDateTime(toStartOfMonth(b.mission_date_debut), '%Y-%m'),
    b.zone_maritime,
    b.nom_ou_ville_origine
ORDER BY "Mois" DESC, "Zone Maritime de déploiement", "Nom ou Ville d'origine"
;
