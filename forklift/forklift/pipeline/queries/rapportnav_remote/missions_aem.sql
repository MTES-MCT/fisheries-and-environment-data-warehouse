-- =====================================================================
-- Une ligne par mission = TOUS les indicateurs AEM (AEMTableExport),
-- en dialecte ClickHouse.
--
-- Bases/schemas supposés :
--   rapportnav_proxy  (mission_action, target_2, mission_general_info, mission)
--   monitorenv_proxy  (missions, env_actions, themes, themes_env_actions,
--                       missions_control_units, control_units)
--   monitorfish_proxy (mission_actions)
--
-- ⚠️ A confirmer/tester (non vérifiable sans accès direct aux bases) :
--   - `value` (env_actions) et `infractions` (mission_actions) supposées
--     de type String contenant du JSON brut. Si typées JSON natif
--     ClickHouse, remplacer JSONExtract* par la syntaxe `col.champ`.
--   - noms de clé JSON en camelCase (sérialisation Jackson par défaut
--     côté backend Kotlin), jamais vérifiés sur un payload réel.
--
-- Points de vigilance métier (vérifiés dans le code source au 20/07/2026) :
-- 1) monitorenv_proxy.env_actions : tout le détail métier est dans la
--    colonne `value` (JSON), pas en colonnes.
-- 2) monitorfish_proxy.mission_actions : colonnes plates sauf `infractions`
--    et `segments` (JSON), et `infractions` EST l'array lui-même (pas
--    imbriqué sous une clé), contrairement à env_actions.value.infractions.
-- 3) Rapprochement mission_id entre rapportnav / monitorenv / monitorfish :
--    PAS le même référentiel d'ID a priori (cf. missions_pam.sql côté
--    DWH) — jointure directe ci-dessous à sécuriser.
-- 4) TODO backend jamais implémentés, toujours 0 : envTraffic.nbrOfRedirectShip
--    (3.3.3), envTraffic.nbrOfSeizure (3.3.4).
-- 5) PAM/ULAM : aucun champ catégoriel n'existe en base (le flow Prefect
--    s'appuie sur un dictionnaire Python codé en dur, mapper_facade_control).
--    La requête filtre les MISSIONS ayant au moins une unité PAM/ULAM
--    (cf. missions_pam_ulam), sur pattern texte du préfixe de
--    control_units.name -- une unité hors convention de nommage serait
--    silencieusement exclue de l'éligibilité. unite_nom / unite_nb_distinctes
--    (cf. mission_units) restent non filtrés et reflètent toutes les
--    unités de la mission, y compris une éventuelle unité "autre".
-- 6) "Complétude pour stats" : pas de champ mission-level stocké. La vraie
--    règle (MissionEntity.isCompleteForStats()) combine 3 validateurs
--    (actions nav, données env, infos générales), non reproduits ici.
--    mission_status = 'ENDED' (100% fidèle, basé sur les dates) est le
--    filtre "missions closes" le plus fiable ; nav_toutes_actions_completes
--    n'est qu'une brique partielle (nav uniquement).
-- 7) is_deleted (monitorenv_proxy.missions.deleted) n'est pas filtré par
--    défaut : les missions supprimées restent dans le résultat.
-- =====================================================================

WITH
-- =====================================================================
-- BLOC NAV (rapportnav_proxy.mission_action)
-- =====================================================================
-- rapportnav_proxy.mission.id est un UUID (clé interne rapportnav), sans
-- rapport avec le mission_id Int32 utilisé par mission_action, target_2,
-- mission_general_info et l'API AEM. Ce mission_id Int32 est l'id de la
-- mission côté MonitorEnv (cf. GetEnvMissionById dans rapportnav2, et
-- missions_pam.sql côté DWH) : la date de fin de mission est donc lue
-- dans monitorenv_proxy.missions, pas dans rapportnav_proxy.mission.
status_actions AS (
    SELECT
        ma.mission_id,
        ma.status,
        ma.start_datetime_utc,
        -- Reconstitue la fin de chaque intervalle de statut en le clôturant
        -- au début du suivant (fenêtre par mission, triée par date). ifNull
        -- force un type non-nullable pour le "default" de leadInFrame, requis
        -- pour matcher exactement le type de start_datetime_utc. Si la
        -- mission n'a pas de end_datetime_utc, on retombe sur le
        -- start_datetime_utc de l'action (durée nulle plutôt qu'une date
        -- inventée).
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
heures_de_mer_nav AS (
    SELECT
        mission_id,
        SUM(dateDiff('second', start_datetime_utc, corrected_end_datetime_utc) / 3600.0)
            AS heures_de_mer_nav_ancrage_navigation  -- brique de 7.1 et 4.3.1
    FROM status_actions
    WHERE status IN ('ANCHORED', 'NAVIGATING')
    GROUP BY mission_id
),
control_targets_nav AS (
    SELECT
        ma.mission_id,
        -- target_2.id est UUID : après un LEFT JOIN sans correspondance,
        -- ClickHouse remplit avec l'UUID zéro (pas une chaîne vide), d'où
        -- la comparaison à '00000000-...' plutôt qu'à ''.
        countIf(toString(t.id) != '00000000-0000-0000-0000-000000000000') AS nb_targets_control_nav
    FROM rapportnav_proxy.mission_action ma
    LEFT JOIN rapportnav_proxy.target_2 t ON t.action_id = toString(ma.id)
    WHERE ma.action_type = 'CONTROL'
    GROUP BY ma.mission_id
),

-- =====================================================================
-- BLOC FILTRES RAPPORT : unité(s), complétude
-- =====================================================================

-- Unité(s) rattachée(s) à la mission (toutes, sans filtre).
mission_units AS (
    SELECT
        mcu.mission_id,
        arrayStringConcat(groupArray(cu.name), ', ') AS unit_names,
        -- nombre d'unités distinctes -> permet de repérer les missions
        -- inter-services (cf. MissionEntity.isInterServices() : >1 unité
        -- distincte = mission conjointe, logique métier différente pour
        -- la complétude stats)
        COUNT(DISTINCT cu.id) AS nb_unites_distinctes
    FROM monitorenv_proxy.missions_control_units mcu
    INNER JOIN monitorenv_proxy.control_units cu ON cu.id = mcu.control_unit_id
    GROUP BY mcu.mission_id
),

-- Filtre au niveau MISSION : missions ayant au moins une unité PAM ou
-- ULAM rattachée (mission_units ci-dessus reste non filtrée, donc
-- unite_nom/unite_nb_distinctes continuent de refléter TOUTES les
-- unités de la mission, y compris une éventuelle unité "autre" en cas
-- de mission inter-services).
-- ⚠️ Aucune colonne structurelle "PAM"/"ULAM" n'existe en base -- filtre
-- texte sur le préfixe de control_units.name (ex. "PAM Iris", "ULAM 2B"),
-- par analogie avec le dictionnaire codé en dur du flow Prefect
-- (mapper_facade_control). Une unité hors convention de nommage serait
-- silencieusement exclue.
missions_pam_ulam AS (
    SELECT DISTINCT mcu.mission_id
    FROM monitorenv_proxy.missions_control_units mcu
    INNER JOIN monitorenv_proxy.control_units cu ON cu.id = mcu.control_unit_id
    WHERE startsWith(upper(cu.name), 'PAM') OR startsWith(upper(cu.name), 'ULAM')
),

-- Complétude côté nav uniquement : mission_action.is_complete_for_stats
-- est un champ RÉEL, stocké par action (pas par mission). On agrège ici
-- "toutes les actions nav de la mission sont complètes".
-- ⚠️ Ce n'est QU'UNE BRIQUE de la vraie complétude utilisée par forklift
-- (MissionEntity.isCompleteForStats()), qui combine en plus la
-- complétude des données env (isEnvDataCompleteForStats) et des infos
-- générales de mission (isGeneralInfoCompleteForStats) -- non
-- reproduites ici. Pour filtrer les "missions ouvertes" au sens strict
-- du flow Prefect (df.isMissionFinished == True), utiliser plutôt
-- mission_status = 'ENDED' dans le SELECT final (calcul 100% fidèle à
-- MissionEntity.calculateMissionStatus(), basé uniquement sur les dates).
nav_completeness AS (
    SELECT
        mission_id,
        countIf(coalesce(is_complete_for_stats, 0) = 0) AS nb_actions_nav_incompletes,
        countIf(coalesce(is_complete_for_stats, 0) = 0) = 0 AS toutes_actions_nav_completes
    FROM rapportnav_proxy.mission_action
    GROUP BY mission_id
),

nav_agg AS (
    SELECT
        ma.mission_id,

        -- 1.1 Out of Migration Rescue
        sumIf(dateDiff('second', ma.start_datetime_utc, ma.end_datetime_utc) / 3600.0,
              ma.action_type = 'RESCUE' AND coalesce(ma.is_migration_rescue, 0) = 0)
            AS oom_rescue_heures_de_mer,                                       -- 1.1.1
        countIf(ma.action_type = 'RESCUE' AND coalesce(ma.is_migration_rescue, 0) = 0)
            AS oom_rescue_nb_operations,                                       -- 1.1.3
        sumIf(ma.number_persons_rescued, ma.action_type = 'RESCUE' AND coalesce(ma.is_migration_rescue, 0) = 0)
            AS oom_rescue_nb_personnes_secourues,                              -- 1.1.4

        -- 1.2 Migration Rescue (SAR migrants)
        sumIf(dateDiff('second', ma.start_datetime_utc, ma.end_datetime_utc) / 3600.0,
              ma.action_type = 'RESCUE' AND ma.is_migration_rescue = 1)
            AS sar_migrants_heures_de_mer,                                     -- 1.2.1
        countIf(ma.action_type = 'RESCUE' AND ma.is_migration_rescue = 1)
            AS sar_migrants_nb_operations,                                     -- 1.2.3 (doublon assumé de 1.2.6, cf. TODO code source)
        sumIf(ma.nb_vessels_tracked_without_intervention, ma.action_type = 'RESCUE' AND ma.is_migration_rescue = 1)
            AS sar_migrants_nb_embarcations_suivies_sans_intervention,         -- 1.2.4
        sumIf(ma.nb_assisted_vessels_returning_to_shore, ma.action_type = 'RESCUE' AND ma.is_migration_rescue = 1)
            AS sar_migrants_nb_embarcations_assistees_retour_terre,            -- 1.2.5
        countIf(ma.action_type = 'RESCUE' AND ma.is_migration_rescue = 1)
            AS sar_migrants_nb_operations_sauvetage,                           -- 1.2.6
        sumIf(ma.number_persons_rescued, ma.action_type = 'RESCUE' AND ma.is_migration_rescue = 1)
            AS sar_migrants_nb_personnes_secourues,                            -- 1.2.7

        -- 2 Vessel Rescue (ANED)
        sumIf(dateDiff('second', ma.start_datetime_utc, ma.end_datetime_utc) / 3600.0,
              ma.action_type = 'RESCUE' AND ma.is_vessel_rescue = 1)
            AS aned_heures_de_mer,                                             -- 2.1
        countIf(ma.action_type = 'RESCUE' AND ma.is_vessel_rescue = 1)
            AS aned_nb_operations,                                             -- 2.3
        countIf(ma.action_type = 'RESCUE' AND ma.is_vessel_rescue = 1 AND ma.is_vessel_noticed = 1)
            AS aned_nb_interventions_mise_en_demeure,                          -- 2.4
        countIf(ma.action_type = 'RESCUE' AND ma.is_vessel_rescue = 1 AND ma.is_vessel_towed = 1)
            AS aned_nb_remorquages,                                            -- 2.7

        -- 3.4 Illegal Immigration (hors API/DWH, calculé quand même)
        sumIf(dateDiff('second', ma.start_datetime_utc, ma.end_datetime_utc) / 3600.0,
              ma.action_type = 'ILLEGAL_IMMIGRATION')
            AS illegal_immig_heures_de_mer,                                    -- 3.4.1
        sumIf(ma.nb_of_intercepted_vessels, ma.action_type = 'ILLEGAL_IMMIGRATION')
            AS illegal_immig_nb_navires_interceptes,                           -- 3.4.3
        sumIf(ma.nb_of_intercepted_migrants, ma.action_type = 'ILLEGAL_IMMIGRATION')
            AS illegal_immig_nb_migrants_interceptes,                          -- 3.4.4
        sumIf(ma.nb_of_suspected_smugglers, ma.action_type = 'ILLEGAL_IMMIGRATION')
            AS illegal_immig_nb_passeurs_suspectes,                            -- 3.4.4bis

        -- 4.2 Pollution (partie nav uniquement)
        sumIf(dateDiff('second', ma.start_datetime_utc, ma.end_datetime_utc) / 3600.0,
              ma.action_type = 'ANTI_POLLUTION')
            AS pollution_heures_de_mer_nav,                                    -- brique nav de 4.2.1
        countIf(ma.action_type = 'ANTI_POLLUTION' AND ma.simple_brewing_operation = 1)
            AS pollution_nb_operations_simple_brassage,                        -- 4.2.3 (complet, 100% nav)
        countIf(ma.action_type = 'ANTI_POLLUTION' AND ma.anti_pol_device_deployed = 1)
            AS pollution_nb_dispositifs_deployes,                              -- 4.2.4 (complet, 100% nav)
        countIf(ma.action_type = 'ANTI_POLLUTION' AND ma.diversion_carried_out = 1)
            AS pollution_nb_deroutements_nav,                                  -- brique nav de 4.2.7
        countIf(ma.action_type = 'ANTI_POLLUTION' AND ma.pollution_observed_by_authorized_agent = 1)
            AS pollution_nb_pollutions_detectees_nav,                          -- brique nav de 4.2.8

        -- 5 Sea Safety
        (
            sumIf(dateDiff('second', ma.start_datetime_utc, ma.end_datetime_utc) / 3600.0,
                  ma.action_type IN ('VIGIMER', 'BAAEM_PERMANENCE', 'NAUTICAL_EVENT'))
            +
            sumIf(dateDiff('second', ma.start_datetime_utc, ma.end_datetime_utc) / 3600.0,
                  ma.action_type = 'PUBLIC_ORDER')
        ) AS sea_safety_heures_de_mer,                                         -- 5.1 (complet, 100% nav)
        sumIf(dateDiff('second', ma.start_datetime_utc, ma.end_datetime_utc) / 3600.0,
              ma.action_type = 'PUBLIC_ORDER')
            AS sea_safety_heures_ordre_public,                                 -- 5.3
        countIf(ma.action_type = 'PUBLIC_ORDER')
            AS sea_safety_nb_operations_ordre_public                           -- 5.4

    FROM rapportnav_proxy.mission_action ma
    GROUP BY ma.mission_id
),

-- =====================================================================
-- BLOC ENV (monitorenv_proxy.env_actions + themes) — extraction JSON.
-- env_actions n'est lue qu'une seule fois (+ une lecture agrégée de
-- themes_env_actions), tout le calcul par domaine se fait ensuite en
-- conditionnel (sumIf/countIf/has) sur ces deux résultats.
-- =====================================================================
env_action_theme_ids AS (
    -- Un thème (ou sous-thème, stockés à plat) par ligne -> regroupés en
    -- tableau, une seule fois par action.
    SELECT
        env_actions_id AS action_id,
        groupArray(themes_id) AS theme_ids
    FROM monitorenv_proxy.themes_env_actions
    GROUP BY env_actions_id
),
env_agg AS (
    SELECT
        ea.mission_id AS mission_id,

        -- 3.3 Environmental Traffic (theme 103)
        sumIf(
            dateDiff('second', ea.action_start_datetime_utc, ea.action_end_datetime_utc) / 3600.0,
            has(ifNull(et.theme_ids, []), 103)
        ) AS env_traffic_heures_de_mer,                                        -- 3.3.1

        -- 4.1 Not Pollution Control Surveillance : CONTROL sans thème 19/102
        sumIf(
            dateDiff('second', ea.action_start_datetime_utc, ea.action_end_datetime_utc) / 3600.0,
            ea.action_type = 'CONTROL' AND NOT hasAny(ifNull(et.theme_ids, []), [19, 102])
        ) AS not_pollution_heures_de_mer,                                      -- 4.1.1
        sumIf(
            if(JSONHas(ea.value, 'actionNumberOfControls'), JSONExtractInt(ea.value, 'actionNumberOfControls'), 1),
            ea.action_type = 'CONTROL' AND NOT hasAny(ifNull(et.theme_ids, []), [19, 102])
        ) AS not_pollution_nb_operations,                                      -- 4.1.3 (approx : défaut 1 si absent)
        sumIf(
            length(arrayFilter(x -> length(JSONExtractArrayRaw(x, 'natinf')) > 0,
                                JSONExtractArrayRaw(ifNull(ea.value, ''), 'infractions'))),
            ea.action_type = 'CONTROL' AND NOT hasAny(ifNull(et.theme_ids, []), [19, 102])
        ) AS not_pollution_nb_infractions,                                     -- 4.1.4
        sumIf(
            length(arrayFilter(x -> JSONExtractString(x, 'infractionType') = 'WITH_REPORT',
                                JSONExtractArrayRaw(ifNull(ea.value, ''), 'infractions'))),
            ea.action_type = 'CONTROL' AND NOT hasAny(ifNull(et.theme_ids, []), [19, 102])
        ) AS not_pollution_nb_pv,                                              -- 4.1.5

        -- 4.2 Pollution (partie env) : thèmes 19/102
        sumIf(
            dateDiff('second', ea.action_start_datetime_utc, ea.action_end_datetime_utc) / 3600.0,
            hasAny(ifNull(et.theme_ids, []), [19, 102])
        ) AS pollution_heures_de_mer_env,                                      -- brique env de 4.2.1
        sumIf(
            arraySum(arrayMap(x -> length(JSONExtractArrayRaw(x, 'natinf')),
                               JSONExtractArrayRaw(ifNull(ea.value, ''), 'infractions'))),
            hasAny(ifNull(et.theme_ids, []), [19, 102])
        ) AS pollution_nb_infractions,                                         -- 4.2.5 (indisponible en nav, 100% env)
        sumIf(
            length(arrayFilter(x -> JSONExtractString(x, 'infractionType') = 'WITH_REPORT',
                                JSONExtractArrayRaw(ifNull(ea.value, ''), 'infractions'))),
            hasAny(ifNull(et.theme_ids, []), [19, 102])
        ) AS pollution_nb_pv,                                                  -- 4.2.6 (indisponible en nav, 100% env)

        -- 4.4 Cultural Maritime : theme 104 (+ sous-thème 165 pour 4.4.2)
        countIf(has(ifNull(et.theme_ids, []), 165))
            AS cultural_nb_operations_scientifiques,                           -- 4.4.2
        countIf(has(ifNull(et.theme_ids, []), 104))
            AS cultural_nb_operations_police_bcm,                              -- 4.4.3
        sumIf(
            dateDiff('second', ea.action_start_datetime_utc, ea.action_end_datetime_utc) / 3600.0,
            has(ifNull(et.theme_ids, []), 104)
        ) AS cultural_heures_de_mer,                                           -- 4.4.1

        -- brique env de 7.4 (contrôles + surveillances sur véhicule VESSEL)
        sumIf(
            if(JSONHas(ea.value, 'actionNumberOfControls'), JSONExtractInt(ea.value, 'actionNumberOfControls'), 0),
            JSONExtractString(ea.value, 'vehicleType') = 'VESSEL' AND ea.action_type = 'CONTROL'
        )
        + countIf(JSONExtractString(ea.value, 'vehicleType') = 'VESSEL' AND ea.action_type = 'SURVEILLANCE')
            AS nb_targets_control_env                                         -- brique env de 7.4

    FROM monitorenv_proxy.env_actions ea
    LEFT JOIN env_action_theme_ids et ON et.action_id = ea.id
    GROUP BY ea.mission_id
),

-- =====================================================================
-- BLOC FISH (monitorfish_proxy.mission_actions) — colonnes plates.
-- `infractions` est directement l'array JSON (pas imbriqué sous une clé).
-- ⚠️ mission_actions.mission_id référence la mission MonitorFish, PAS
-- l'id mission rapportnav : rapprochement à sécuriser (missions_pam.sql).
-- =====================================================================
fish_agg AS (
    SELECT
        fa.mission_id,
        sumIf(dateDiff('second', fa.action_datetime_utc, fa.action_end_datetime_utc) / 3600.0,
              fa.action_type IN ('SEA_CONTROL', 'LAND_CONTROL'))
            AS polpeche_heures_de_mer_fish,                                    -- brique fish de 4.3.1
        countIf(fa.action_type IN ('SEA_CONTROL', 'LAND_CONTROL'))
            AS polpeche_nb_operations,                                         -- 4.3.3
        countIf(fa.action_type = 'SEA_CONTROL')
            AS polpeche_nb_navires_inspectes,                                  -- 4.3.5
        sumIf(
            length(arrayFilter(x -> JSONExtractString(x, 'infractionType') = 'WITH_RECORD',
                                JSONExtractArrayRaw(ifNull(fa.infractions, '')))),
            fa.action_type = 'SEA_CONTROL'
        ) AS polpeche_nb_pv,                                                   -- 4.3.6
        sumIf(
            length(arrayFilter(x -> JSONHas(x, 'natinf'), JSONExtractArrayRaw(ifNull(fa.infractions, '')))),
            fa.action_type = 'SEA_CONTROL'
        ) AS polpeche_nb_infractions,                                          -- 4.3.7
        countIf(fa.action_type = 'SEA_CONTROL' AND fa.seizure_and_diversion = 1)
            AS polpeche_nb_navires_accompagnes_deroutes,                       -- 4.3.8
        sumIf(fa.species_quantity_seized, fa.action_type = 'SEA_CONTROL')
            AS polpeche_quantite_kg_saisie_rejetee                             -- 4.3.9
    FROM monitorfish_proxy.mission_actions fa
    GROUP BY fa.mission_id
),
fish_control_targets AS (   -- brique fish de 7.4 (chaque ligne SEA_CONTROL = 1 navire ciblé)
    SELECT mission_id, countIf(action_type = 'SEA_CONTROL') AS nb_targets_control_fish
    FROM monitorfish_proxy.mission_actions
    GROUP BY mission_id
)

-- =====================================================================
-- ASSEMBLAGE FINAL — une ligne par mission rapportnav
-- =====================================================================
SELECT
    toInt32(m.id) AS mission_id,

    -- --- Champs de filtrage / contexte du rapport ---
    toDateTime(m.start_datetime_utc) AS mission_date_debut,
    toDateTime(m.end_datetime_utc)   AS mission_date_fin,
    toString(mu.unit_names)          AS unite_nom,                            -- concat si mission inter-services
    toInt32(coalesce(mu.nb_unites_distinctes, 0)) AS unite_nb_distinctes,     -- >1 = mission inter-services

    -- Champs d'en-tête API AEM (aem.idUUID / serviceId / facade /
    -- missionTypes / missionSource / isDeleted), sourcés comme dans
    -- ComputeAEMData.execute() (rapportnav2).
    toString(gi.mission_id_uuid)          AS mission_id_uuid,                 -- aem.idUUID (mission_general_info.mission_id_uuid)
    toInt32(coalesce(gi.service_id, 0))   AS service_id,                      -- aem.serviceId (mission_general_info.service_id)
    toString(coalesce(m.facade, ''))      AS facade,                         -- aem.facade (monitorenv_proxy.missions.facade)
    arrayStringConcat(coalesce(m.mission_types, []), ', ') AS mission_types,  -- aem.missionTypes (text[] -> concat pour filtrage simple)
    toString(m.mission_source)            AS mission_source,                 -- aem.missionSource (enum monitorenv)
    -- ⚠️ colonne réelle "deleted", pas "is_deleted" (MissionModel.kt monitorenv).
    -- Non filtré par défaut : ajouter WHERE NOT is_deleted si besoin.
    toUInt8(m.deleted)                    AS is_deleted,

    -- Statut de mission calculé comme MissionEntity.calculateMissionStatus()
    -- (rapportnav2) : uniquement basé sur start/end vs. maintenant.
    toString(multiIf(
        m.end_datetime_utc IS NULL OR m.start_datetime_utc IS NULL, 'UNAVAILABLE',
        m.start_datetime_utc < now() AND m.end_datetime_utc > now(), 'IN_PROGRESS',
        m.end_datetime_utc <= now(), 'ENDED',
        m.start_datetime_utc >= now(), 'UPCOMING',
        'UNAVAILABLE'
    )) AS mission_status,
    -- Proxy de df.isMissionFinished utilisé par le flow Prefect pour
    -- exclure les missions ouvertes/en cours du calcul des KPI.
    toUInt8(m.end_datetime_utc IS NOT NULL AND m.end_datetime_utc < now()) AS is_mission_finished,
    -- Brique nav uniquement de la complétude stats -- pas équivalente à
    -- completenessForStats_status == 'COMPLETE' du flow Prefect (cf.
    -- nav_completeness plus haut). A combiner avec mission_status =
    -- 'ENDED' pour un filtre "missions closes" fiable.
    toUInt8(coalesce(nc.toutes_actions_nav_completes, 0)) AS nav_toutes_actions_completes,
    toInt32(coalesce(nc.nb_actions_nav_incompletes, 0))   AS nav_nb_actions_incompletes,

    -- 1.1 / 1.2 / 2 / 3.4 / 5 : voir nav_agg (100% nav)
    toFloat64(coalesce(n.oom_rescue_heures_de_mer, 0))                    AS oom_rescue_heures_de_mer,
    toInt64(coalesce(n.oom_rescue_nb_operations, 0))                      AS oom_rescue_nb_operations,
    toInt64(coalesce(n.oom_rescue_nb_personnes_secourues, 0))             AS oom_rescue_nb_personnes_secourues,
    toFloat64(coalesce(n.sar_migrants_heures_de_mer, 0))                  AS sar_migrants_heures_de_mer,
    toInt64(coalesce(n.sar_migrants_nb_operations, 0))                    AS sar_migrants_nb_operations,
    toInt64(coalesce(n.sar_migrants_nb_embarcations_suivies_sans_intervention, 0)) AS sar_migrants_nb_embarcations_suivies_sans_intervention,
    toInt64(coalesce(n.sar_migrants_nb_embarcations_assistees_retour_terre, 0))    AS sar_migrants_nb_embarcations_assistees_retour_terre,
    toInt64(coalesce(n.sar_migrants_nb_operations_sauvetage, 0))          AS sar_migrants_nb_operations_sauvetage,
    toInt64(coalesce(n.sar_migrants_nb_personnes_secourues, 0))           AS sar_migrants_nb_personnes_secourues,
    toFloat64(coalesce(n.aned_heures_de_mer, 0))                          AS aned_heures_de_mer,
    toInt64(coalesce(n.aned_nb_operations, 0))                            AS aned_nb_operations,
    toInt64(coalesce(n.aned_nb_interventions_mise_en_demeure, 0))         AS aned_nb_interventions_mise_en_demeure,
    toInt64(coalesce(n.aned_nb_remorquages, 0))                           AS aned_nb_remorquages,
    toFloat64(coalesce(n.illegal_immig_heures_de_mer, 0))                 AS illegal_immig_heures_de_mer,
    toInt64(coalesce(n.illegal_immig_nb_navires_interceptes, 0))          AS illegal_immig_nb_navires_interceptes,
    toInt64(coalesce(n.illegal_immig_nb_migrants_interceptes, 0))         AS illegal_immig_nb_migrants_interceptes,
    toInt64(coalesce(n.illegal_immig_nb_passeurs_suspectes, 0))           AS illegal_immig_nb_passeurs_suspectes,
    toFloat64(coalesce(n.sea_safety_heures_de_mer, 0))                    AS sea_safety_heures_de_mer,
    toFloat64(coalesce(n.sea_safety_heures_ordre_public, 0))              AS sea_safety_heures_ordre_public,
    toInt64(coalesce(n.sea_safety_nb_operations_ordre_public, 0))         AS sea_safety_nb_operations_ordre_public,

    -- 3.3 Environmental Traffic (100% env ; redirect/saisie = TODO backend, toujours 0)
    toFloat64(coalesce(e.env_traffic_heures_de_mer, 0))                   AS env_traffic_heures_de_mer,       -- 3.3.1
    toInt64(0) AS env_traffic_nb_navires_derouted_saisis,                                                     -- 3.3.3 (TODO backend, jamais implémenté)
    toInt64(0) AS env_traffic_nb_saisies,                                                                     -- 3.3.4 (TODO backend, jamais implémenté)

    -- 4.1 Not Pollution Control Surveillance (100% env)
    toFloat64(coalesce(e.not_pollution_heures_de_mer, 0))                 AS not_pollution_heures_de_mer,
    toInt64(coalesce(e.not_pollution_nb_operations, 0))                   AS not_pollution_nb_operations,
    toInt64(coalesce(e.not_pollution_nb_infractions, 0))                  AS not_pollution_nb_infractions,
    toInt64(coalesce(e.not_pollution_nb_pv, 0))                           AS not_pollution_nb_pv,

    -- 4.2 Pollution Control Surveillance (nav + env réunis)
    toFloat64(coalesce(n.pollution_heures_de_mer_nav, 0) + coalesce(e.pollution_heures_de_mer_env, 0)) AS pollution_heures_de_mer, -- 4.2.1
    toInt64(coalesce(n.pollution_nb_operations_simple_brassage, 0))       AS pollution_nb_operations_simple_brassage,  -- 4.2.3
    toInt64(coalesce(n.pollution_nb_dispositifs_deployes, 0))             AS pollution_nb_dispositifs_deployes,        -- 4.2.4
    toInt64(coalesce(e.pollution_nb_infractions, 0))                      AS pollution_nb_infractions,                 -- 4.2.5
    toInt64(coalesce(e.pollution_nb_pv, 0))                               AS pollution_nb_pv,                          -- 4.2.6
    toInt64(coalesce(n.pollution_nb_deroutements_nav, 0))                 AS pollution_nb_deroutements_nav,            -- 4.2.7 (partie env = TODO 0 côté backend)
    toInt64(coalesce(n.pollution_nb_pollutions_detectees_nav, 0))         AS pollution_nb_pollutions_detectees_nav,    -- 4.2.8 (partie env = TODO 0 côté backend)

    -- 4.3 Illegal Fish (nav + fish réunis -- sous réserve de la fiabilité
    -- du rapprochement de mission_id fish <-> rapportnav)
    toFloat64(coalesce(hm.heures_de_mer_nav_ancrage_navigation, 0) + coalesce(f.polpeche_heures_de_mer_fish, 0)) AS polpeche_heures_de_mer, -- 4.3.1
    toInt64(coalesce(f.polpeche_nb_operations, 0))                        AS polpeche_nb_operations,
    toInt64(coalesce(f.polpeche_nb_navires_inspectes, 0))                 AS polpeche_nb_navires_inspectes,
    toInt64(coalesce(f.polpeche_nb_pv, 0))                                AS polpeche_nb_pv,
    toInt64(coalesce(f.polpeche_nb_infractions, 0))                       AS polpeche_nb_infractions,
    toInt64(coalesce(f.polpeche_nb_navires_accompagnes_deroutes, 0))      AS polpeche_nb_navires_accompagnes_deroutes,
    toFloat64(coalesce(f.polpeche_quantite_kg_saisie_rejetee, 0))         AS polpeche_quantite_kg_saisie_rejetee,

    -- 4.4 Cultural Maritime (100% env)
    toFloat64(coalesce(e.cultural_heures_de_mer, 0))                      AS cultural_heures_de_mer,
    toInt64(coalesce(e.cultural_nb_operations_scientifiques, 0))          AS cultural_nb_operations_scientifiques,
    toInt64(coalesce(e.cultural_nb_operations_police_bcm, 0))             AS cultural_nb_operations_police_bcm,

    -- 7 Sovereign Protect (nav + env + fish)
    toFloat64(coalesce(hm.heures_de_mer_nav_ancrage_navigation, 0))       AS sovereign_heures_de_mer,          -- 7.1 (100% nav)
    toInt32(coalesce(gi.nbr_of_recognized_vessel, 0))                     AS sovereign_nb_navires_reconnus,    -- 7.3 (saisie manuelle, mission_general_info)
    toInt64(
        coalesce(ct.nb_targets_control_nav, 0) + coalesce(e.nb_targets_control_env, 0) + coalesce(fct.nb_targets_control_fish, 0)
    ) AS sovereign_nb_navires_controles                                                                        -- 7.4

-- rapportnav_proxy.mission (UUID) n'est pas la bonne table pivot puisque
-- toutes les jointures se font sur le mission_id Int32 : on pivote donc
-- sur monitorenv_proxy.missions, source du référentiel d'id entier.
FROM monitorenv_proxy.missions m
LEFT JOIN nav_agg n              ON n.mission_id = m.id
LEFT JOIN heures_de_mer_nav hm   ON hm.mission_id = m.id
LEFT JOIN control_targets_nav ct ON ct.mission_id = m.id
LEFT JOIN mission_units mu       ON mu.mission_id = m.id
-- INNER JOIN volontaire (pas LEFT) : n'inclut que les missions ayant au
-- moins une unité PAM ou ULAM rattachée.
INNER JOIN missions_pam_ulam pu  ON pu.mission_id = m.id
LEFT JOIN nav_completeness nc    ON nc.mission_id = m.id
LEFT JOIN rapportnav_proxy.mission_general_info gi ON gi.mission_id = m.id
LEFT JOIN env_agg e              ON e.mission_id = m.id           -- ⚠️ jointure mission_id à sécuriser (idem pour fish ci-dessous)
LEFT JOIN fish_agg f              ON f.mission_id = m.id          -- ⚠️ idem
LEFT JOIN fish_control_targets fct ON fct.mission_id = m.id       -- ⚠️ idem

-- Filtre repris de forklift/.../queries/monitorenv_remote/missions.sql
-- (requête qui sélectionne les mission_ids envoyés à l'endpoint AEM, en
-- amont de l'appel API). Aucune restriction d'unité (contrairement à
-- missions_pam.sql, utilisée pour l'endpoint PATROL, qui restreint en
-- plus aux 4 control_unit_id des PAM : 10404, 10080, 10141, 10121).
-- rapportnav.aem côté DWH couvre donc TOUTES les unités (PAM + ULAM),
-- pas seulement les PAM, malgré ce que le nom du rapport suggère.
-- ⚠️ Date du 2025-01-01 codée en dur, sans commentaire dans le code
-- source expliquant le choix — à vérifier avec Alexandre avant de la
-- reprendre telle quelle comme filtre "officiel".
WHERE m.start_datetime_utc >= toDateTime('2025-01-01 00:00:00')
ORDER BY m.start_datetime_utc desc

-- =====================================================================
-- ⚠️ Piège identifié dans le flow Prefect, NON reproduit ici (à traiter
-- côté BI/Metabase si besoin de comparer avec rapportnav.aem du DWH) :
-- dans _process_data() (extract_rapportnav_analytics.py), les colonnes
-- numériques manquantes sont remplies avec -1, PAS 0. Dans rapportnav.aem,
-- une valeur -1 veut dire "donnée absente/non calculée", pas "zéro
-- occurrence" -- un SUM()/AVG() naïf sur ces colonnes fausse les totaux.
-- Cette requête-ci ressort les valeurs manquantes en NULL, pas en -1.
-- =====================================================================
