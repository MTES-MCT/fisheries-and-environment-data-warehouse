-- =====================================================================
-- Une ligne par mission = TOUS les indicateurs AEM (AEMTableExport),
-- colonnes renommées pour alignement 1:1 avec le référentiel officiel
-- (colonne "Indicateur" du tableau de règles de calcul).
--
-- Bases/schemas :
--   rapportnav_proxy  (mission_action, target_2, mission_general_info)
--   monitorenv_proxy  (missions, env_actions, themes, themes_env_actions,
--                       missions_control_units, control_units)
--   monitorfish_proxy (mission_actions)
--
-- Hypothèses non vérifiables sans accès direct aux bases :
--   - `value` (env_actions) et `infractions` (mission_actions) supposées
--     de type String contenant du JSON brut.
--   - noms de clé JSON en camelCase (sérialisation Jackson côté backend).
--
-- Points de vigilance :
-- 1) [tech: env] monitorenv_proxy.env_actions : tout le détail métier est
--    dans la colonne `value` (JSON), pas en colonnes.
-- 2) [tech: fish] monitorfish_proxy.mission_actions : colonnes plates sauf
--    `infractions` (array JSON directement, pas imbriqué sous une clé).
-- 3) [tech: nav/env/fish] Rapprochement mission_id entre rapportnav /
--    monitorenv / monitorfish : pas le même référentiel d'ID a priori --
--    jointure directe à sécuriser.
-- 4) [tech: env/nav] TODO backend jamais implémentés, toujours 0 :
--    envTraffic.nbrOfRedirectShip (3.3.3), envTraffic.nbrOfSeizure (3.3.4),
--    7.5/7.6 piraterie-brigandage (indicateurs nouveaux, aucune règle de
--    calcul fournie par le référentiel, aucun action_type dédié identifié).
-- 5) [tech: nav] PAM/ULAM : rapportnav_proxy.service.service_type est une
--    colonne dédiée réelle (ServiceTypeEnum), pas recalculée. Le rapport
--    AEM couvre PAR DÉFAUT toutes les unités (PAM + ULAM) -- malgré son
--    nom, il n'est PAS restreint aux PAM. unite_nom (contient déjà
--    PAM/ULAM en clair) permet un filtrage texte optionnel côté Metabase.
-- 6) [métier] "Complétude pour stats" : pas de champ mission-level stocké
--    à ce jour. La vraie règle (MissionEntity.isCompleteForStats())
--    combine 3 validateurs, non reproduits ici. mission_status = 'ENDED'
--    est le filtre "missions closes" le plus fiable en attendant ;
--    nav_toutes_actions_completes n'est qu'une brique partielle (nav
--    uniquement). Un champ dédié est prévu côté source -- filtre
--    commenté ajouté dans le WHERE final, à activer dès que le champ
--    sera disponible.
-- 7) [tech: env] Filtre sur les missions supprimées : uniquement
--    monitorenv_proxy.missions.deleted (WHERE final). Aucun flag
--    "deleted"/"is_deleted" n'existe sur mission_action, env_actions ni
--    monitorfish_proxy.mission_actions (vérifié dans leurs entités JPA
--    respectives) -- le filtre ne peut donc porter que sur les missions
--    monitorenv, pas sur les actions individuellement.
-- 8) [métier] 4.3.1 et 7.1 ont des définitions différentes : 4.3.1 =
--    statuts "navigation" seuls, 7.1 = statuts "navigation" + "mouillage".
-- 9) [tech: nav] Piège côté flow Prefect (extract_rapportnav_analytics.py),
--    NON reproduit ici : les colonnes numériques manquantes y sont
--    remplies avec -1, pas 0/NULL. Cette requête-ci ressort les valeurs
--    manquantes en NULL -- à garder en tête si comparaison avec
--    rapportnav.aem (DWH).
-- 10) [tech: nav/env/fish] Garde-fou "fin >= début" appliqué UNIQUEMENT
--     sur heures_de_mer_nav (7.1/4.3.1). Les autres dateDiff (RESCUE,
--     ANTI_POLLUTION, ILLEGAL_IMMIGRATION, VIGIMER/BAAEM, PUBLIC_ORDER/
--     NAUTICAL_EVENT côté nav ; env_actions et mission_actions fish
--     côté durée) n'ont PAS ce garde-fou -- une ligne désordonnée y
--     soustrairait silencieusement des heures. Pas corrigé partout pour
--     ne pas alourdir chaque sumIf sans données réelles pour juger de
--     la fréquence du problème -- à surveiller si des totaux paraissent
--     anormalement bas.
-- 11) [tech: env] Jointure env_action_theme_ids <-> env_actions castée en
--     toString() des deux côtés (même risque UUID natif / String déjà
--     rencontré sur target_2/control_2/infraction_2).
-- 12) [métier] gi.service_id (mission_general_info) est la source de
--     l'unité (unite_nom, aem.serviceId). Cette colonne n'a jamais été
--     vérifiée dans le code source (supposée par analogie avec
--     analytics_missions_full_data.sql) -- à confirmer si unite_nom
--     reste vide.
-- =====================================================================

WITH
status_actions AS (
    SELECT
        ma.mission_id,
        ma.status,
        ma.start_datetime_utc,
        -- [tech: nav] Frame ROWS BETWEEN ... explicite indispensable :
        -- sans lui, ClickHouse applique par défaut RANGE BETWEEN
        -- UNBOUNDED PRECEDING AND CURRENT ROW, qui n'inclut jamais la
        -- ligne suivante -- leadInFrame(x, 1, default) retombe alors
        -- systématiquement sur le default pour CHAQUE ligne (constaté
        -- sur données réelles : corrected_end_datetime_utc identique à
        -- envm.end_datetime_utc pour toutes les lignes d'une mission).
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
-- Garde-fou corrected_end_datetime_utc >= start_datetime_utc : sans lui,
-- une ligne STATUS désordonnée produit un dateDiff négatif qui vient
-- silencieusement soustraire des heures plutôt que d'en ajouter (même
-- classe de bug que celui déjà corrigé sur build_action_status_durations.sql,
-- ici appliqué en filtre plutôt qu'en garde pour range()).
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
        -- [métier] COUNT (pas DISTINCT) volontaire : la définition de 7.4
        -- dit "somme des cibles contrôlées" -- un même navire contrôlé
        -- deux fois dans la mission doit compter deux fois (deux
        -- opérations de contrôle distinctes). COUNT(DISTINCT t.id) serait
        -- de toute façon un no-op puisque id est la clé primaire de
        -- target_2, déjà unique par ligne. À confirmer avec le métier que
        -- cette lecture de "somme des cibles" est la bonne avant de
        -- publier ce chiffre.
        countIf(toString(t.id) != '00000000-0000-0000-0000-000000000000') AS nb_targets_control_nav
    FROM rapportnav_proxy.mission_action ma
    LEFT JOIN rapportnav_proxy.target_2 t ON toString(t.action_id) = toString(ma.id)
    WHERE ma.action_type = 'CONTROL'
    GROUP BY ma.mission_id
),

mission_units AS (
    -- conservée uniquement pour unite_nb_distinctes (détection missions
    -- inter-services) -- rapportnav_proxy.service ne porte qu'une seule
    -- unité par mission (mission.service_id), donc ne peut pas remplacer
    -- cet usage précis.
    SELECT
        mcu.mission_id,
        COUNT(DISTINCT cu.id) AS nb_unites_distinctes
    FROM monitorenv_proxy.missions_control_units mcu
    INNER JOIN monitorenv_proxy.control_units cu ON cu.id = mcu.control_unit_id
    GROUP BY mcu.mission_id
),

-- [tech: nav] nom d'unité et type sourcés depuis rapportnav_proxy.service,
-- via mission_general_info.service_id (colonne également utilisée plus
-- bas pour service_id/aem.serviceId). service_type est une colonne
-- dédiée réelle (ServiceTypeEnum, vérifié dans ServiceModel.kt). La
-- bordée est comprise dans le texte de unit_name, pas exposée séparément.
mission_service AS (
    SELECT
        gi.mission_id,
        sv.name         AS unit_name,
        sv.service_type
    FROM rapportnav_proxy.mission_general_info gi
    LEFT JOIN rapportnav_proxy.service sv ON sv.id = gi.service_id
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

        -- 1.1 Sauvegarde de la vie humaine hors phénomène migratoire
        sumIf(dateDiff('second', ma.start_datetime_utc, ma.end_datetime_utc) / 3600.0,
              ma.action_type = 'RESCUE' AND coalesce(toUInt8(ma.is_migration_rescue), 0) = 0)
            AS n1_1_1_nb_heures_de_mer,
        countIf(ma.action_type = 'RESCUE' AND coalesce(toUInt8(ma.is_migration_rescue), 0) = 0)
            AS n1_1_3_nb_operations_conduites,
        sumIf(ma.number_persons_rescued, ma.action_type = 'RESCUE' AND coalesce(toUInt8(ma.is_migration_rescue), 0) = 0)
            AS n1_1_4_nb_personnes_secourues,

        -- 1.2 Sauvegarde de la vie humaine dans le cadre d'un phénomène migratoire
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
            AS n1_2_6_nb_operations_sauvetage,   -- doublon assumé de 1.2.3, cf. TODO code source rapportnav2
        sumIf(ma.number_persons_rescued, ma.action_type = 'RESCUE' AND toUInt8(ma.is_migration_rescue) = 1)
            AS n1_2_7_nb_personnes_secourues,

        -- 2 Assistance aux navires en difficulté et sécurité maritime
        sumIf(dateDiff('second', ma.start_datetime_utc, ma.end_datetime_utc) / 3600.0,
              ma.action_type = 'RESCUE' AND toUInt8(ma.is_vessel_rescue) = 1)
            AS n2_1_nb_heures_de_mer,
        countIf(ma.action_type = 'RESCUE' AND toUInt8(ma.is_vessel_rescue) = 1)
            AS n2_3_nb_operations,
        countIf(ma.action_type = 'RESCUE' AND toUInt8(ma.is_vessel_rescue) = 1 AND toUInt8(ma.is_vessel_noticed) = 1)
            AS n2_4_nb_mise_en_demeure,
        countIf(ma.action_type = 'RESCUE' AND toUInt8(ma.is_vessel_rescue) = 1 AND toUInt8(ma.is_vessel_towed) = 1)
            AS n2_7_nb_remorquages,

        -- 3.4 Lutte contre l'immigration illégale par voie maritime
        sumIf(dateDiff('second', ma.start_datetime_utc, ma.end_datetime_utc) / 3600.0,
              ma.action_type = 'ILLEGAL_IMMIGRATION')
            AS n3_4_1_nb_heures_de_mer,
        sumIf(ma.nb_of_intercepted_vessels, ma.action_type = 'ILLEGAL_IMMIGRATION')
            AS n3_4_3_nb_navires_interceptes,
        sumIf(ma.nb_of_intercepted_migrants, ma.action_type = 'ILLEGAL_IMMIGRATION')
            AS n3_4_4_nb_migrants_interceptes,
        sumIf(ma.nb_of_suspected_smugglers, ma.action_type = 'ILLEGAL_IMMIGRATION')
            AS n3_4_5_nb_passeurs_presumes_interceptes,

        -- 4.2 Répression rejets illicites / lutte pollutions (partie nav)
        sumIf(dateDiff('second', ma.start_datetime_utc, ma.end_datetime_utc) / 3600.0,
              ma.action_type = 'ANTI_POLLUTION')
            AS pollution_heures_de_mer_nav,   -- brique nav de 4.2.1, combinée plus bas avec la brique env
        countIf(ma.action_type = 'ANTI_POLLUTION' AND toUInt8(ma.simple_brewing_operation) = 1)
            AS n4_2_3_participation_brassage_simple,
        countIf(ma.action_type = 'ANTI_POLLUTION' AND toUInt8(ma.anti_pol_device_deployed) = 1)
            AS n4_2_4_nb_dispositifs_deployes,
        countIf(ma.action_type = 'ANTI_POLLUTION' AND toUInt8(ma.diversion_carried_out) = 1)
            AS n4_2_7_nb_deroutements_nav,   -- brique nav uniquement, cf. TODO backend pour la partie env
        countIf(ma.action_type = 'ANTI_POLLUTION' AND toUInt8(ma.pollution_observed_by_authorized_agent) = 1)
            AS n4_2_8_nb_pollutions_detectees_nav,   -- brique nav uniquement, cf. TODO backend pour la partie env

        -- 5 Sûreté maritime et maintien de l'ordre public en mer
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

        -- 3.3 Lutte contre le trafic en mer (espèces protégées, thème 103)
        sumIf(
            dateDiff('second', ea.action_start_datetime_utc, ea.action_end_datetime_utc) / 3600.0,
            has(ifNull(et.theme_ids, []), 103)
        ) AS n3_3_1_nb_heures_de_mer,

        -- 4.1 Surveillance et contrôles environnement (hors rejets illicites)
        -- action_type couvre CONTROL + SURVEILLANCE ("contrôles et
        -- surveillances" dans la définition officielle).
        -- [métier] à confirmer que CONTROL et SURVEILLANCE doivent bien
        -- être additionnés sans distinction pour 4.1.1/4.1.3/4.1.4/4.1.5
        -- -- le libellé de l'indicateur les regroupe, mais aucune
        -- vérification terrain n'a été faite sur des données réelles à
        -- ce stade.
        sumIf(
            dateDiff('second', ea.action_start_datetime_utc, ea.action_end_datetime_utc) / 3600.0,
            ea.action_type IN ('CONTROL', 'SURVEILLANCE') AND NOT hasAny(ifNull(et.theme_ids, []), [19, 102])
        ) AS n4_1_1_nb_heures_de_mer,
        sumIf(
            if(JSONHas(ea.value, 'actionNumberOfControls'), JSONExtractInt(ea.value, 'actionNumberOfControls'), 0),
            ea.action_type = 'CONTROL' AND JSONExtractString(ea.value, 'vehicleType') = 'VESSEL'
                AND NOT hasAny(ifNull(et.theme_ids, []), [19, 102])
        )
        + countIf(
            ea.action_type = 'SURVEILLANCE' AND JSONExtractString(ea.value, 'vehicleType') = 'VESSEL'
                AND NOT hasAny(ifNull(et.theme_ids, []), [19, 102])
        )
            AS n4_1_3_nb_operations,
            -- [métier] reprend la même logique que
            -- nb_targets_control_env (7.4) -- compte les cibles VESSEL
            -- réellement contrôlées plutôt qu'un défaut fixe de 1 par
            -- action. MAIS actionNumberOfControls est un champ JSON dont
            -- on ne sait pas avec certitude s'il représente exactement le
            -- nombre de cibles VESSEL distinctes du contrôle (un contrôle
            -- pourrait en théorie viser plusieurs navires) -- à confirmer
            -- avec le métier avant de publier ce chiffre en dashboard.
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

        -- 4.2 Répression rejets illicites / pollutions (partie env, thèmes 19/102)
        sumIf(
            dateDiff('second', ea.action_start_datetime_utc, ea.action_end_datetime_utc) / 3600.0,
            hasAny(ifNull(et.theme_ids, []), [19, 102])
        ) AS pollution_heures_de_mer_env,   -- combinée plus bas avec la brique nav pour 4.2.1
        sumIf(
            arraySum(arrayMap(x -> length(JSONExtractArrayRaw(x, 'natinf')),
                               JSONExtractArrayRaw(ifNull(ea.value, ''), 'infractions'))),
            hasAny(ifNull(et.theme_ids, []), [19, 102])
        ) AS n4_2_5_nb_infractions,   -- indisponible côté nav, 100% env
        sumIf(
            length(arrayFilter(x -> JSONExtractString(x, 'infractionType') = 'WITH_REPORT',
                                JSONExtractArrayRaw(ifNull(ea.value, ''), 'infractions'))),
            hasAny(ifNull(et.theme_ids, []), [19, 102])
        ) AS n4_2_6_nb_pv,   -- indisponible côté nav, 100% env

        -- 4.4 Protection des biens culturels maritimes (thème 104, sous-thème 165)
        countIf(has(ifNull(et.theme_ids, []), 165))
            AS n4_4_2_nb_operations_scientifiques,
        countIf(has(ifNull(et.theme_ids, []), 104))
            AS n4_4_3_nb_operations_police_bcm,
        sumIf(
            dateDiff('second', ea.action_start_datetime_utc, ea.action_end_datetime_utc) / 3600.0,
            has(ifNull(et.theme_ids, []), 104)
        ) AS n4_4_1_nb_heures_de_mer,

        -- brique env de 7.4 (contrôles + surveillances sur véhicule VESSEL)
        sumIf(
            if(JSONHas(ea.value, 'actionNumberOfControls'), JSONExtractInt(ea.value, 'actionNumberOfControls'), 0),
            JSONExtractString(ea.value, 'vehicleType') = 'VESSEL' AND ea.action_type = 'CONTROL'
        )
        + countIf(JSONExtractString(ea.value, 'vehicleType') = 'VESSEL' AND ea.action_type = 'SURVEILLANCE')
            AS nb_targets_control_env   -- brique env de 7.4

    FROM monitorenv_proxy.env_actions ea
    LEFT JOIN env_action_theme_ids et ON toString(et.action_id) = toString(ea.id)
    GROUP BY ea.mission_id
),

fish_agg AS (
    SELECT
        fa.mission_id,
        -- 4.3.1 : contribution fish, durée fin - début des actions
        -- de contrôle CNSP.
        -- [métier] BLOQUANT constaté sur données réelles : sur la mission
        -- vérifiée, TOUS les action_end_datetime_utc des actions fish
        -- sont manquants -- dateDiff(start, NULL) renvoie NULL pour
        -- chaque ligne, donc n4_3_1_nb_heures_de_mer_fish ressort à 0
        -- (ou NULL) systématiquement tant que action_end_datetime_utc
        -- n'est pas renseigné côté source MonitorFish.
        -- [tech: fish] Aucun champ de durée alternatif n'existe --
        -- vérifié colonne par colonne dans MissionActionEntity.kt
        -- (monitorfish) : seules action_datetime_utc et
        -- action_end_datetime_utc portent une notion temporelle sur
        -- cette table, rien d'autre à substituer. À trancher avec le
        -- métier/CNSP sur le remplissage réel de ce champ en pratique.
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
    WHERE fa.completion = 'COMPLETED'  -- [tech: fish] colonne réelle (mission_action_completion), vérifiée dans MissionActionEntity.kt
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
    toInt32(coalesce(mu.nb_unites_distinctes, 0)) AS unite_nb_distinctes,

    toString(gi.mission_id_uuid)          AS mission_id_uuid,
    toInt32(coalesce(gi.service_id, 0))   AS service_id,
    toString(coalesce(m.facade, ''))      AS facade,
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
    toFloat64(coalesce(n.n1_1_1_nb_heures_de_mer, 0))              AS n1_1_1_nb_heures_de_mer,
    toInt64(coalesce(n.n1_1_3_nb_operations_conduites, 0))         AS n1_1_3_nb_operations_conduites,
    toInt64(coalesce(n.n1_1_4_nb_personnes_secourues, 0))          AS n1_1_4_nb_personnes_secourues,
    -- 1.2
    toFloat64(coalesce(n.n1_2_1_nb_heures_de_mer, 0))                          AS n1_2_1_nb_heures_de_mer,
    toInt64(coalesce(n.n1_2_3_nb_operations_conduites, 0))                     AS n1_2_3_nb_operations_conduites,
    toInt64(coalesce(n.n1_2_4_nb_embarcations_suivies_sans_intervention, 0))   AS n1_2_4_nb_embarcations_suivies_sans_intervention,
    toInt64(coalesce(n.n1_2_5_nb_embarcations_assistees_retour_terre, 0))      AS n1_2_5_nb_embarcations_assistees_retour_terre,
    toInt64(coalesce(n.n1_2_6_nb_operations_sauvetage, 0))                     AS n1_2_6_nb_operations_sauvetage,
    toInt64(coalesce(n.n1_2_7_nb_personnes_secourues, 0))                      AS n1_2_7_nb_personnes_secourues,
    -- 2
    toFloat64(coalesce(n.n2_1_nb_heures_de_mer, 0))          AS n2_1_nb_heures_de_mer,
    toInt64(coalesce(n.n2_3_nb_operations, 0))                AS n2_3_nb_operations,
    toInt64(coalesce(n.n2_4_nb_mise_en_demeure, 0))            AS n2_4_nb_mise_en_demeure,
    toInt64(coalesce(n.n2_7_nb_remorquages, 0))                 AS n2_7_nb_remorquages,

    -- 3.3
    toFloat64(coalesce(e.n3_3_1_nb_heures_de_mer, 0))   AS n3_3_1_nb_heures_de_mer,
    toInt64(0)                                            AS n3_3_3_nb_navires_deroutes_ou_saisis,   -- TODO backend, jamais implémenté
    toInt64(0)                                             AS n3_3_4_nb_saisies,                       -- TODO backend, jamais implémenté

    -- 3.4
    toFloat64(coalesce(n.n3_4_1_nb_heures_de_mer, 0))                  AS n3_4_1_nb_heures_de_mer,
    toInt64(coalesce(n.n3_4_3_nb_navires_interceptes, 0))               AS n3_4_3_nb_navires_interceptes,
    toInt64(coalesce(n.n3_4_4_nb_migrants_interceptes, 0))               AS n3_4_4_nb_migrants_interceptes,
    toInt64(coalesce(n.n3_4_5_nb_passeurs_presumes_interceptes, 0))       AS n3_4_5_nb_passeurs_presumes_interceptes,

    -- 4.1
    toFloat64(coalesce(e.n4_1_1_nb_heures_de_mer, 0))  AS n4_1_1_nb_heures_de_mer,
    toInt64(coalesce(e.n4_1_3_nb_operations, 0))        AS n4_1_3_nb_operations,
    toInt64(coalesce(e.n4_1_4_nb_infractions, 0))        AS n4_1_4_nb_infractions,
    toInt64(coalesce(e.n4_1_5_nb_pv, 0))                  AS n4_1_5_nb_pv,

    -- 4.2 (nav + env réunis pour 4.2.1)
    toFloat64(coalesce(n.pollution_heures_de_mer_nav, 0) + coalesce(e.pollution_heures_de_mer_env, 0)) AS n4_2_1_nb_heures_de_mer,
    toInt64(coalesce(n.n4_2_3_participation_brassage_simple, 0))  AS n4_2_3_participation_brassage_simple,
    toInt64(coalesce(n.n4_2_4_nb_dispositifs_deployes, 0))         AS n4_2_4_nb_dispositifs_deployes,
    toInt64(coalesce(e.n4_2_5_nb_infractions, 0))                   AS n4_2_5_nb_infractions,
    toInt64(coalesce(e.n4_2_6_nb_pv, 0))                             AS n4_2_6_nb_pv,
    toInt64(coalesce(n.n4_2_7_nb_deroutements_nav, 0))                AS n4_2_7_nb_deroutements,   -- brique nav uniquement, env = TODO backend
    toInt64(coalesce(n.n4_2_8_nb_pollutions_detectees_nav, 0))         AS n4_2_8_nb_pollutions_detectees,  -- brique nav uniquement, env = TODO backend

    -- 4.3 (nav [statuts navigation seuls] + fish [durée des contrôles] pour 4.3.1)
    toFloat64(coalesce(hm.heures_de_mer_nav_navigation_seule, 0) + coalesce(f.n4_3_1_nb_heures_de_mer_fish, 0)) AS n4_3_1_nb_heures_de_mer,
    toInt64(coalesce(f.n4_3_3_nb_operations_polpeche, 0))           AS n4_3_3_nb_operations_polpeche,
    toInt64(coalesce(f.n4_3_5_nb_navires_inspectes, 0))              AS n4_3_5_nb_navires_inspectes,
    toInt64(coalesce(f.n4_3_6_nb_pv_mer, 0))                          AS n4_3_6_nb_pv_mer,
    toInt64(coalesce(f.n4_3_7_nb_infractions_mer, 0))                  AS n4_3_7_nb_infractions_mer,
    toInt64(coalesce(f.n4_3_8_nb_navires_accompagnes_deroutes, 0))     AS n4_3_8_nb_navires_accompagnes_deroutes,
    toFloat64(coalesce(f.n4_3_9_quantite_kg_saisie, 0))                 AS n4_3_9_quantite_kg_saisie,

    -- 4.4
    toFloat64(coalesce(e.n4_4_1_nb_heures_de_mer, 0))          AS n4_4_1_nb_heures_de_mer,
    toInt64(coalesce(e.n4_4_2_nb_operations_scientifiques, 0))  AS n4_4_2_nb_operations_scientifiques,
    toInt64(coalesce(e.n4_4_3_nb_operations_police_bcm, 0))      AS n4_4_3_nb_operations_police_bcm,

    -- 5
    toFloat64(coalesce(n.n5_1_nb_heures_de_mer_surete_maritime, 0)) AS n5_1_nb_heures_de_mer_surete_maritime,
    toFloat64(coalesce(n.n5_3_nb_heures_de_mer_ordre_public, 0))     AS n5_3_nb_heures_de_mer_ordre_public,
    toInt64(coalesce(n.n5_4_nb_operations_ordre_public, 0))           AS n5_4_nb_operations_ordre_public,

    -- 7 (navigation + mouillage pour 7.1)
    toFloat64(coalesce(hm.heures_de_mer_nav_ancrage_navigation, 0)) AS n7_1_nb_heures_de_mer,
    toInt32(coalesce(gi.nbr_of_recognized_vessel, 0))                AS n7_3_nb_navires_reconnus,
    toInt64(
        coalesce(ct.nb_targets_control_nav, 0) + coalesce(e.nb_targets_control_env, 0) + coalesce(fct.nb_targets_control_fish, 0)
    ) AS n7_4_nb_controles_en_mer,

    -- 7.5/7.6 : nouveaux indicateurs (piraterie/brigandage), aucune règle
    -- de calcul fournie par le référentiel ("TODO nouveau") -- pas
    -- d'action_type dédié identifié dans ActionType.kt (rapportnav2) à
    -- ce jour. Placeholders à 0, même traitement que 3.3.3/3.3.4.
    -- [métier] à activer dès que la règle de calcul et le champ source
    -- existeront (rapportnav2 et/ou côté DWH).
    toFloat64(0) AS n7_5_nb_heures_de_mer_piraterie_brigandage,
    toFloat64(0) AS n7_6_nb_heures_de_vol_piraterie_brigandage

FROM monitorenv_proxy.missions m
LEFT JOIN nav_agg n                             ON n.mission_id = m.id
LEFT JOIN heures_de_mer_nav hm                  ON hm.mission_id = m.id
LEFT JOIN control_targets_nav ct                ON ct.mission_id = m.id
LEFT JOIN mission_units mu                      ON mu.mission_id = m.id
LEFT JOIN mission_service ms                    ON ms.mission_id = m.id
LEFT JOIN nav_completeness nc                   ON nc.mission_id = m.id
LEFT JOIN rapportnav_proxy.mission_general_info gi ON gi.mission_id = m.id
LEFT JOIN env_agg e                             ON e.mission_id = m.id
LEFT JOIN fish_agg f                             ON f.mission_id = m.id
LEFT JOIN fish_control_targets fct               ON fct.mission_id = m.id

WHERE toDateTime(m.start_datetime_utc) >= toDateTime('2025-01-01 00:00:00')
  AND toUInt8(m.deleted) = 0
  -- AND m.completeness_for_stats = 'COMPLETE'  -- [métier] champ à venir côté source, activer dès disponibilité (nom de colonne à confirmer)
  AND ms.service_type IN ('PAM', 'ULAM')
  -- AND m.id IN (37542, 36754)  -- TEMPORAIRE, À RETIRER : restriction de validation sur 2 missions connues, le temps de vérifier les correctifs (leadInFrame notamment)
ORDER BY m.start_datetime_utc DESC
;
