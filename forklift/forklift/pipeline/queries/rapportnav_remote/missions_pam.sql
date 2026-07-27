-- =====================================================================
-- Portage SQL de ComputeControlPolicies / ComputeNavControlPolicy /
-- ComputeProFishingControlPolicy / ComputeEnvControlPolicy
-- (rapportnav2, backend/.../analytics/patrol/controlPolicies/).
-- Nécessaire car l'API analytics/v1/patrol est sunsettée : cette
-- logique n'est plus disponible ailleurs qu'ici.
--
-- ⚠️ VALIDÉ CONTRE LE CODE, mais deux points à confirmer avant mise en
-- prod (cf. message) :
--   1) "other" utilise EXACTEMENT le même calcul que "proFishing" côté
--      Kotlin (CountInfractions.countFishInfractions ==
--      countOtherFishInfractions, code strictement identique). Reproduit
--      à l'identique ci-dessous (colonnes dupliquées), PAS corrigé —
--      à trancher avec l'équipe avant de publier ce chiffre en dashboard.
--   2) hasBeenDone == true est une condition réelle du code Kotlin
--      (countNavInfractions, countEnvInfractions) : un control_2 avec
--      has_been_done = false ne doit JAMAIS compter, même s'il a des
--      infractions rattachées.
--
-- CORRECTIF (testé) : target_2.action_id, PAS action_Id. L'annotation
-- @Column(name = "action_Id") dans TargetModel.kt est trompeuse -- Postgres
-- replie tout identifiant non échappé en minuscules à la création de la
-- table, donc la colonne réelle est action_id. ClickHouse, sensible à la
-- casse, échouait avec "Identifier 't.action_Id' cannot be resolved".
--
-- CORRECTIF 2 (testé) : mission_action.id est arrivé côté ClickHouse en
-- UUID natif, mais les colonnes de clé étrangère (target_2.action_id,
-- control_2.target_id, infraction_2.control_id) en String -- la
-- réplication ne type pas les FK comme les PK. D'où
-- NO_COMMON_TYPE (UUID vs String) sur les jointures. Cast toString() des
-- deux côtés de chaque jointure concernée, plus robuste que toUUID() qui
-- plante sur toute valeur malformée.
--
-- CORRECTIF 3 (testé) : écrire "= true" côté ClickHouse ne suffit PAS --
-- le moteur PostgreSQL de ClickHouse retraduit les littéraux booléens en
-- 0/1 avant de forwarder le WHERE à Postgres via pqxx, quelle que soit
-- la syntaxe utilisée côté ClickHouse. Seul un cast toUInt8(colonne)
-- empêche ce pushdown littéral (ClickHouse n'optimise pas les prédicats
-- enveloppés dans une fonction en pushdown vers la source), forçant
-- l'évaluation côté ClickHouse sur la valeur brute rapatriée.
-- =====================================================================

WITH
-- ---------------------------------------------------------------------
-- FILTRES : unité, bordée (rapportnav_proxy.service, même regex que
-- analytics_missions_full_data.sql) et complétude (nav_completeness,
-- même définition que query_aem_par_mission_3_bases_clickhouse.sql).
-- ---------------------------------------------------------------------
service_detailed AS (
    SELECT
        id,
        name,
        CASE WHEN LOWER(name) LIKE '%pam%' THEN 'PAM' ELSE 'ULAM' END AS service_type,
        CASE
            WHEN name LIKE '% A%' THEN 'A'
            WHEN name LIKE '% B%' THEN 'B'
            ELSE NULL
        END AS bordee,
        IF(
            extract(name, 'PAM\\s+(.+?)\\s+[AB]') != '',
            extract(name, 'PAM\\s+(.+?)\\s+[AB]'),
            extract(name, '(?i)(?:ULAM|ulam)[_ ](\\d+)')
        ) AS unite
    FROM rapportnav_proxy.service
),
nav_completeness AS (
    SELECT
        mission_id,
        countIf(coalesce(toUInt8(is_complete_for_stats), 0) = 0) = 0 AS toutes_actions_nav_completes
    FROM rapportnav_proxy.mission_action
    GROUP BY mission_id
),

-- ---------------------------------------------------------------------
-- HEURES / JOURS DE MER (repris de status_actions + heures_de_mer_nav
-- dans query_aem_par_mission_3_bases_clickhouse.sql -- même correctif
-- leadInFrame sur end_datetime_utc absent).
-- ---------------------------------------------------------------------
status_actions AS (
    SELECT
        ma.mission_id,
        ma.status,
        ma.start_datetime_utc,
        leadInFrame(
            ma.start_datetime_utc, 1, ifNull(envm.end_datetime_utc, ma.start_datetime_utc)
        ) OVER (PARTITION BY ma.mission_id ORDER BY ma.start_datetime_utc) AS corrected_end_datetime_utc
    FROM rapportnav_proxy.mission_action ma
    INNER JOIN monitorenv_proxy.missions envm ON envm.id = ma.mission_id
    WHERE ma.action_type = 'STATUS'
),
heures_de_mer_agg AS (
    SELECT
        mission_id,
        SUM(dateDiff('second', start_datetime_utc, corrected_end_datetime_utc) / 3600.0) AS heures_de_mer
    FROM status_actions
    WHERE status IN ('ANCHORED', 'NAVIGATING')
    GROUP BY mission_id
),
-- Jours de mer : explosion par jour calendaire, seuil 4h/jour (cf.
-- GetNbOfDaysAtSeaFromNavigationStatus.kt / MapStatusDurations.kt côté
-- rapportnav2 -- convention déjà actée dans nos échanges précédents).
jours_de_mer_agg AS (
    SELECT
        mission_id,
        countDistinctIf(day, hours_in_day >= 4) AS jours_de_mer
    FROM (
        SELECT
            mission_id,
            day,
            dateDiff(
                'second',
                greatest(start_datetime_utc, toDateTime(day)),
                least(corrected_end_datetime_utc, toDateTime(day) + INTERVAL 1 DAY)
            ) / 3600.0 AS hours_in_day
        FROM (
            -- ⚠️ garde-fou indispensable : sans ce filtre, une ligne où
            -- corrected_end_datetime_utc < start_datetime_utc (données
            -- désordonnées / leadInFrame ayant repris un timestamp
            -- antérieur) donne un dateDiff négatif, cast en UInt32 il
            -- "wrap" vers ~4,3 milliards -> range() explose (déjà vu et
            -- corrigé sur build_action_status_durations.sql, reproduit
            -- ici par erreur sans le filtre).
            SELECT * FROM status_actions
            WHERE corrected_end_datetime_utc >= start_datetime_utc
              AND status IN ('ANCHORED', 'NAVIGATING')
        )
        ARRAY JOIN
            arrayMap(
                d -> toDate(start_datetime_utc) + d,
                range(toUInt32(dateDiff('day', toDate(start_datetime_utc), toDate(corrected_end_datetime_utc)) + 1))
            ) AS day
    )
    GROUP BY mission_id
),

-- ---------------------------------------------------------------------
-- ACTIONS DE CONTRÔLE / HEURES DE SURVEILLANCE / CIBLES CONTRÔLÉES,
-- TOUTES POLICES CONFONDUES (nav + fish + env)
-- ⚠️ Surveillance côté fish : aucun action_type "surveillance" identifié
-- dans monitorfish_proxy.mission_actions (SEA_CONTROL/LAND_CONTROL sont
-- des contrôles, pas de la surveillance) -- mis à 0, à confirmer avec
-- l'équipe MonitorFish si une notion équivalente existe.
-- ---------------------------------------------------------------------
nav_control_surveillance_agg AS (
    SELECT
        mission_id,
        countIf(action_type = 'CONTROL')                                              AS nb_actions_controle,
        sumIf(dateDiff('second', start_datetime_utc, end_datetime_utc) / 3600.0,
              action_type = 'SURVEILLANCE')                                            AS heures_surveillance
    FROM rapportnav_proxy.mission_action
    GROUP BY mission_id
),
nav_targets_controle_agg AS (
    -- repris de control_targets_nav (query_aem_par_mission_3_bases_clickhouse.sql)
    SELECT
        ma.mission_id,
        countIf(toString(t.id) != '00000000-0000-0000-0000-000000000000') AS nb_cibles_controlees
    FROM rapportnav_proxy.mission_action ma
    LEFT JOIN rapportnav_proxy.target_2 t ON toString(t.action_id) = toString(ma.id)
    WHERE ma.action_type = 'CONTROL'
    GROUP BY ma.mission_id
),
fish_control_agg AS (
    SELECT
        mission_id,
        countIf(action_type IN ('SEA_CONTROL', 'LAND_CONTROL')) AS nb_actions_controle,
        countIf(action_type = 'SEA_CONTROL')                    AS nb_cibles_controlees  -- repris de fish_control_targets
    FROM monitorfish_proxy.mission_actions
    GROUP BY mission_id
),
env_control_surveillance_agg AS (
    SELECT
        ea.mission_id,
        countIf(ea.action_type = 'CONTROL')                                            AS nb_actions_controle,
        sumIf(dateDiff('second', ea.action_start_datetime_utc, ea.action_end_datetime_utc) / 3600.0,
              ea.action_type = 'SURVEILLANCE')                                          AS heures_surveillance,
        -- repris de nb_targets_control_env (query_aem_par_mission_3_bases_clickhouse.sql)
        sumIf(
            if(JSONHas(ea.value, 'actionNumberOfControls'), JSONExtractInt(ea.value, 'actionNumberOfControls'), 0),
            JSONExtractString(ea.value, 'vehicleType') = 'VESSEL' AND ea.action_type = 'CONTROL'
        )
        + countIf(JSONExtractString(ea.value, 'vehicleType') = 'VESSEL' AND ea.action_type = 'SURVEILLANCE')
                                                                                          AS nb_cibles_controlees
    FROM monitorenv_proxy.env_actions ea
    GROUP BY ea.mission_id
),

-- ---------------------------------------------------------------------
-- NAV : security / navigation / gens_de_mer / administrative
-- Chaîne réelle : mission_action(action_type='CONTROL')
--   -> target_2 (action_Id) -> control_2 (target_id, control_type,
--   has_been_done) -> infraction_2 (control_id, infraction_type)
-- ⚠️ ComputeNavControlPolicy ne filtre QUE action_type = 'CONTROL' (pas
-- CONTROL_SECTOR / CONTROL_NAUTICAL_LEISURE / OTHER_CONTROL, qui existent
-- pourtant dans l'enum ActionType) -- reproduit à l'identique.
-- ---------------------------------------------------------------------
nav_controls AS (
    SELECT
        ma.mission_id,
        ma.control_method                          AS control_method,   -- 'SEA' | 'LAND'
        c.control_type                              AS control_type,
        c.amount_of_controls                        AS amount_of_controls,
        c.id                                         AS control_id
    FROM rapportnav_proxy.mission_action ma
    INNER JOIN rapportnav_proxy.target_2 t  ON toString(t.action_id) = toString(ma.id)
    INNER JOIN rapportnav_proxy.control_2 c ON toString(c.target_id) = toString(t.id)
    WHERE ma.action_type = 'CONTROL'
      AND c.control_type IN ('SECURITY', 'NAVIGATION', 'GENS_DE_MER', 'ADMINISTRATIVE')
),
nav_infractions AS (
    SELECT
        ma.mission_id,
        c.control_type                              AS control_type,
        countIf(i.infraction_type = 'WITH_REPORT')    AS nb_infractions_avec_pv,
        countIf(i.infraction_type = 'WITHOUT_REPORT') AS nb_infractions_sans_pv
    FROM rapportnav_proxy.mission_action ma
    INNER JOIN rapportnav_proxy.target_2 t     ON toString(t.action_id) = toString(ma.id)
    INNER JOIN rapportnav_proxy.control_2 c    ON toString(c.target_id) = toString(t.id)
    INNER JOIN rapportnav_proxy.infraction_2 i ON toString(i.control_id) = toString(c.id)
    WHERE ma.action_type = 'CONTROL'
      AND toUInt8(c.has_been_done) = 1
      AND c.control_type IN ('SECURITY', 'NAVIGATION', 'GENS_DE_MER', 'ADMINISTRATIVE')
    GROUP BY ma.mission_id, c.control_type
),
nav_policy_agg AS (
    SELECT
        nc.mission_id,
        nc.control_type,
        count()                                          AS nb_controls,
        countIf(nc.control_method = 'SEA')                AS nb_controls_sea,
        countIf(nc.control_method = 'LAND')                AS nb_controls_land,
        coalesce(max(ni.nb_infractions_avec_pv), 0)         AS nb_infractions_avec_pv,
        coalesce(max(ni.nb_infractions_sans_pv), 0)         AS nb_infractions_sans_pv
    FROM nav_controls nc
    LEFT JOIN nav_infractions ni
        ON ni.mission_id = nc.mission_id AND ni.control_type = nc.control_type
    GROUP BY nc.mission_id, nc.control_type
),

-- ---------------------------------------------------------------------
-- PRO FISHING / OTHER (monitorfish_proxy.mission_actions)
-- ⚠️ "other" = copie conforme de "proFishing" côté Kotlin (cf. note en
-- tête de fichier). Reproduit ici à l'identique.
-- ---------------------------------------------------------------------
fish_policy_agg AS (
    SELECT
        fa.mission_id,
        countIf(fa.action_type IN ('SEA_CONTROL', 'LAND_CONTROL'))        AS nb_controls,
        countIf(fa.action_type = 'SEA_CONTROL')                          AS nb_controls_sea,
        countIf(fa.action_type = 'LAND_CONTROL')                         AS nb_controls_land,
        sumIf(
            length(arrayFilter(x -> JSONExtractString(x, 'infractionType') = 'WITH_RECORD',
                                JSONExtractArrayRaw(ifNull(fa.infractions, '')))),
            fa.action_type IN ('SEA_CONTROL', 'LAND_CONTROL')
        )                                                                 AS nb_infractions_avec_pv,
        sumIf(
            length(arrayFilter(x -> JSONExtractString(x, 'infractionType') = 'WITHOUT_RECORD',
                                JSONExtractArrayRaw(ifNull(fa.infractions, '')))),
            fa.action_type IN ('SEA_CONTROL', 'LAND_CONTROL')
        )                                                                 AS nb_infractions_sans_pv
    FROM monitorfish_proxy.mission_actions fa
    GROUP BY fa.mission_id
),

-- ---------------------------------------------------------------------
-- ENV POLLUTION (monitorenv_proxy.env_actions)
-- ⚠️ countEnvInfractions filtre en plus target.source == MONITORENV et
-- control.hasBeenDone == true -- non reproductible finement ici sans
-- accès direct aux targets/controls JSON imbriqués dans env_actions.value
-- (cf. note ⚠️ du fichier query_aem_par_mission_3_bases_clickhouse.sql :
-- tout le détail métier env est dans la colonne JSON `value`). Approximé
-- ci-dessous sans le filtre source=MONITORENV -- à corriger si le champ
-- est accessible en JSON (ex. value.targets[].source).
-- ---------------------------------------------------------------------
env_policy_agg AS (
    SELECT
        ea.mission_id,
        countIf(ea.action_type = 'CONTROL')                                AS nb_controls,
        sumIf(
            length(arrayFilter(x -> JSONExtractString(x, 'infractionType') = 'WITH_REPORT',
                                JSONExtractArrayRaw(ifNull(ea.value, ''), 'infractions'))),
            ea.action_type = 'CONTROL'
        )                                                                  AS nb_infractions_avec_pv,
        sumIf(
            length(arrayFilter(x -> JSONExtractString(x, 'infractionType') = 'WITHOUT_REPORT',
                                JSONExtractArrayRaw(ifNull(ea.value, ''), 'infractions'))),
            ea.action_type = 'CONTROL'
        )                                                                  AS nb_infractions_sans_pv
    FROM monitorenv_proxy.env_actions ea
    GROUP BY ea.mission_id
)

-- =====================================================================
-- ASSEMBLAGE — une ligne par mission, colonnes pivotées par politique
-- publique (même forme que controlPolicies_* dans l'ancien rapportnav.patrol)
-- =====================================================================
SELECT
    toInt32(m.id)                                          AS mission_id,
    toDateTime(m.start_datetime_utc)                       AS mission_date_debut,     -- FILTRE période
    toString(sd.unite)                                     AS unite,                  -- FILTRE unité
    toString(sd.bordee)                                    AS bordee,                 -- FILTRE bordée
    toUInt8(coalesce(nc.toutes_actions_nav_completes, 0))  AS is_complete_for_stats,   -- FILTRE isCompleteForStats

    toFloat64(dateDiff('second', m.start_datetime_utc, m.end_datetime_utc) / 3600.0)
                                                             AS nb_heures_mission,
    toFloat64(coalesce(hm.heures_de_mer, 0))                AS nb_heures_de_mer,
    toInt64(coalesce(jm.jours_de_mer, 0))                   AS nb_jours_de_mer,

    toInt64(
        coalesce(ncs.nb_actions_controle, 0) + coalesce(fc.nb_actions_controle, 0) + coalesce(ecs.nb_actions_controle, 0)
    )                                                        AS nb_actions_controle_toutes_polices,
    toFloat64(
        coalesce(ncs.heures_surveillance, 0) + coalesce(ecs.heures_surveillance, 0)
    )                                                        AS nb_heures_surveillance_toutes_polices,
    toInt64(
        coalesce(nt.nb_cibles_controlees, 0) + coalesce(fc.nb_cibles_controlees, 0) + coalesce(ecs.nb_cibles_controlees, 0)
    )                                                        AS nb_cibles_controlees_toutes_polices,

    -- SECURITY
    toInt64(coalesce(sumIf(np.nb_controls, np.control_type = 'SECURITY'), 0))            AS security_nb_controls,
    toInt64(coalesce(sumIf(np.nb_controls_sea, np.control_type = 'SECURITY'), 0))         AS security_nb_controls_sea,
    toInt64(coalesce(sumIf(np.nb_controls_land, np.control_type = 'SECURITY'), 0))        AS security_nb_controls_land,
    toInt64(coalesce(sumIf(np.nb_infractions_avec_pv, np.control_type = 'SECURITY'), 0))  AS security_nb_infractions_avec_pv,
    toInt64(coalesce(sumIf(np.nb_infractions_sans_pv, np.control_type = 'SECURITY'), 0))  AS security_nb_infractions_sans_pv,

    -- NAVIGATION
    toInt64(coalesce(sumIf(np.nb_controls, np.control_type = 'NAVIGATION'), 0))           AS navigation_nb_controls,
    toInt64(coalesce(sumIf(np.nb_controls_sea, np.control_type = 'NAVIGATION'), 0))        AS navigation_nb_controls_sea,
    toInt64(coalesce(sumIf(np.nb_controls_land, np.control_type = 'NAVIGATION'), 0))       AS navigation_nb_controls_land,
    toInt64(coalesce(sumIf(np.nb_infractions_avec_pv, np.control_type = 'NAVIGATION'), 0)) AS navigation_nb_infractions_avec_pv,
    toInt64(coalesce(sumIf(np.nb_infractions_sans_pv, np.control_type = 'NAVIGATION'), 0)) AS navigation_nb_infractions_sans_pv,

    -- GENS_DE_MER
    toInt64(coalesce(sumIf(np.nb_controls, np.control_type = 'GENS_DE_MER'), 0))            AS gens_de_mer_nb_controls,
    toInt64(coalesce(sumIf(np.nb_controls_sea, np.control_type = 'GENS_DE_MER'), 0))         AS gens_de_mer_nb_controls_sea,
    toInt64(coalesce(sumIf(np.nb_controls_land, np.control_type = 'GENS_DE_MER'), 0))        AS gens_de_mer_nb_controls_land,
    toInt64(coalesce(sumIf(np.nb_infractions_avec_pv, np.control_type = 'GENS_DE_MER'), 0))  AS gens_de_mer_nb_infractions_avec_pv,
    toInt64(coalesce(sumIf(np.nb_infractions_sans_pv, np.control_type = 'GENS_DE_MER'), 0))  AS gens_de_mer_nb_infractions_sans_pv,

    -- ADMINISTRATIVE
    toInt64(coalesce(sumIf(np.nb_controls, np.control_type = 'ADMINISTRATIVE'), 0))            AS administrative_nb_controls,
    toInt64(coalesce(sumIf(np.nb_controls_sea, np.control_type = 'ADMINISTRATIVE'), 0))         AS administrative_nb_controls_sea,
    toInt64(coalesce(sumIf(np.nb_controls_land, np.control_type = 'ADMINISTRATIVE'), 0))        AS administrative_nb_controls_land,
    toInt64(coalesce(sumIf(np.nb_infractions_avec_pv, np.control_type = 'ADMINISTRATIVE'), 0))  AS administrative_nb_infractions_avec_pv,
    toInt64(coalesce(sumIf(np.nb_infractions_sans_pv, np.control_type = 'ADMINISTRATIVE'), 0))  AS administrative_nb_infractions_sans_pv,

    -- PRO_FISHING
    toInt64(coalesce(fp.nb_controls, 0))            AS pro_fishing_nb_controls,
    toInt64(coalesce(fp.nb_controls_sea, 0))         AS pro_fishing_nb_controls_sea,
    toInt64(coalesce(fp.nb_controls_land, 0))        AS pro_fishing_nb_controls_land,
    toInt64(coalesce(fp.nb_infractions_avec_pv, 0))  AS pro_fishing_nb_infractions_avec_pv,
    toInt64(coalesce(fp.nb_infractions_sans_pv, 0))  AS pro_fishing_nb_infractions_sans_pv,

    -- ENV_POLLUTION (pas de split sea/land, cf. ComputeEnvControlPolicy : null)
    toInt64(coalesce(ep.nb_controls, 0))            AS env_pollution_nb_controls,
    toInt64(coalesce(ep.nb_infractions_avec_pv, 0))  AS env_pollution_nb_infractions_avec_pv,
    toInt64(coalesce(ep.nb_infractions_sans_pv, 0))  AS env_pollution_nb_infractions_sans_pv

FROM monitorenv_proxy.missions m
LEFT JOIN rapportnav_proxy.mission_general_info gi ON gi.mission_id = m.id
-- ⚠️ même réserve que dans query_patrol_complement_mission.sql : le nom
-- de colonne gi.service_id est repris par analogie avec
-- analytics_missions_full_data.sql, pas vérifié directement dans
-- MissionGeneralInfoModel.kt -- à confirmer.
LEFT JOIN service_detailed sd                       ON toString(sd.id) = toString(gi.service_id)
LEFT JOIN nav_completeness nc                       ON nc.mission_id = m.id
LEFT JOIN heures_de_mer_agg hm                       ON hm.mission_id = m.id
LEFT JOIN jours_de_mer_agg jm                        ON jm.mission_id = m.id
LEFT JOIN nav_control_surveillance_agg ncs           ON ncs.mission_id = m.id
LEFT JOIN nav_targets_controle_agg nt                ON nt.mission_id = m.id
LEFT JOIN fish_control_agg fc                        ON fc.mission_id = m.id
LEFT JOIN env_control_surveillance_agg ecs           ON ecs.mission_id = m.id
LEFT JOIN nav_policy_agg np ON np.mission_id = m.id
LEFT JOIN fish_policy_agg fp ON fp.mission_id = m.id
LEFT JOIN env_policy_agg ep  ON ep.mission_id = m.id
WHERE m.start_datetime_utc >= toDateTime('2026-01-01 00:00:00')  -- borne provisoire, à remplacer par un filtre Metabase dynamique
  AND sd.service_type = 'PAM'  -- FILTRE : unités PAM uniquement
GROUP BY m.id, m.start_datetime_utc, m.end_datetime_utc, sd.unite, sd.bordee, nc.toutes_actions_nav_completes,
         hm.heures_de_mer, jm.jours_de_mer,
         ncs.nb_actions_controle, ncs.heures_surveillance,
         nt.nb_cibles_controlees,
         fc.nb_actions_controle, fc.nb_cibles_controlees,
         ecs.nb_actions_controle, ecs.heures_surveillance, ecs.nb_cibles_controlees,
         fp.nb_controls, fp.nb_controls_sea, fp.nb_controls_land,
         fp.nb_infractions_avec_pv, fp.nb_infractions_sans_pv,
         ep.nb_controls, ep.nb_infractions_avec_pv, ep.nb_infractions_sans_pv
ORDER BY mission_date_debut DESC
;
