-- =====================================================================
-- Alimente rapportnav.fact_controle_pam_ulam (query_filepath pour la
-- ligne "fact_controle_pam_ulam" de sync_table_from_db_connection.csv).
-- Grain : 1 ligne par contrôle individuel, TOUTES SOURCES CONFONDUES
-- (nav + fish + env, cf. discussion en chat) -- colonne `source`
-- ('NAV'/'FISH'/'ENV'). Table détail (pas pré-agrégée) : sert les cartes
-- et listes de drill-down des maquettes (géoloc, nom d'établissement...),
-- contrairement à fact_moyen_pam_ulam / fact_cible_pam_ulam qui sont déjà
-- agrégées par mois.
--
-- ⚠️⚠️ POURQUOI 3 SOURCES : rapportnav_proxy.mission_action (nav) ne
-- contient QUE les actions saisies directement dans RapportNav
-- (source=RAPPORT_NAV, vérifié : ni MissionFishActionEntity ni
-- MissionEnvActionEntity n'écrivent dans mission_action, rapportnav2).
-- Les contrôles de pêche/environnement synchronisés depuis MonitorFish/
-- MonitorEnv n'y apparaissent jamais. D'où l'union avec 2 tables déjà
-- construites et actives dans ce repo (PAS créées pour cette PR) :
--   - monitorfish.analytics_controls_full_data (rafraîchie quotidiennement,
--     cf. sync_table_with_pandas.csv "32 1 * * *")
--   - monitorenv.analytics_actions + monitorenv.actions_infractions
--     (rafraîchies HORAIRE, cf. sync_table_with_pandas.csv "20/22 * * * *")
-- Le filtre PAM/ULAM par nom repris ci-dessous pour ENV est recopié tel
-- quel de monitorenv_remote/analytics_actions.sql (déjà en prod pour
-- d'autres usages) -- PAS inventé pour cette PR. Appliqué par analogie au
-- FISH (repos MonitorFish/MonitorEnv clonés et inspectés, cf. ci-dessous,
-- mais cette logique par nom n'existe pas ailleurs côté FISH -- c'est
-- une extrapolation, pas une confirmation).
--
-- ⚠️ 3 modèles de données réellement différents, normalisés du mieux
-- possible dans un schéma commun. Points VÉRIFIÉS contre les repos
-- MonitorFish et MonitorEnv clonés (backend/src/main/kotlin + migrations
-- SQL) :
--   - mission_id partagé entre les 3 systèmes : CONFIRMÉ, pas une
--     hypothèse. MonitorFish n'a AUCUN concept de mission propre -- il
--     interroge l'API MonitorEnv en direct avec ce même missionId:Int
--     (APIMissionRepository.kt, monitorfish) et RapportNav fait de même
--     (déjà établi dans missions_aem.sql/rapport_pam_ulam_mission.sql).
--   - control_unit_id : PAS supposé partagé entre systèmes malgré tout
--     (repli volontaire sur le filtre par NOM d'unité pour fish/env,
--     comme pour nav -- évite cette hypothèse plus fragile, non vérifiée).
--   - FISH nb_controles = 1 par ligne : CONFIRMÉ -- MissionAction.kt
--     (monitorfish) n'a aucun champ de type "amount_of_controls"/compteur
--     sur une action, contrairement à rapportnav_proxy.control_2.
--   - FISH infraction avec/sans PV : monitorfish a bien un InfractionType
--     à 3 valeurs (WITH_RECORD/WITHOUT_RECORD/PENDING, InfractionType.kt)
--     -- même piège que nav (WAITING). MAIS la table déjà construite
--     monitorfish.analytics_controls_full_data (analytics_controls_full_data.sql,
--     CTE controls_infraction_natinfs_array) ne calcule QUE
--     infraction_report = présence d'au moins une infraction WITH_RECORD
--     -- elle n'expose PAS WITHOUT_RECORD/PENDING séparément dans ses
--     colonnes de sortie. C'est une vraie limite de la table source déjà
--     construite (pas quelque chose que je peux corriger sans modifier
--     analytics_controls_full_data.sql, hors périmètre de cette PR) :
--     nb_infractions_sans_pv pour FISH mélange donc WITHOUT_RECORD et
--     PENDING -- fiable pour nb_infractions_avec_pv, PAS pour
--     nb_infractions_sans_pv.
--   - ENV infraction avec/sans PV : CONFIRMÉ fiable. InfractionTypeEnum.kt
--     (monitorenv, package envActionControl.infraction) a EXACTEMENT les
--     3 mêmes valeurs (WAITING/WITH_REPORT/WITHOUT_REPORT) que la copie
--     miroir dans rapportnav2 -- pas une coïncidence de nom de colonne,
--     un vrai enum partagé en pratique.
--   - sous_type_controle : pas d'équivalent identifié côté FISH (colonne
--     vide) ; côté ENV, theme_level_1 utilisé par approximation (pas
--     confirmé comme l'équivalent fonctionnel de control_type nav --
--     aucune méthode de calcul métier trouvée qui le confirme).
--
-- Filtre "missions à partir de 2025" appliqué sur les 3 sources (cf.
-- discussion en chat) : sans lui, la source ENV en particulier
-- récupérerait tout l'historique de monitorenv.analytics_actions, non
-- utile pour ce rapport et coûteux à chaque refresh horaire.
--
-- ⚠️ Ce fichier DOIT tourner après dim_unit_reference.sql ET après les
-- flows sync_table_with_pandas qui alimentent monitorfish.analytics_controls_full_data
-- / monitorenv.analytics_actions / monitorenv.actions_infractions
-- (aucune dépendance native entre lignes des 2 flows -- cf. commentaire
-- détaillé dans dim_unit_reference.sql, même risque, périmètre élargi à
-- un flow différent).
-- =====================================================================
WITH
-- Filtre unités PAM + ULAM pour la source NAV (même logique que les
-- autres requêtes pam_ulam_*.sql).
pam_ulam_control_units AS (
    SELECT DISTINCT cu.id AS control_unit_id
    FROM monitorenv_proxy.control_units cu
    LEFT JOIN rapportnav_proxy.service_control_unit scu ON scu.control_unit_id = cu.id
    LEFT JOIN rapportnav_proxy.service s ON s.id = scu.service_id AND s.deleted_at IS NULL
    WHERE s.service_type IN ('PAM', 'ULAM')
       OR startsWith(upper(cu.name), 'ULAM')
       OR startsWith(upper(cu.name), 'PAM')
),
mission_unit_pairs AS (
    SELECT DISTINCT
        mcu.mission_id,
        cu.id AS control_unit_id,
        cu.name AS unit_name,
        toString(coalesce(uref.facade_ref, '')) AS facade,
        toString(coalesce(nullIf(uref.unit_type, ''), multiIf(
            startsWith(upper(cu.name), 'PAM'), 'PAM',
            startsWith(upper(cu.name), 'ULAM'), 'ULAM',
            'AUTRE'
        ))) AS unit_type
    FROM monitorenv_proxy.missions_control_units mcu
    INNER JOIN monitorenv_proxy.control_units cu ON cu.id = mcu.control_unit_id
    INNER JOIN pam_ulam_control_units uu ON uu.control_unit_id = cu.id
    LEFT JOIN rapportnav.dim_unit_reference uref ON uref.control_unit_id = cu.id
),
-- Contrôles/infractions has_been_done=true par action -- même logique
-- vérifiée que dans rapport_pam_ulam_action.sql (control_infraction_flags/
-- action_controls), dupliquée ici.
control_infraction_flags AS (
    SELECT
        c.id AS control_id,
        toString(t.action_id) AS action_id,
        coalesce(c.amount_of_controls, 0) AS amount_of_controls,
        maxIf(1, coalesce(i.infraction_type, '') = 'WITH_REPORT') AS has_with_report,
        maxIf(1, coalesce(i.infraction_type, '') = 'WITHOUT_REPORT') AS has_without_report
    FROM rapportnav_proxy.control_2 c
    INNER JOIN rapportnav_proxy.target_2 t ON t.id = c.target_id
    LEFT JOIN rapportnav_proxy.infraction_2 i ON i.control_id = c.id
    WHERE coalesce(c.has_been_done, false) = true
    GROUP BY c.id, t.action_id, c.amount_of_controls
),
action_controls AS (
    SELECT
        action_id,
        sum(amount_of_controls) AS nb_controles,
        sumIf(amount_of_controls, has_with_report = 1) AS nb_infractions_avec_pv,
        sumIf(amount_of_controls, has_without_report = 1) AS nb_infractions_sans_pv
    FROM control_infraction_flags
    GROUP BY action_id
),
action_natinfs AS (
    SELECT
        toString(t.action_id) AS action_id,
        groupUniqArray(n.natinf_code) AS natinf_codes
    FROM rapportnav_proxy.target_2 t
    INNER JOIN rapportnav_proxy.control_2 c ON c.target_id = t.id AND coalesce(c.has_been_done, false) = true
    LEFT JOIN rapportnav_proxy.infraction_2 i ON i.control_id = c.id
    LEFT JOIN rapportnav_proxy.infraction_natinf_2 n ON n.infraction_id = i.id
    GROUP BY t.action_id
),

-- ---- Source NAV ----
nav_controls AS (
    SELECT
        'NAV' AS source,
        toString(ma.id) AS action_id,
        ma.mission_id AS mission_id,
        mup.control_unit_id AS control_unit_id,
        mup.unit_name AS unit_name,
        mup.facade AS facade,
        mup.unit_type AS unit_type,
        toDateTime64(ma.start_datetime_utc, 6) AS action_datetime_utc,
        toString(ma.action_type) AS action_type,
        toString(coalesce(ma.control_type, '')) AS sous_type_controle,
        toString(coalesce(nullIf(ma.vessel_type, ''), nullIf(ma.leisure_type, ''), nullIf(ma.fishing_gear_type, ''), nullIf(ma.sector_establishment_type, ''), '')) AS cible_label,
        toUInt16(coalesce(ac.nb_controles, 0)) AS nb_controles,
        toUInt16(coalesce(ac.nb_infractions_avec_pv, 0)) AS nb_infractions_avec_pv,
        toUInt16(coalesce(ac.nb_infractions_sans_pv, 0)) AS nb_infractions_sans_pv,
        toUInt8(1) AS nb_infractions_sans_pv_fiable,
        arrayMap(x -> toString(x), coalesce(an.natinf_codes, [])) AS natinf_codes,
        ma.latitude AS latitude,
        ma.longitude AS longitude
    FROM rapportnav_proxy.mission_action ma
    INNER JOIN mission_unit_pairs mup ON mup.mission_id = ma.mission_id
    LEFT JOIN action_controls ac ON ac.action_id = toString(ma.id)
    LEFT JOIN action_natinfs an ON an.action_id = toString(ma.id)
    WHERE ma.control_type IS NOT NULL AND toString(ma.control_type) != ''
      AND ma.start_datetime_utc >= toDateTime('2025-01-01 00:00:00')
),

-- ---- Source FISH ----
-- monitorfish.analytics_controls_full_data : déjà construite, 1 ligne =
-- 1 contrôle (CONFIRMÉ, cf. avertissement en tête de fichier). Filtre
-- PAM/ULAM par nom d'unité (control_unit), pas par id (espace d'id non
-- supposé partagé avec monitorenv/rapportnav).
-- ⚠️ nb_infractions_sans_pv non fiable ici (mélange WITHOUT_RECORD et
-- PENDING) -- cf. avertissement détaillé en tête de fichier. Gardé sous
-- le même nom que nav/env pour permettre un UNION positionnel simple
-- (colonnes identiques dans les 3 sources), mais accompagné de
-- nb_infractions_sans_pv_fiable (0 ici, 1 pour nav/env) pour permettre
-- de l'exclure explicitement côté Metabase si besoin.
fish_controls AS (
    SELECT
        'FISH' AS source,
        toString(f.id) AS action_id,
        f.mission_id AS mission_id,
        f.control_unit_id AS control_unit_id,
        f.control_unit AS unit_name,
        f.facade AS facade,
        toString(multiIf(
            startsWith(upper(f.control_unit), 'PAM'), 'PAM',
            startsWith(upper(f.control_unit), 'ULAM'), 'ULAM',
            'AUTRE'
        )) AS unit_type,
        toDateTime64(f.control_datetime_utc, 6) AS action_datetime_utc,
        toString(f.control_type) AS action_type,
        '' AS sous_type_controle,
        toString(coalesce(f.vessel_name, '')) AS cible_label,
        toUInt16(1) AS nb_controles,
        toUInt16(f.infraction_report) AS nb_infractions_avec_pv,
        toUInt16(if(f.infraction = 1 AND f.infraction_report = 0, 1, 0)) AS nb_infractions_sans_pv,
        toUInt8(0) AS nb_infractions_sans_pv_fiable,
        f.infraction_natinfs AS natinf_codes,
        f.latitude AS latitude,
        f.longitude AS longitude
    FROM monitorfish.analytics_controls_full_data f
    WHERE (startsWith(upper(f.control_unit), 'ULAM') OR startsWith(upper(f.control_unit), 'PAM'))
      AND f.control_datetime_utc >= toDateTime('2025-01-01 00:00:00')
),

-- ---- Source ENV ----
-- monitorenv.analytics_actions + monitorenv.actions_infractions : déjà
-- construites, filtre PAM/ULAM repris tel quel de
-- monitorenv_remote/analytics_actions.sql (is_aff_mar, déjà en prod).
env_infractions_by_action AS (
    SELECT
        env_action_id,
        countIf(coalesce(infraction_type, '') = 'WITH_REPORT') AS nb_infractions_avec_pv,
        countIf(coalesce(infraction_type, '') = 'WITHOUT_REPORT') AS nb_infractions_sans_pv,
        groupUniqArray(arrayJoin(natinf)) AS natinf_codes
    FROM monitorenv.actions_infractions
    GROUP BY env_action_id
),
env_controls AS (
    SELECT
        'ENV' AS source,
        toString(a.id) AS action_id,
        a.mission_id AS mission_id,
        a.control_unit_id AS control_unit_id,
        a.control_unit AS unit_name,
        a.action_facade AS facade,
        toString(multiIf(
            startsWith(upper(a.control_unit), 'PAM'), 'PAM',
            startsWith(upper(a.control_unit), 'ULAM'), 'ULAM',
            'AUTRE'
        )) AS unit_type,
        toDateTime64(a.action_start_datetime_utc, 6) AS action_datetime_utc,
        toString(a.action_type) AS action_type,
        toString(a.theme_level_1) AS sous_type_controle,
        '' AS cible_label,
        toUInt16(coalesce(a.number_of_controls, 0)) AS nb_controles,
        toUInt16(coalesce(ei.nb_infractions_avec_pv, 0)) AS nb_infractions_avec_pv,
        toUInt16(coalesce(ei.nb_infractions_sans_pv, 0)) AS nb_infractions_sans_pv,
        toUInt8(1) AS nb_infractions_sans_pv_fiable,
        arrayMap(x -> toString(x), coalesce(ei.natinf_codes, [])) AS natinf_codes,
        a.latitude AS latitude,
        a.longitude AS longitude
    FROM monitorenv.analytics_actions a
    LEFT JOIN env_infractions_by_action ei ON ei.env_action_id = a.id
    WHERE a.action_type = 'CONTROL'
      AND (
        startsWith(upper(a.control_unit), 'ULAM')
        OR (a.administration = 'DIRM / DM' AND startsWith(upper(a.control_unit), 'PAM'))
      )
      AND a.action_start_datetime_utc >= toDateTime('2025-01-01 00:00:00')
)

SELECT *, now() AS updated_at FROM nav_controls
UNION ALL
SELECT *, now() AS updated_at FROM fish_controls
UNION ALL
SELECT *, now() AS updated_at FROM env_controls;
