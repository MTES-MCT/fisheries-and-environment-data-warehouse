-- =====================================================================
-- Alimente rapportnav.fact_cible_pam_ulam.
-- Grain : 1 ligne par unité × source (NAV/FISH/ENV) × sous-type de
-- contrôle (action_subtype/action_subsubtype, même hiérarchie que
-- fact_action_pam_ulam) × mois. Table pré-agrégée, prête à être posée
-- directement en Metabase.
--
-- ⚠️ RÉÉCRITURE : l'ancienne version ne lisait que rapportnav_proxy
-- (target_2.target_type, 4 valeurs génériques DEFAULT/VEHICLE/COMPANY/
-- INDIVIDUAL -- pas la granularité voulue par les maquettes) et filtrait
-- sur `ma.control_type IS NOT NULL`. Or `control_type` n'est un champ
-- renseigné QUE pour OTHER_CONTROL (texte libre, cf. discussion en chat)
-- -- ce filtre excluait donc silencieusement CONTROL_NAUTICAL_LEISURE/
-- CONTROL_SECTOR/CONTROL_SLEEPING_FISHING_GEAR de cette table. La
-- "granularité de cible" demandée par les maquettes est en réalité déjà
-- portée par action_subtype/action_subsubtype (leisure_type/
-- fishing_gear_type/sector_establishment_type/vessel_type -- même
-- hiérarchie unifiée que fact_action_pam_ulam) : réutilisée ici plutôt
-- que target_2.target_type.
--
-- 3 sources désormais unies (comme fact_action_pam_ulam) : NAV (famille
-- CONTROL unifiée), FISH (monitorfish.analytics_controls_full_data,
-- control_type != 'OBSERVATION'), ENV (monitorenv.analytics_actions,
-- action_type='CONTROL'). `source` exposé en colonne pour permettre un
-- filtre par police tout en gardant les totaux "toutes polices
-- confondues" par simple SUM().
--
-- nb_controles_amp/nb_controles_300m/nb_controles_avec_plongee/
-- nb_controles_journee_securite : mêmes champs "détail" confirmés
-- utilisés par la maquette (Contrôles de navires/loisirs nautiques) que
-- sur fact_action_pam_ulam -- NAV uniquement (nbr_of_control_amp/300m,
-- has_diving_during_operation, is_control_during_security_day n'existent
-- pas côté fish/env, 0 par défaut pour ces sources).
--
-- politique_publique : Pêche professionnelle / Equipement de sécurité /
-- Police de la navigation / Gens de mer / Environnement-pollution /
-- Autres -- confirmé sur les maquettes Metabase ULAM ET PAM (même table
-- "politique publique" sur les deux dashboards). NAV dérivé de
-- control_2.control_type (ADMINISTRATIVE/GENS_DE_MER/NAVIGATION/SECURITY
-- -- champ DIFFÉRENT de mission_action.control_type, texte libre pour
-- OTHER_CONTROL seulement) ; FISH/ENV fixes (pas de classification
-- interne dans ces systèmes).
--
-- ⚠️ Ce fichier DOIT tourner après dim_unit_reference.sql ET après les
-- flows sync_table_with_pandas qui alimentent monitorfish.analytics_controls_full_data
-- / monitorenv.analytics_actions / monitorenv.actions_infractions (aucune
-- dépendance native entre lignes de ces 2 flows, cf. dim_unit_reference.sql).
-- =====================================================================
WITH
-- Référentiel unités PAM/ULAM : source unique rapportnav.dim_unit_reference
-- (liste manuellement maintenue -- une unité pas encore ajoutée n'apparaît
-- pas dans ce rapport).
pam_ulam_control_units AS (
    SELECT
        control_unit_id,
        facade_ref,
        unit_type
    FROM rapportnav.dim_unit_reference
    WHERE unit_type IN ('PAM', 'ULAM')
),
-- 1 ligne par (mission, unité individuelle).
mission_unit_pairs AS (
    SELECT DISTINCT
        mcu.mission_id,
        cu.id AS control_unit_id,
        cu.name AS unit_name,
        uu.facade_ref AS facade,
        uu.unit_type AS unit_type
    FROM monitorenv_proxy.missions_control_units mcu
    INNER JOIN monitorenv_proxy.control_units cu ON cu.id = mcu.control_unit_id
    INNER JOIN pam_ulam_control_units uu ON uu.control_unit_id = cu.id
),
-- Politique publique / thématique pour la famille CONTROL NAV -- extrait
-- réduit de action_type_mapping (rapport_pam_ulam_action.sql) : mêmes 6
-- clés (action_subtype), à resynchroniser si cette table change là-bas.
control_policy_mapping AS (
    SELECT '' AS action_subtype_key, 'Contrôle des activités maritimes' AS politique_publique, 'Transversal' AS thematique
    UNION ALL SELECT 'NAUTICAL_LEISURE', 'Contrôle des activités maritimes', 'Loisirs nautiques'
    UNION ALL SELECT 'SECTOR', 'Contrôle des activités maritimes', 'Transversal'
    UNION ALL SELECT 'SLEEPING_FISHING_GEAR', 'Contrôle des activités maritimes', 'Pêches maritimes'
    UNION ALL SELECT 'OTHER_CONTROL', 'Contrôle des activités maritimes', 'Transversal'
    UNION ALL SELECT 'SHIP', 'Contrôle des activités maritimes', 'Transversal'
),
-- Contrôles/infractions NAV (target_2 -> control_2 -> infraction_2), même
-- logique que rapport_pam_ulam_action.sql : "Nb de ctrl" =
-- SUM(amount_of_controls) des contrôles has_been_done=true.
control_infraction_flags AS (
    SELECT
        c.id AS control_id,
        toString(t.action_id) AS action_id,
        coalesce(c.amount_of_controls, 0) AS amount_of_controls,
        coalesce(c.has_been_done, false) AS has_been_done,
        maxIf(1, coalesce(i.infraction_type, '') = 'WITH_REPORT') AS has_with_report,
        maxIf(1, coalesce(i.infraction_type, '') = 'WITHOUT_REPORT') AS has_without_report,
        maxIf(1, coalesce(i.infraction_type, '') = 'WAITING') AS has_waiting
    FROM rapportnav_proxy.control_2 c
    INNER JOIN rapportnav_proxy.target_2 t ON t.id = c.target_id
    LEFT JOIN rapportnav_proxy.infraction_2 i ON i.control_id = c.id
    GROUP BY c.id, t.action_id, c.amount_of_controls, c.has_been_done
),
action_controls AS (
    SELECT
        action_id,
        sumIf(amount_of_controls, has_been_done = true) AS nb_controls,
        sumIf(amount_of_controls, has_been_done = true AND has_with_report = 1) AS nb_infractions_avec_pv,
        sumIf(amount_of_controls, has_been_done = true AND has_without_report = 1) AS nb_infractions_sans_pv,
        sumIf(amount_of_controls, has_been_done = true AND has_waiting = 1) AS nb_infractions_en_attente
    FROM control_infraction_flags
    GROUP BY action_id
),
action_targets AS (
    SELECT
        toString(t.action_id) AS action_id,
        uniqExact(t.id) AS nb_targets
    FROM rapportnav_proxy.target_2 t
    LEFT JOIN rapportnav_proxy.control_2 c ON c.target_id = t.id AND coalesce(c.has_been_done, false) = true
    GROUP BY t.action_id
),
-- Politique publique des contrôles NAV, cf. rapport_pam_ulam_action.sql
-- (mêmes 4 valeurs control_2.control_type -> Police de la navigation/
-- Gens de mer/Equipement de sécurité/Autres), dupliquée ici.
action_control_policy AS (
    SELECT
        toString(t.action_id) AS action_id,
        arrayElement(topK(1)(toString(c.control_type)), 1) AS control_type_predominant
    FROM rapportnav_proxy.control_2 c
    INNER JOIN rapportnav_proxy.target_2 t ON t.id = c.target_id
    WHERE coalesce(c.has_been_done, false) = true
    GROUP BY t.action_id
),

-- ---- Source NAV : famille CONTROL unifiée uniquement ----
nav_control_rows AS (
    SELECT
        'NAV' AS source,
        mup.control_unit_id AS control_unit_id,
        mup.unit_name AS unit_name,
        mup.facade AS facade,
        mup.unit_type AS unit_type,
        toString(multiIf(
            ma.action_type = 'CONTROL_NAUTICAL_LEISURE', 'NAUTICAL_LEISURE',
            ma.action_type = 'CONTROL_SLEEPING_FISHING_GEAR', 'SLEEPING_FISHING_GEAR',
            ma.action_type = 'CONTROL_SECTOR', 'SECTOR',
            ma.action_type = 'OTHER_CONTROL', 'OTHER_CONTROL',
            ma.action_type = 'CONTROL' AND nullIf(ma.vessel_type, '') IS NOT NULL, 'SHIP',
            ''
        )) AS action_subtype,
        toString(coalesce(
            nullIf(ma.vessel_type, ''),
            nullIf(ma.leisure_type, ''),
            nullIf(ma.fishing_gear_type, ''),
            -- sector_type + sector_establishment_type renseignés ensemble
            -- pour CONTROL_SECTOR (filière + établissement précis) --
            -- concaténés plutôt que coalescés, cf. rapport_pam_ulam_action.sql.
            nullIf(
                arrayStringConcat(arrayFilter(
                    x -> x != '',
                    [coalesce(ma.sector_type, ''), coalesce(ma.sector_establishment_type, '')]
                ), ' / '),
                ''
            ),
            nullIf(ma.control_type, ''),
            ''
        )) AS action_subsubtype,
        -- politique_publique : control_2.control_type prime, repli sur
        -- control_policy_mapping (générique par action_subtype) sinon --
        -- cf. rapport_pam_ulam_action.sql.
        toString(coalesce(
            nullIf(multiIf(
                acp.control_type_predominant = 'NAVIGATION', 'Police de la navigation',
                acp.control_type_predominant = 'GENS_DE_MER', 'Gens de mer',
                acp.control_type_predominant = 'SECURITY', 'Equipement de sécurité',
                acp.control_type_predominant = 'ADMINISTRATIVE', 'Autres',
                ''
            ), ''),
            cpm.politique_publique,
            ''
        )) AS politique_publique,
        toString(coalesce(cpm.thematique, '')) AS thematique,
        toDate(toStartOfMonth(ma.start_datetime_utc)) AS mois,
        toUInt16(coalesce(acl.nb_controls, 0)) AS nb_controles,
        toUInt16(coalesce(acl.nb_infractions_avec_pv, 0)) AS nb_infractions_avec_pv,
        toUInt16(coalesce(acl.nb_infractions_sans_pv, 0)) AS nb_infractions_sans_pv,
        toUInt16(coalesce(acl.nb_infractions_en_attente, 0)) AS nb_infractions_en_attente,
        toUInt16(coalesce(atg.nb_targets, 0)) AS nb_cibles,
        toUInt16(coalesce(ma.nbr_of_control_amp, 0)) AS nb_controles_amp,
        toUInt16(coalesce(ma.nbr_of_control_300m, 0)) AS nb_controles_300m,
        toUInt16(coalesce(ma.has_diving_during_operation, 0)) AS nb_controles_avec_plongee,
        toUInt16(coalesce(ma.is_control_during_security_day, 0)) AS nb_controles_journee_securite
    FROM rapportnav_proxy.mission_action ma
    -- INNER JOIN : filtre aux missions ayant au moins une unité PAM ou
    -- ULAM ; fanout intentionnel 1 ligne par unité individuelle.
    INNER JOIN mission_unit_pairs mup ON mup.mission_id = ma.mission_id
    LEFT JOIN action_controls acl ON acl.action_id = toString(ma.id)
    LEFT JOIN action_targets atg ON atg.action_id = toString(ma.id)
    LEFT JOIN action_control_policy acp ON acp.action_id = toString(ma.id)
    LEFT JOIN control_policy_mapping cpm
        ON cpm.action_subtype_key = multiIf(
            ma.action_type = 'CONTROL_NAUTICAL_LEISURE', 'NAUTICAL_LEISURE',
            ma.action_type = 'CONTROL_SLEEPING_FISHING_GEAR', 'SLEEPING_FISHING_GEAR',
            ma.action_type = 'CONTROL_SECTOR', 'SECTOR',
            ma.action_type = 'OTHER_CONTROL', 'OTHER_CONTROL',
            ma.action_type = 'CONTROL' AND nullIf(ma.vessel_type, '') IS NOT NULL, 'SHIP',
            ''
        )
    WHERE ma.action_type IN ('CONTROL', 'CONTROL_NAUTICAL_LEISURE', 'CONTROL_SLEEPING_FISHING_GEAR', 'CONTROL_SECTOR', 'OTHER_CONTROL')
      AND ma.start_datetime_utc >= toDateTime('2025-01-01 00:00:00')
),

-- ---- Source FISH : contrôles seulement (OBSERVATION exclu) ----
fish_control_rows AS (
    SELECT
        'FISH' AS source,
        f.control_unit_id AS control_unit_id,
        f.control_unit AS unit_name,
        f.facade AS facade,
        toString(multiIf(
            startsWith(upper(f.control_unit), 'PAM'), 'PAM',
            startsWith(upper(f.control_unit), 'ULAM'), 'ULAM',
            'AUTRE'
        )) AS unit_type,
        'FISH' AS action_subtype,
        toString(f.control_type) AS action_subsubtype,
        'Pêche professionnelle' AS politique_publique,
        toString(coalesce(nullIf(f.segment, ''), 'Pêches maritimes')) AS thematique,
        toDate(toStartOfMonth(f.control_datetime_utc)) AS mois,
        toUInt16(1) AS nb_controles,
        toUInt16(f.infraction_report) AS nb_infractions_avec_pv,
        toUInt16(if(f.infraction = 1 AND f.infraction_report = 0, 1, 0)) AS nb_infractions_sans_pv,
        toUInt16(0) AS nb_infractions_en_attente,
        toUInt16(1) AS nb_cibles,
        -- Pas d'équivalent AMP/bande 300/plongée/journée sécu côté FISH.
        toUInt16(0) AS nb_controles_amp,
        toUInt16(0) AS nb_controles_300m,
        toUInt16(0) AS nb_controles_avec_plongee,
        toUInt16(0) AS nb_controles_journee_securite
    FROM monitorfish.analytics_controls_full_data f
    WHERE f.control_type != 'OBSERVATION'
      AND (startsWith(upper(f.control_unit), 'ULAM') OR startsWith(upper(f.control_unit), 'PAM'))
      AND f.control_datetime_utc >= toDateTime('2025-01-01 00:00:00')
),

-- ---- Source ENV : contrôles seulement ----
env_infractions_by_action AS (
    SELECT
        env_action_id,
        countIf(coalesce(infraction_type, '') = 'WITH_REPORT') AS nb_infractions_avec_pv,
        countIf(coalesce(infraction_type, '') = 'WITHOUT_REPORT') AS nb_infractions_sans_pv,
        countIf(coalesce(infraction_type, '') = 'WAITING') AS nb_infractions_en_attente
    FROM monitorenv.actions_infractions
    GROUP BY env_action_id
),
env_control_rows AS (
    SELECT
        'ENV' AS source,
        a.control_unit_id AS control_unit_id,
        a.control_unit AS unit_name,
        a.action_facade AS facade,
        toString(multiIf(
            startsWith(upper(a.control_unit), 'PAM'), 'PAM',
            startsWith(upper(a.control_unit), 'ULAM'), 'ULAM',
            'AUTRE'
        )) AS unit_type,
        toString(a.theme_level_2) AS action_subtype,
        '' AS action_subsubtype,
        'Environnement / pollution' AS politique_publique,
        '' AS thematique,
        toDate(toStartOfMonth(a.action_start_datetime_utc)) AS mois,
        toUInt16(coalesce(a.number_of_controls, 0)) AS nb_controles,
        toUInt16(coalesce(ei.nb_infractions_avec_pv, 0)) AS nb_infractions_avec_pv,
        toUInt16(coalesce(ei.nb_infractions_sans_pv, 0)) AS nb_infractions_sans_pv,
        toUInt16(coalesce(ei.nb_infractions_en_attente, 0)) AS nb_infractions_en_attente,
        toUInt16(1) AS nb_cibles,
        -- Pas d'équivalent AMP/bande 300/plongée/journée sécu côté ENV.
        toUInt16(0) AS nb_controles_amp,
        toUInt16(0) AS nb_controles_300m,
        toUInt16(0) AS nb_controles_avec_plongee,
        toUInt16(0) AS nb_controles_journee_securite
    FROM monitorenv.analytics_actions a
    LEFT JOIN env_infractions_by_action ei ON ei.env_action_id = a.id
    WHERE a.action_type = 'CONTROL'
      AND (
        startsWith(upper(a.control_unit), 'ULAM')
        OR (a.administration = 'DIRM / DM' AND startsWith(upper(a.control_unit), 'PAM'))
      )
      AND a.action_start_datetime_utc >= toDateTime('2025-01-01 00:00:00')
),

all_rows AS (
    SELECT * FROM nav_control_rows
    UNION ALL SELECT * FROM fish_control_rows
    UNION ALL SELECT * FROM env_control_rows
)

SELECT
    source,
    control_unit_id,
    unit_name,
    facade,
    unit_type,
    action_subtype,
    action_subsubtype,
    politique_publique,
    thematique,
    mois,
    sum(nb_controles) AS nb_controles,
    sum(nb_infractions_avec_pv) AS nb_infractions_avec_pv,
    sum(nb_infractions_sans_pv) AS nb_infractions_sans_pv,
    sum(nb_infractions_en_attente) AS nb_infractions_en_attente,
    sum(nb_cibles) AS nb_cibles,
    sum(nb_controles_amp) AS nb_controles_amp,
    sum(nb_controles_300m) AS nb_controles_300m,
    sum(nb_controles_avec_plongee) AS nb_controles_avec_plongee,
    sum(nb_controles_journee_securite) AS nb_controles_journee_securite,
    now() AS updated_at
FROM all_rows
GROUP BY
    source, control_unit_id, unit_name, facade, unit_type,
    action_subtype, action_subsubtype, politique_publique, thematique, mois;
