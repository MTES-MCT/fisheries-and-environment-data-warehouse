-- =====================================================================
-- Alimente rapportnav.fact_cible_pam_ulam (query_filepath pour la ligne
-- "fact_cible_pam_ulam" de sync_table_from_db_connection.csv).
-- Nouvelle table (cf. discussion en chat) -- grain : 1 ligne par unité ×
-- sous-type de contrôle × target_type × mois. Table pré-agrégée, prête à
-- être posée directement en Metabase.
--
-- "Sous-type de contrôle" = mission_action.control_type (cf. commentaire
-- détaillé dans rapport_pam_ulam_moyen.sql -- même champ, même limitation
-- nav-only vérifiée contre rapportnav2).
-- "target_type" = target_2.target_type (DEFAULT/VEHICLE/COMPANY/INDIVIDUAL
-- -- cf. TargetType.kt, rapportnav2) -- classification de CE QUI a été
-- contrôlé, complémentaire de control_type (POURQUOI/quel type de
-- contrôle). Contrairement à rapport_pam_ulam_moyen.sql, PAS de fan-out
-- artificiel ici : control_2.target_id -> target_2.id est un vrai lien
-- (1 contrôle a exactement 1 cible), donc nb_controles/infractions sont
-- correctement attribués à leur target_type réel, pas dupliqués.
--
-- ⚠️ Actions sans control_type renseigné, ou cibles sans contrôle
-- has_been_done=true, sont EXCLUES (cf. même limitation que
-- rapport_pam_ulam_moyen.sql).
--
-- Couvre les unités PAM ET ULAM -- 1 ligne par UNITÉ INDIVIDUELLE (même
-- logique que rapport_pam_ulam_moyen.sql, pas de concaténation
-- "ULAM 33, ULAM 40").
-- ⚠️ Ce fichier DOIT tourner après dim_unit_reference.sql dans
-- sync_table_from_db_connection.csv (aucune dépendance native entre
-- lignes de ce flow -- cf. commentaire détaillé dans dim_unit_reference.sql).
-- =====================================================================
WITH
-- Filtre unités PAM + ULAM (même logique que les autres requêtes) :
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
-- 1 ligne par (mission, unité individuelle) -- cf. rapport_pam_ulam_moyen.sql,
-- même logique, dupliquée ici (requêtes indépendantes, cf. convention du
-- repo).
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
-- Contrôles/infractions has_been_done=true, groupés par (action, cible,
-- target_type) -- PAS collapsé sur toute l'action (contrairement à
-- rapport_pam_ulam_moyen.sql) : ici le lien control_2->target_2 est
-- direct, pas d'approximation à faire.
control_by_target AS (
    SELECT
        c.id AS control_id,
        toString(t.action_id) AS action_id,
        t.id AS target_id,
        toString(t.target_type) AS target_type,
        coalesce(c.amount_of_controls, 0) AS amount_of_controls,
        maxIf(1, coalesce(i.infraction_type, '') = 'WITH_REPORT') AS has_with_report,
        maxIf(1, coalesce(i.infraction_type, '') = 'WITHOUT_REPORT') AS has_without_report,
        maxIf(1, coalesce(i.infraction_type, '') = 'WAITING') AS has_waiting
    FROM rapportnav_proxy.control_2 c
    INNER JOIN rapportnav_proxy.target_2 t ON t.id = c.target_id
    LEFT JOIN rapportnav_proxy.infraction_2 i ON i.control_id = c.id
    WHERE coalesce(c.has_been_done, false) = true
    GROUP BY c.id, t.action_id, t.id, t.target_type, c.amount_of_controls
),
action_target_type_controls AS (
    SELECT
        action_id,
        target_type,
        uniqExact(target_id) AS nb_cibles,
        sum(amount_of_controls) AS nb_controles,
        sumIf(amount_of_controls, has_with_report = 1) AS nb_infractions_avec_pv,
        sumIf(amount_of_controls, has_without_report = 1) AS nb_infractions_sans_pv,
        sumIf(amount_of_controls, has_waiting = 1) AS nb_infractions_en_attente
    FROM control_by_target
    GROUP BY action_id, target_type
)

SELECT
    mup.control_unit_id AS control_unit_id,
    mup.unit_name AS unit_name,
    mup.facade AS facade,
    mup.unit_type AS unit_type,
    toString(ma.control_type) AS sous_type_controle,
    atc.target_type AS target_type,
    toDate(toStartOfMonth(ma.start_datetime_utc)) AS mois,
    sum(atc.nb_controles) AS nb_controles,
    sum(atc.nb_infractions_avec_pv) AS nb_infractions_avec_pv,
    sum(atc.nb_infractions_sans_pv) AS nb_infractions_sans_pv,
    sum(atc.nb_infractions_en_attente) AS nb_infractions_en_attente,
    sum(atc.nb_cibles) AS nb_cibles,
    now() AS updated_at
FROM rapportnav_proxy.mission_action ma
-- INNER JOIN : ne garde que les actions ayant à la fois un control_type
-- renseigné ET au moins un contrôle has_been_done=true (cf. avertissement
-- en tête de fichier).
INNER JOIN action_target_type_controls atc ON atc.action_id = toString(ma.id)
-- INNER JOIN : filtre aux missions ayant au moins une unité PAM ou ULAM ;
-- fanout intentionnel 1 ligne par unité individuelle (cf. commentaire
-- mission_unit_pairs).
INNER JOIN mission_unit_pairs mup ON mup.mission_id = ma.mission_id
WHERE ma.control_type IS NOT NULL AND toString(ma.control_type) != ''
GROUP BY
    mup.control_unit_id, mup.unit_name, mup.facade, mup.unit_type,
    ma.control_type, atc.target_type, toDate(toStartOfMonth(ma.start_datetime_utc));
