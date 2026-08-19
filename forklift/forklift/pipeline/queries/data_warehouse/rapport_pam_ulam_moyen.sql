-- =====================================================================
-- Alimente rapportnav.fact_moyen_pam_ulam (query_filepath pour la ligne
-- "fact_moyen_pam_ulam" de sync_table_from_db_connection.csv).
-- Grain (changé en cours de PR, cf. discussion en chat) : 1 ligne par
-- unité × sous-type de contrôle × type de moyen × mois -- PAS 1 ligne par
-- (mission, action, moyen) comme dans la version précédente. Table
-- pré-agrégée, prête à être posée directement en Metabase.
--
-- "Sous-type de contrôle" = mission_action.control_type (ADMINISTRATIVE,
-- GENS_DE_MER, NAVIGATION, SECURITY, SECTOR, TRANSPORT,
-- LANDING_OBLIGATION, FISHING_REPORTING_OBLIGATION, TECHNICAL_MEASURE,
-- INN_ACTIVITY, OTHER -- cf. ControlType.kt, rapportnav2).
-- ⚠️ VÉRIFIÉ contre rapportnav2 (repo cloné et inspecté) : control_type et
-- ses champs frères (leisure_type, fishing_gear_type,
-- sector_establishment_type, nbr_of_control_amp/300m -- ajoutés par
-- V1.2025.09.23.17.30__alter_mission_action_action_new_column.sql)
-- vivent sur mission_action, PAS sur target_2/control_2. Confirmé
-- nav-only : seule MissionNavActionEntity a une méthode
-- toMissionActionModel() qui écrit dans mission_action -- ni
-- MissionFishActionEntity ni MissionEnvActionEntity n'y écrivent jamais.
-- Cette table hérite donc de la même limitation nav-only que le reste de
-- fact_action_pam_ulam (pas un trou nouveau, déjà signalé).
--
-- ⚠️ Actions sans control_type renseigné (TRAINING, RESOURCES_MAINTENANCE,
-- MEETING, NOTE...) sont EXCLUES de cette table -- "sous-type de
-- contrôle" n'a pas de sens pour elles. Pour un suivi des heures
-- d'entretien par moyen, utiliser fact_action_pam_ulam (grain action, non
-- affecté par ce filtre).
--
-- ⚠️ PIÈGE DOUBLE COMPTAGE (même philosophie que l'ancienne version) : une
-- action a 1 seul control_type mais peut mobiliser plusieurs moyens de
-- types différents -- chaque moyen porte le plein nb_controles/infractions
-- de l'action. SUM(nb_controles) sur cette table, sommé across plusieurs
-- types de moyen pour un même sous_type_controle, PEUT donc dépasser le
-- nombre réel de contrôles. Pour un total sans double compte, utiliser
-- fact_action_pam_ulam ou fact_cible_pam_ulam (pas de moyen là-bas).
--
-- Couvre les unités PAM ET ULAM -- 1 ligne par UNITÉ INDIVIDUELLE (pas de
-- concaténation façon "ULAM 33, ULAM 40" comme dans les autres requêtes
-- pam_ulam_*.sql) : une mission conjointe donne une ligne par unité
-- participante, chacune créditée du plein indicateur (cf. discussion en
-- chat sur le grain demandé "par unité").
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
-- 1 ligne par (mission, unité individuelle) -- PAS agrégé en une seule
-- ligne par mission comme mission_units dans les autres requêtes
-- pam_ulam_*.sql : nécessaire pour le grain "par unité" demandé ici.
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
-- Contrôles/infractions has_been_done=true, collapsés par ACTION (pas par
-- control_type -- redondant ici, mission_action.control_type fournit déjà
-- une seule valeur par action). Même logique vérifiée que
-- control_infraction_flags/action_controls dans rapport_pam_ulam_action.sql
-- (CountInfractions.countNavInfractions, rapportnav2 : SUM(amount_of_controls)
-- par contrôle ayant au moins une infraction du type recherché).
control_infraction_flags AS (
    SELECT
        c.id AS control_id,
        toString(t.action_id) AS action_id,
        coalesce(c.amount_of_controls, 0) AS amount_of_controls,
        maxIf(1, coalesce(i.infraction_type, '') = 'WITH_REPORT') AS has_with_report,
        maxIf(1, coalesce(i.infraction_type, '') = 'WITHOUT_REPORT') AS has_without_report,
        maxIf(1, coalesce(i.infraction_type, '') = 'WAITING') AS has_waiting
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
        sumIf(amount_of_controls, has_without_report = 1) AS nb_infractions_sans_pv,
        sumIf(amount_of_controls, has_waiting = 1) AS nb_infractions_en_attente
    FROM control_infraction_flags
    GROUP BY action_id
),
action_targets AS (
    SELECT
        toString(t.action_id) AS action_id,
        uniqExact(t.id) AS nb_cibles
    FROM rapportnav_proxy.target_2 t
    INNER JOIN rapportnav_proxy.control_2 c ON c.target_id = t.id AND coalesce(c.has_been_done, false) = true
    GROUP BY t.action_id
),
-- Moyens par (action, type de moyen).
action_resource_by_type AS (
    SELECT
        toString(mar.action_id) AS action_id,
        toString(coalesce(cur.type, '')) AS resource_type,
        uniqExact(mar.resource_id) AS nb_moyens
    FROM rapportnav_proxy.mission_action_resource mar
    LEFT JOIN monitorenv_proxy.control_unit_resources cur ON cur.id = mar.resource_id
    GROUP BY mar.action_id, cur.type
)

SELECT
    mup.control_unit_id AS control_unit_id,
    mup.unit_name AS unit_name,
    mup.facade AS facade,
    mup.unit_type AS unit_type,
    toString(ma.control_type) AS sous_type_controle,
    ares.resource_type AS type_moyen,
    toDate(toStartOfMonth(ma.start_datetime_utc)) AS mois,
    sum(coalesce(ac.nb_controles, 0)) AS nb_controles,
    sum(coalesce(ac.nb_infractions_avec_pv, 0)) AS nb_infractions_avec_pv,
    sum(coalesce(ac.nb_infractions_sans_pv, 0)) AS nb_infractions_sans_pv,
    sum(coalesce(ac.nb_infractions_en_attente, 0)) AS nb_infractions_en_attente,
    sum(coalesce(at.nb_cibles, 0)) AS nb_cibles,
    sum(ares.nb_moyens) AS nb_moyens,
    now() AS updated_at
FROM rapportnav_proxy.mission_action ma
-- INNER JOIN : ne garde que les actions ayant un control_type renseigné
-- (cf. avertissement en tête de fichier).
-- INNER JOIN : produit croisé control_type (1 valeur/action) × resource_type
-- (cf. ⚠️ PIÈGE DOUBLE COMPTAGE en tête de fichier).
INNER JOIN action_resource_by_type ares ON ares.action_id = toString(ma.id)
LEFT JOIN action_controls ac ON ac.action_id = toString(ma.id)
LEFT JOIN action_targets at ON at.action_id = toString(ma.id)
-- INNER JOIN : filtre aux missions ayant au moins une unité PAM ou ULAM ;
-- fanout intentionnel 1 ligne par unité individuelle (cf. commentaire
-- mission_unit_pairs).
INNER JOIN mission_unit_pairs mup ON mup.mission_id = ma.mission_id
WHERE ma.control_type IS NOT NULL AND toString(ma.control_type) != ''
GROUP BY
    mup.control_unit_id, mup.unit_name, mup.facade, mup.unit_type,
    ma.control_type, ares.resource_type, toDate(toStartOfMonth(ma.start_datetime_utc));
