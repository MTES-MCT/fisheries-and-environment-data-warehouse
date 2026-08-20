-- =====================================================================
-- Alimente rapportnav.fact_controle_croise_pam_ulam.
-- Grain : 1 ligne par unité × statut × origine × type de cible ×
-- conclusion × mois. Table pré-agrégée, prête à être posée directement
-- en Metabase.
--
-- "Contrôle croisé" = rapportnav_proxy.inquiry -- anciennement table
-- cross_control / ActionType.CROSS_CONTROL, RENOMMÉE en "inquiry" par la
-- migration rapportnav2 V1.2025.07.02.08.56__update_cross_control_table_
-- to_inquiry_table.sql (même concept, nouveau nom). Vérifié contre
-- InquiryModel.kt/InquiryStatusType.kt/InquiryOriginType.kt/
-- InquiryConclusionType.kt (rapportnav2) :
--   - status : CLOSED/IN_PROGRESS -> "Contrôles clôturés"/"en cours"
--   - origin : OPPORTUNITY_CONTROL/FOLLOW_UP_CONTROL/CNSP_REPORTING/
--     OTHER_REPORTING/URCEM_DEDICATED_STATION -- match exact avec les 5
--     catégories vues sur la maquette (Contrôle d'opportunité/Suite d'un
--     contrôle physique/Signalement CNSP/Autre signalement/Poste dédié
--     URCEM)
--   - vessel_id vs establishment_id (mutuellement exclusifs) -> "type de
--     cible" (navire/établissement) vu sur la maquette
--   - conclusion : NO_FOLLOW_UP/WITH_REPORT (pas vu explicitement sur la
--     capture, exposé quand même -- classification utile)
--   - start_datetime_utc/end_datetime_utc -> temps total (temps moyen =
--     heures_totales/nb_controles_croises, à calculer côté Metabase)
--
-- NAV-only : ni monitorfish ni monitorenv n'ont de notion de "contrôle
-- croisé" dans les tables déjà construites qu'on réutilise ailleurs.
--
-- Unité : inquiry.service_id -> service_control_unit -> control_unit_id
-- (un service peut être lié à plusieurs unités -- fanout 1 ligne par
-- unité individuelle, même logique que les autres requêtes
-- pam_ulam_*.sql). Filtré aux unités PAM/ULAM via dim_unit_reference.
-- ⚠️ Ce fichier DOIT tourner après dim_unit_reference.sql dans
-- sync_table_from_db_connection.csv (aucune dépendance native entre
-- lignes de ce flow -- cf. commentaire détaillé dans dim_unit_reference.sql).
-- =====================================================================
WITH
-- Référentiel unités PAM/ULAM : source unique rapportnav.dim_unit_reference
-- (scanne en direct monitorenv_proxy.control_units, filtré au nom PAM/ULAM --
-- pas besoin d'ajout manuel pour qu'une unité apparaisse ici, cf. le fix de
-- dim_unit_reference.sql).
pam_ulam_control_units AS (
    SELECT
        control_unit_id,
        facade_ref,
        unit_type
    FROM rapportnav.dim_unit_reference
    WHERE unit_type IN ('PAM', 'ULAM')
),
-- 1 ligne par (service, unité individuelle).
service_units AS (
    SELECT
        scu.service_id,
        cu.id AS control_unit_id,
        cu.name AS unit_name,
        uu.facade_ref AS facade,
        uu.unit_type AS unit_type
    FROM rapportnav_proxy.service_control_unit scu
    INNER JOIN monitorenv_proxy.control_units cu ON cu.id = scu.control_unit_id
    INNER JOIN pam_ulam_control_units uu ON uu.control_unit_id = cu.id
),
-- Isole rapportnav_proxy.inquiry dans sa propre CTE, sans filtre : évite
-- que ClickHouse pousse le WHERE >= toDateTime(...) plus bas directement
-- sur la table PostgreSQL distante -- repéré en CI, cf. discussion en
-- chat : ClickHouse sérialise ce filtre en littéral entier côté Postgres
-- ("start_datetime_utc >= 1735689600"), qui rejette la comparaison
-- timestamptz >= integer (même famille de bug que le has_been_done = true
-- poussé en "= 1" dans le test). rapport_pam_ulam_cible.sql filtre
-- rapportnav_proxy.mission_action de la même façon (table pilote du FROM,
-- WHERE direct) sans ce problème -- mais cette CTE-ci (nav_control_rows)
-- a plusieurs LEFT JOIN supplémentaires vers d'autres CTEs elles-mêmes
-- adossées à rapportnav_proxy (control_2/target_2), contre un seul INNER
-- JOIN ici vers une relation déjà entièrement locale (service_units) --
-- hypothèse : la requête plus simple ici a été jugée éligible à la
-- poussée par l'optimiseur ClickHouse, l'autre non. Pas de certitude
-- absolue (pas de ClickHouse local pour vérifier le mécanisme exact).
inquiry_rows AS (
    SELECT * FROM rapportnav_proxy.inquiry
)

SELECT
    su.control_unit_id AS control_unit_id,
    su.unit_name AS unit_name,
    su.facade AS facade,
    su.unit_type AS unit_type,
    toString(coalesce(i.status, '')) AS statut,
    toString(coalesce(i.origin, '')) AS origine,
    toString(multiIf(
        i.vessel_id IS NOT NULL, 'NAVIRE',
        i.establishment_id IS NOT NULL, 'ETABLISSEMENT',
        ''
    )) AS type_cible,
    toString(coalesce(i.conclusion, '')) AS conclusion,
    -- assumeNotNull : i.start_datetime_utc est Nullable côté proxy, mais le
    -- WHERE en fin de requête (>= 2025-01-01) exclut déjà toute ligne NULL
    -- avant le SELECT -- nécessaire pour servir de clé ORDER BY (même
    -- contrainte allow_nullable_key que rapport_pam_ulam_mission.sql /
    -- missions_aem.sql, repérée en CI -- cf. discussion en chat).
    toDate(toStartOfMonth(assumeNotNull(i.start_datetime_utc))) AS mois,
    count(*) AS nb_controles_croises,
    sum(toFloat64(if(
        i.end_datetime_utc IS NOT NULL AND i.end_datetime_utc >= i.start_datetime_utc,
        dateDiff('second', i.start_datetime_utc, i.end_datetime_utc) / 3600.0,
        0
    ))) AS heures_totales,
    now() AS updated_at
FROM inquiry_rows i
-- INNER JOIN : filtre aux contrôles croisés rattachés à un service ayant
-- au moins une unité PAM ou ULAM ; fanout intentionnel 1 ligne par unité
-- individuelle.
INNER JOIN service_units su ON su.service_id = i.service_id
-- assumeNotNull(...) ici aussi (pas juste un no-op) : empêche ClickHouse
-- de reconnaître un pattern "colonne brute op littéral" poussable vers
-- PostgreSQL -- cf. commentaire sur inquiry_rows plus haut. Sans certitude
-- absolue que ceci suffise (pas de ClickHouse local pour vérifier) ; si le
-- literal DateTime->entier réapparaît en CI malgré la CTE + ce wrapping,
-- il faudra une fixture-level table matérialisée à la place.
WHERE assumeNotNull(i.start_datetime_utc) >= toDateTime('2025-01-01 00:00:00')
GROUP BY
    su.control_unit_id, su.unit_name, su.facade, su.unit_type,
    i.status, i.origin,
    multiIf(
        i.vessel_id IS NOT NULL, 'NAVIRE',
        i.establishment_id IS NOT NULL, 'ETABLISSEMENT',
        ''
    ),
    i.conclusion,
    toDate(toStartOfMonth(assumeNotNull(i.start_datetime_utc)));
