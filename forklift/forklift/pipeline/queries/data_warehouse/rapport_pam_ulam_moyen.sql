-- =====================================================================
-- Alimente rapportnav.fact_moyen_pam_ulam.
-- Grain : 1 ligne par unité × moyen individuel (resource_id/resource_name)
-- × mois. Table pré-agrégée, prête à être posée directement en Metabase.
--
-- ⚠️ RÉÉCRITURE : l'ancienne version groupait par TYPE de moyen (pas par
-- moyen nommé) et filtrait sur `ma.control_type IS NOT NULL` -- un champ
-- qui n'est en réalité renseigné QUE pour OTHER_CONTROL (texte libre, cf.
-- discussion en chat), ce qui excluait CONTROL_NAUTICAL_LEISURE/SECTOR/
-- SLEEPING_FISHING_GEAR ET, plus grave pour cette table, excluait aussi
-- RESOURCES_MAINTENANCE (control_type jamais renseigné dessus) -- les
-- heures d'entretien par moyen étaient donc structurellement absentes de
-- cette table malgré ce que son ancien commentaire affirmait. Corrigé :
-- grain par moyen individuel + actions de la famille CONTROL (nav) ET de
-- maintenance (RESOURCES_MAINTENANCE) toutes deux incluses.
--
-- Moyens = concept NAV uniquement (rapportnav_proxy.mission_action_resource) :
-- ni monitorfish.analytics_controls_full_data ni monitorenv.analytics_actions
-- n'exposent de notion de moyen individuel utilisé -- cette table reste
-- donc nav-only, contrairement à fact_action_pam_ulam/fact_cible_pam_ulam.
--
-- ⚠️ PIÈGE DOUBLE COMPTAGE inchangé : une action mobilisant plusieurs
-- moyens à la fois fait apparaître son plein nb_controles/heures sur
-- CHAQUE moyen. SUM(nb_controles) sommé across plusieurs moyens PEUT donc
-- dépasser le nombre réel de contrôles -- pour un total sans double
-- compte, utiliser fact_action_pam_ulam ou fact_cible_pam_ulam.
--
-- Couvre les unités PAM ET ULAM -- 1 ligne par UNITÉ INDIVIDUELLE (pas de
-- concaténation façon "ULAM 33, ULAM 40") : une mission conjointe donne
-- une ligne par unité participante, chacune créditée du plein indicateur.
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
-- 1 ligne par (mission, unité individuelle) -- nécessaire pour le grain
-- "par unité" demandé ici. facade/unit_type viennent directement de
-- pam_ulam_control_units (garanti présent par l'INNER JOIN, plus besoin
-- de repli).
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
-- Référentiel moyen : nom réel + classification mer/terre/air (même
-- mapping que rapport_pam_ulam_action.sql/rapport_pam_ulam_mission.sql,
-- dupliqué ici, cf. convention du repo).
resource_dim AS (
    SELECT
        id AS resource_id,
        name AS resource_name,
        type AS resource_type_raw,
        multiIf(
            type IN ('AIRPLANE', 'HELICOPTER', 'DRONE'), 'AIR',
            type IN ('CAR', 'MOTORCYCLE', 'PEDESTRIAN', 'EQUESTRIAN'), 'TERRE',
            type IN (
                'BARGE', 'FAST_BOAT', 'FRIGATE', 'HYDROGRAPHIC_SHIP', 'KAYAK',
                'LIGHT_FAST_BOAT', 'MINE_DIVER', 'NET_LIFTER', 'PATROL_BOAT',
                'PIROGUE', 'RIGID_HULL', 'SEA_SCOOTER', 'SEMI_RIGID',
                'SUPPORT_SHIP', 'TRAINING_SHIP', 'TUGBOAT'
            ), 'MER',
            'AUTRE'
        ) AS terrain_category
    FROM monitorenv_proxy.control_unit_resources
),
-- Contrôles/infractions has_been_done=true, collapsés par ACTION -- même
-- logique que control_infraction_flags/action_controls dans
-- rapport_pam_ulam_action.sql : SUM(amount_of_controls) par contrôle
-- ayant au moins une infraction du type recherché.
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
)

SELECT
    mup.control_unit_id AS control_unit_id,
    mup.unit_name AS unit_name,
    mup.facade AS facade,
    mup.unit_type AS unit_type,
    mar.resource_id AS resource_id,
    toString(coalesce(rd.resource_name, '')) AS resource_name,
    toString(coalesce(rd.resource_type_raw, '')) AS resource_type,
    toString(coalesce(rd.terrain_category, 'AUTRE')) AS terrain_category,
    toDate(toStartOfMonth(ma.start_datetime_utc)) AS mois,
    sum(coalesce(ac.nb_controles, 0)) AS nb_controles,
    sum(coalesce(ac.nb_infractions_avec_pv, 0)) AS nb_infractions_avec_pv,
    sum(coalesce(ac.nb_infractions_sans_pv, 0)) AS nb_infractions_sans_pv,
    sum(coalesce(ac.nb_infractions_en_attente, 0)) AS nb_infractions_en_attente,
    sum(coalesce(at.nb_cibles, 0)) AS nb_cibles,
    countIf(ma.action_type IN ('CONTROL', 'CONTROL_NAUTICAL_LEISURE', 'CONTROL_SLEEPING_FISHING_GEAR', 'CONTROL_SECTOR', 'OTHER_CONTROL')) AS nb_actions_controle,
    sumIf(
        toFloat64(if(
            ma.end_datetime_utc IS NOT NULL AND ma.end_datetime_utc >= ma.start_datetime_utc,
            dateDiff('second', ma.start_datetime_utc, ma.end_datetime_utc) / 3600.0,
            coalesce(toFloat64(ma.nbr_of_hours), 0)
        )),
        ma.action_type = 'RESOURCES_MAINTENANCE'
    ) AS heures_entretien,
    countIf(ma.action_type = 'RESOURCES_MAINTENANCE') AS nb_actions_entretien,
    now() AS updated_at
FROM rapportnav_proxy.mission_action ma
-- INNER JOIN : fanout intentionnel 1 ligne par moyen mobilisé sur
-- l'action (cf. ⚠️ PIÈGE DOUBLE COMPTAGE en tête de fichier).
INNER JOIN rapportnav_proxy.mission_action_resource mar ON mar.action_id = ma.id
LEFT JOIN resource_dim rd ON rd.resource_id = mar.resource_id
-- INNER JOIN : filtre aux missions ayant au moins une unité PAM ou ULAM ;
-- fanout intentionnel 1 ligne par unité individuelle (cf. commentaire
-- mission_unit_pairs).
INNER JOIN mission_unit_pairs mup ON mup.mission_id = ma.mission_id
LEFT JOIN action_controls ac ON ac.action_id = toString(ma.id)
LEFT JOIN action_targets at ON at.action_id = toString(ma.id)
-- Périmètre : famille CONTROL nav (contrôles) + RESOURCES_MAINTENANCE
-- (entretien) -- les autres action_type (TRAINING, MEETING, NOTE...)
-- n'ont pas de sens pour un suivi "moyens", cf. fact_action_pam_ulam pour
-- ces action_type.
WHERE (
        ma.action_type IN ('CONTROL', 'CONTROL_NAUTICAL_LEISURE', 'CONTROL_SLEEPING_FISHING_GEAR', 'CONTROL_SECTOR', 'OTHER_CONTROL')
        OR ma.action_type = 'RESOURCES_MAINTENANCE'
      )
  AND ma.start_datetime_utc >= toDateTime('2025-01-01 00:00:00')
GROUP BY
    mup.control_unit_id, mup.unit_name, mup.facade, mup.unit_type,
    mar.resource_id, rd.resource_name, rd.resource_type_raw, rd.terrain_category,
    toDate(toStartOfMonth(ma.start_datetime_utc));
