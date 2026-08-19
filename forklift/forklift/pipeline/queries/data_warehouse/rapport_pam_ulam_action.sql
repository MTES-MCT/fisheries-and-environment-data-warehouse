-- =====================================================================
-- Alimente rapportnav.fact_action_pam_ulam (query_filepath pour la ligne
-- "fact_action_pam_ulam" de sync_table_from_db_connection.csv).
-- Grain : 1 ligne par action/contrôle individuel × unité individuelle,
-- TOUTES SOURCES CONFONDUES (nav + fish + env, colonne `source`) --
-- fusion de l'ancien fact_action_pam_ulam (nav seul) et de
-- fact_controle_pam_ulam (nav+fish+env, contrôles seulement), décidée en
-- cours de PR : les 2 tables se recouvraient sur les contrôles nav (même
-- donnée, présente 2 fois), source de confusion pour la construction des
-- dashboards. Une seule table maintenant, avec beaucoup de colonnes
-- vides selon la source (assumé -- cf. discussion en chat, le choix
-- explicite a été 1 table plutôt que colonnes plus courtes).
--
-- ⚠️⚠️ POURQUOI 3 SOURCES : rapportnav_proxy.mission_action (nav) ne
-- contient QUE les actions saisies directement dans RapportNav
-- (source=RAPPORT_NAV, vérifié : ni MissionFishActionEntity ni
-- MissionEnvActionEntity n'écrivent dans mission_action, rapportnav2).
-- FISH et ENV n'ont PAS de notion d'activité hors-contrôle (pas de
-- formation/réunion/entretien/permanence côté MonitorFish/MonitorEnv) --
-- seule la source NAV peut donc porter ces action_type là ; FISH/ENV
-- n'apparaissent que pour des contrôles.
-- Sources FISH/ENV déjà construites et actives dans ce repo (PAS créées
-- pour cette PR) :
--   - monitorfish.analytics_controls_full_data (quotidien, cf.
--     sync_table_with_pandas.csv "32 1 * * *")
--   - monitorenv.analytics_actions + monitorenv.actions_infractions
--     (HORAIRE, cf. sync_table_with_pandas.csv "20/22 * * * *")
--
-- ⚠️ 3 modèles de données réellement différents. Points VÉRIFIÉS contre
-- les repos MonitorFish/MonitorEnv/rapportnav2 clonés (backend/src/main/
-- kotlin + migrations SQL) :
--   - mission_id partagé entre les 3 systèmes : CONFIRMÉ. MonitorFish n'a
--     AUCUN concept de mission propre -- il interroge l'API MonitorEnv en
--     direct avec ce même missionId:Int (APIMissionRepository.kt,
--     monitorfish).
--   - control_unit_id : PAS supposé partagé entre systèmes (repli
--     volontaire sur le filtre par NOM d'unité pour fish/env, comme pour
--     nav).
--   - FISH nb_controls = 1 par ligne : CONFIRMÉ -- MissionAction.kt
--     (monitorfish) n'a aucun champ "amount_of_controls", contrairement à
--     rapportnav_proxy.control_2.
--   - FISH infraction avec/sans PV : monitorfish a un InfractionType à 3
--     valeurs (WITH_RECORD/WITHOUT_RECORD/PENDING) mais la table déjà
--     construite monitorfish.analytics_controls_full_data ne calcule QUE
--     infraction_report = présence d'au moins une infraction WITH_RECORD
--     -- WITHOUT_RECORD/PENDING pas distingués dans ses colonnes de
--     sortie (limite de cette table déjà construite, hors périmètre de
--     cette PR). nb_infractions_sans_pv FISH mélange donc WITHOUT_RECORD
--     et PENDING -- cf. nb_infractions_sans_pv_fiable (0 pour FISH).
--   - ENV infraction avec/sans/en attente : CONFIRMÉ fiable.
--     InfractionTypeEnum.kt (monitorenv) a EXACTEMENT les 3 mêmes valeurs
--     (WAITING/WITH_REPORT/WITHOUT_REPORT) que la copie miroir dans
--     rapportnav2.
--   - politique_publique/thematique FISH : MonitorFish n'a AUCUNE table de
--     classification interne (pas de "politique publique", vérifié --
--     control_objectives est un objectif chiffré, pas une classification ;
--     MissionActionType n'a que SEA/LAND/AIR_CONTROL/AIR_SURVEILLANCE/
--     OBSERVATION). MonitorFish EST par construction "politique pêche" --
--     politique_publique = 'Pêches maritimes' fixe, thematique = segment
--     (segment de flotte) si disponible sinon 'Pêches maritimes' aussi
--     (cf. discussion en chat).
--   - politique_publique/thematique ENV : monitorenv.analytics_actions a
--     déjà theme_level_1/theme_level_2 (vraie hiérarchie de thèmes,
--     table `themes`, déjà exploitée en prod ailleurs) -- exposés ici tels
--     quels en colonnes brutes (env_theme_level_1/env_theme_level_2/
--     env_plan). MAIS aucune table de code ne liste les valeurs réelles de
--     theme_level_1 (peuplé dynamiquement en base, pas de seed Flyway,
--     cherché dans migrations + pipeline interne + frontend monitorenv,
--     rien trouvé) -- donc PAS de mapping politique_publique construit
--     pour ENV pour l'instant (politique_publique/thematique restent
--     vides pour la source ENV) : en attente de la vraie liste des thèmes
--     PAM/ULAM (demandée en chat, requête fournie pour l'extraire de
--     monitorenv.analytics_actions). ⚠️ À COMPLÉTER dès que la liste est
--     disponible -- ne pas deviner les libellés.
--
-- Filtre "missions à partir de 2025" appliqué aux 3 sources : sans lui,
-- ENV en particulier récupérerait tout l'historique de
-- monitorenv.analytics_actions à chaque refresh horaire, non utile ici.
--
-- Couvre les unités PAM ET ULAM -- 1 ligne par UNITÉ INDIVIDUELLE (pas de
-- concaténation façon "ULAM 33, ULAM 40" comme dans l'ancienne version nav
-- -- changement nécessaire pour un control_unit_id cohérent avec fish/env,
-- qui n'ont qu'une unité par ligne nativement) : une mission conjointe
-- donne une ligne par unité participante, chacune créditée du plein
-- indicateur.
-- ⚠️ Ce fichier DOIT tourner après dim_unit_reference.sql ET après les
-- flows sync_table_with_pandas qui alimentent monitorfish.analytics_controls_full_data
-- / monitorenv.analytics_actions / monitorenv.actions_infractions (aucune
-- dépendance native entre lignes de ces 2 flows -- cf. commentaire
-- détaillé dans dim_unit_reference.sql).
-- =====================================================================
WITH
-- Filtre unités PAM + ULAM pour la source NAV (même logique que les
-- autres requêtes pam_ulam_*.sql) : service_type via service_control_unit,
-- repli sur le nom si le lien n'est pas renseigné -- constaté non peuplé
-- en pratique (aucune fixture de test ne renseigne service_control_unit).
pam_ulam_control_units AS (
    SELECT DISTINCT cu.id AS control_unit_id
    FROM monitorenv_proxy.control_units cu
    LEFT JOIN rapportnav_proxy.service_control_unit scu ON scu.control_unit_id = cu.id
    LEFT JOIN rapportnav_proxy.service s ON s.id = scu.service_id AND s.deleted_at IS NULL
    WHERE s.service_type IN ('PAM', 'ULAM')
       OR startsWith(upper(cu.name), 'ULAM')
       OR startsWith(upper(cu.name), 'PAM')
),
-- 1 ligne par (mission, unité individuelle) -- cf. avertissement en tête
-- de fichier sur le changement de grain par rapport à l'ancien
-- fact_action_pam_ulam (unit_names concaténé).
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
resource_dim AS (
    SELECT
        id AS resource_id,
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
-- Un moyen (ou plusieurs) par action -> agrégés en tableau, une ligne par
-- action (NAV seul -- fish/env n'exposent pas cette notion dans les
-- tables déjà construites qu'on réutilise).
action_resources AS (
    SELECT
        toString(mar.action_id) AS action_id,
        groupArray(mar.resource_id) AS resource_ids,
        groupArray(toString(rd.resource_type_raw)) AS resource_types,
        -- ⚠️ approximation : si l'action mobilise des moyens de catégories
        -- différentes, on ne garde que le 1er trouvé.
        arrayElement(groupUniqArray(rd.terrain_category), 1) AS terrain_type_first
    FROM rapportnav_proxy.mission_action_resource mar
    LEFT JOIN resource_dim rd ON rd.resource_id = mar.resource_id
    GROUP BY mar.action_id
),
-- Cibles/contrôles/infractions NAV (target_2 -> control_2 -> infraction_2
-- -> infraction_natinf_2). Schéma et logique VÉRIFIÉS contre rapportnav2
-- (migration V1.2025.03.18.16.14, CountInfractions.kt,
-- ComputeNavControlPolicy.kt) : infraction_type a 3 valeurs réelles
-- (WITH_REPORT/WITHOUT_REPORT/WAITING) ; "Nb de ctrl" = SUM(amount_of_controls)
-- des contrôles has_been_done=true, PAS un COUNT de lignes control_2
-- (contrainte UNIQUE(control_type, target_id) : les contrôles répétés
-- s'accumulent dans amount_of_controls plutôt que sur plusieurs lignes).
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
        uniqExactIf(control_id, has_been_done = true) AS nb_control_types,
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
        uniqExact(t.id) AS nb_targets,
        groupUniqArray(n.natinf_code) AS natinf_codes
    FROM rapportnav_proxy.target_2 t
    LEFT JOIN rapportnav_proxy.control_2 c ON c.target_id = t.id AND coalesce(c.has_been_done, false) = true
    LEFT JOIN rapportnav_proxy.infraction_2 i ON i.control_id = c.id
    LEFT JOIN rapportnav_proxy.infraction_natinf_2 n ON n.infraction_id = i.id
    GROUP BY t.action_id
),
-- Référentiel libellé français / politique publique / thématique par
-- action_type NAV, repris du dictionnaire de données métier "Types et
-- sous-types d'actions" (export CSV du 2026-08-14). Clé = action_type
-- seul, sauf UNIT_MANAGEMENT_TRAINING (action_subtype = champ contrôlé,
-- pas du texte libre).
-- ⚠️ TRAINING (le "vrai") : action_subtype vient d'un champ texte libre
-- (ma.training_type), des dizaines de valeurs distinctes dans le
-- dictionnaire source -- impossible à mapper ligne à ligne. Libellé par
-- défaut de l'action_type appliqué à toute action TRAINING quel que soit
-- le texte saisi.
action_type_mapping AS (
    SELECT 'ANTI_POLLUTION' AS action_type, '' AS action_subtype_key, 'Opération de lutte anti-pollution' AS libelle_francais, 'Contrôle des activités maritimes' AS politique_publique, 'Environnement marin' AS thematique
    UNION ALL SELECT 'BAAEM_PERMANENCE', '', 'Permanence BAAEM - bureau de l''action de l''Etat en mer', 'Contrôle des activités maritimes', 'Transversal'
    UNION ALL SELECT 'COMMUNICATION', '', 'Communication', 'Contrôle des activités maritimes', 'Transversal'
    -- Libellés suffixés "?" dans le dictionnaire source (CONTACT, INQUIRY) :
    -- incertitude du métier sur le nom, reprise telle quelle.
    UNION ALL SELECT 'CONTACT', '', 'Accueil public ?', 'Contrôle des activités maritimes', 'Transversal'
    UNION ALL SELECT 'CONTROL', '', 'Contrôle', 'Contrôle des activités maritimes', 'Transversal'
    UNION ALL SELECT 'CONTROL_NAUTICAL_LEISURE', '', 'Contrôle de loisirs nautiques', 'Contrôle des activités maritimes', 'Loisirs nautiques'
    UNION ALL SELECT 'CONTROL_SECTOR', '', 'Thématique de contrôle', 'Contrôle des activités maritimes', 'Transversal'
    UNION ALL SELECT 'CONTROL_SLEEPING_FISHING_GEAR', '', 'Contrôle d''engin de pêche dormant', 'Contrôle des activités maritimes', 'Pêches maritimes'
    UNION ALL SELECT 'HEARING_CONDUCT', '', 'Préparation et conduite d''audition', 'Contrôle des activités maritimes', 'Transversal'
    UNION ALL SELECT 'ILLEGAL_IMMIGRATION', '', 'Opération de lutte contre l''immigration illégale', 'Contrôle des activités maritimes', 'Flux migratoires'
    UNION ALL SELECT 'INQUIRY', '', 'Enquête/ préparation de contrôle ?', 'Contrôle des activités maritimes', 'Transversal'
    UNION ALL SELECT 'LAND_SURVEILLANCE', '', 'Surveillance générale terrestre', 'Contrôle des activités maritimes', 'Transversal'
    UNION ALL SELECT 'MARITIME_SURVEILLANCE', '', 'Surveillance générale maritime', 'Contrôle des activités maritimes', 'Transversal'
    UNION ALL SELECT 'MEETING', '', 'Réunion', 'Contrôle des activités maritimes', 'Transversal'
    UNION ALL SELECT 'NAUTICAL_EVENT', '', 'Surveillance de manifestation nautique', 'Contrôle des activités maritimes', 'Occupation du domaine public maritime'
    UNION ALL SELECT 'NOTE', '', 'Note libre', 'Contrôle des activités maritimes', 'Transversal'
    UNION ALL SELECT 'OTHER', '', 'Autre (vie et gestion de l''unité)', 'Contrôle des activités maritimes', 'Transversal'
    UNION ALL SELECT 'OTHER_CONTROL', '', 'Autre contrôle', 'Contrôle des activités maritimes', 'Transversal'
    UNION ALL SELECT 'PUBLIC_ORDER', '', 'Ordre public', 'Maintien de l''ordre public', ''
    UNION ALL SELECT 'PV_DRAFTING', '', 'Rédaction de PV', 'Contrôle des activités maritimes', 'Transversal'
    UNION ALL SELECT 'REPRESENTATION', '', 'Représentation', 'Contrôle des activités maritimes', 'Transversal'
    UNION ALL SELECT 'RESCUE', '', 'Assistance/ sauvetage', 'Assistance/ sauvetage', 'Assistance/ sauvetage'
    UNION ALL SELECT 'RESOURCES_MAINTENANCE', '', 'Entretien des moyens', 'Contrôle des activités maritimes', 'Transversal'
    UNION ALL SELECT 'SECURITY_VISIT', '', 'Visite sécurité', 'Contrôle des activités maritimes', 'Transversal'
    UNION ALL SELECT 'TRAINING', '', 'Entraînement', 'Contrôle des activités maritimes', 'Transversal'
    UNION ALL SELECT 'UNIT_MANAGEMENT_OTHER', '', 'Gestion de l''unité - autres', 'Contrôle des activités maritimes', 'Transversal'
    UNION ALL SELECT 'UNIT_MANAGEMENT_PLANNING', '', 'Gestion de l''unité - planning', 'Contrôle des activités maritimes', 'Transversal'
    UNION ALL SELECT 'UNIT_MANAGEMENT_TRAINING', '', 'Entraînement', 'Contrôle des activités maritimes', 'Transversal'
    UNION ALL SELECT 'UNIT_MANAGEMENT_TRAINING', 'DIVING', 'Entraînement', 'Contrôle des activités maritimes', 'Transversal'
    UNION ALL SELECT 'UNIT_MANAGEMENT_TRAINING', 'MAN_OVERBOARD_RECOVERY', 'Formation', 'Assistance/ sauvetage', 'Assistance/ sauvetage'
    UNION ALL SELECT 'UNIT_MANAGEMENT_TRAINING', 'TECHNICAL_INTERVENTION_SHOOTING', 'Formation', 'Maintien de l''ordre public', 'Gestes Techniques Professionnels d''Intervention'
),

-- ---- Source NAV : toutes les actions (contrôles et non-contrôles) ----
nav_rows AS (
    SELECT
        'NAV' AS source,
        toString(ma.id) AS action_id,
        ma.mission_id AS mission_id,
        mup.control_unit_id AS control_unit_id,
        mup.unit_name AS unit_name,
        mup.facade AS facade,
        mup.unit_type AS unit_type,
        toDateTime64(ma.start_datetime_utc, 6) AS start_datetime_utc,
        toDateTime64(ma.end_datetime_utc, 6) AS end_datetime_utc,
        toFloat64(if(
            ma.end_datetime_utc IS NOT NULL AND ma.end_datetime_utc >= ma.start_datetime_utc,
            dateDiff('second', ma.start_datetime_utc, ma.end_datetime_utc) / 3600.0,
            coalesce(toFloat64(ma.nbr_of_hours), 0)
        )) AS duration_h,
        toString(ma.action_type) AS action_type,
        toString(multiIf(
            ma.action_type = 'TRAINING', coalesce(ma.training_type, ''),
            ma.action_type = 'UNIT_MANAGEMENT_TRAINING', coalesce(ma.unit_management_training_type, ''),
            ma.action_type = 'RESOURCES_MAINTENANCE', coalesce(ma.resource_type, ''),
            coalesce(ma.reason, '')
        )) AS action_subtype,
        toString(coalesce(ma.control_type, '')) AS control_type,
        toString(coalesce(nullIf(ma.vessel_type, ''), nullIf(ma.leisure_type, ''), nullIf(ma.fishing_gear_type, ''), nullIf(ma.sector_establishment_type, ''), '')) AS cible_label,
        toString(coalesce(ma.vessel_type, '')) AS vessel_type,
        toString(coalesce(ma.vessel_size, '')) AS vessel_size,
        toString(coalesce(ma.leisure_type, '')) AS leisure_type,
        toString(coalesce(ma.fishing_gear_type, '')) AS fishing_gear_type,
        toString(coalesce(ma.sector_establishment_type, '')) AS sector_establishment_type,
        toString(coalesce(ma.security_visit_type, '')) AS security_visit_type,
        toString(coalesce(nullIf(atm.libelle_francais, ''), toString(ma.action_type))) AS libelle_francais,
        toString(coalesce(atm.politique_publique, '')) AS politique_publique,
        toString(coalesce(atm.thematique, '')) AS thematique,
        '' AS env_theme_level_1,
        '' AS env_theme_level_2,
        '' AS env_plan,
        toUInt16(coalesce(atg.nb_targets, 0)) AS nb_targets,
        toUInt16(coalesce(acl.nb_control_types, 0)) AS nb_control_types,
        toUInt16(coalesce(acl.nb_controls, 0)) AS nb_controls,
        toUInt16(coalesce(acl.nb_infractions_avec_pv, 0)) AS nb_infractions_avec_pv,
        toUInt16(coalesce(acl.nb_infractions_sans_pv, 0)) AS nb_infractions_sans_pv,
        toUInt8(1) AS nb_infractions_sans_pv_fiable,
        toUInt16(coalesce(acl.nb_infractions_en_attente, 0)) AS nb_infractions_en_attente,
        arrayMap(x -> toString(x), coalesce(atg.natinf_codes, [])) AS natinf_codes,
        toUInt16(coalesce(ma.nbr_of_control, 0)) AS nbr_of_control_declare,
        toUInt16(coalesce(ma.nbr_of_control_amp, 0)) AS nbr_of_control_amp,
        toUInt16(coalesce(ma.nbr_of_control_300m, 0)) AS nbr_of_control_300m,
        toUInt16(coalesce(ma.nbr_security_visit, 0)) AS nbr_security_visit,
        toUInt8(coalesce(ma.is_control_during_security_day, 0)) AS is_control_during_security_day,
        toUInt8(coalesce(ma.is_seizure_sleeping_fishing_gear, 0)) AS is_seizure_sleeping_fishing_gear,
        toUInt8(coalesce(ma.has_diving_during_operation, 0)) AS has_diving_during_operation,
        toUInt8(coalesce(ma.is_complete_for_stats, 0)) AS is_complete_for_stats,
        ma.nbr_of_hours AS nbr_of_hours_declared,
        toUInt16(length(coalesce(ar.resource_ids, []))) AS nb_resources_linked,
        coalesce(ar.resource_ids, []) AS resource_ids,
        coalesce(ar.resource_types, []) AS resource_types,
        toString(coalesce(ar.terrain_type_first, 'INDETERMINE')) AS terrain_type,
        toString(coalesce(est.name, '')) AS establishment_name,
        toString(coalesce(est.siren, '')) AS establishment_siren,
        toString(coalesce(est.city, '')) AS establishment_city,
        ma.latitude AS latitude,
        ma.longitude AS longitude
    FROM rapportnav_proxy.mission_action ma
    -- INNER JOIN (pas LEFT) : filtre aux actions dont la mission a au
    -- moins une unité PAM ou ULAM.
    INNER JOIN mission_unit_pairs mup ON mup.mission_id = ma.mission_id
    LEFT JOIN rapportnav_proxy.establishment est ON est.id = ma.establishment_id
    LEFT JOIN action_resources ar ON ar.action_id = toString(ma.id)
    LEFT JOIN action_targets atg ON atg.action_id = toString(ma.id)
    LEFT JOIN action_controls acl ON acl.action_id = toString(ma.id)
    -- action_subtype_key : ne différencie que UNIT_MANAGEMENT_TRAINING.
    LEFT JOIN action_type_mapping atm
        ON atm.action_type = toString(ma.action_type)
        AND atm.action_subtype_key = multiIf(
            ma.action_type = 'UNIT_MANAGEMENT_TRAINING', coalesce(ma.unit_management_training_type, ''),
            ''
        )
    -- STATUS = marqueurs de changement d'état nav (ANCHORED/NAVIGATING/...),
    -- pas une "activité" au sens métier du rapport.
    WHERE ma.action_type != 'STATUS'
      AND ma.start_datetime_utc >= toDateTime('2025-01-01 00:00:00')
),

-- ---- Source FISH : contrôles seulement ----
fish_rows AS (
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
        toDateTime64(f.control_datetime_utc, 6) AS start_datetime_utc,
        toDateTime64(f.control_datetime_utc, 6) AS end_datetime_utc,
        toFloat64(0) AS duration_h,
        toString(f.control_type) AS action_type,
        '' AS action_subtype,
        toString(f.control_type) AS control_type,
        toString(coalesce(f.vessel_name, '')) AS cible_label,
        '' AS vessel_type,
        '' AS vessel_size,
        '' AS leisure_type,
        '' AS fishing_gear_type,
        '' AS sector_establishment_type,
        '' AS security_visit_type,
        toString(f.control_type) AS libelle_francais,
        -- Politique publique/thématique FIXES : MonitorFish n'a aucune
        -- classification interne, cf. avertissement en tête de fichier.
        -- segment = segment de flotte contrôlé, seule granularité
        -- disponible pour thematique.
        'Pêches maritimes' AS politique_publique,
        toString(coalesce(nullIf(f.segment, ''), 'Pêches maritimes')) AS thematique,
        '' AS env_theme_level_1,
        '' AS env_theme_level_2,
        '' AS env_plan,
        toUInt16(1) AS nb_targets,
        toUInt16(1) AS nb_control_types,
        toUInt16(1) AS nb_controls,
        toUInt16(f.infraction_report) AS nb_infractions_avec_pv,
        toUInt16(if(f.infraction = 1 AND f.infraction_report = 0, 1, 0)) AS nb_infractions_sans_pv,
        toUInt8(0) AS nb_infractions_sans_pv_fiable,
        toUInt16(0) AS nb_infractions_en_attente,
        f.infraction_natinfs AS natinf_codes,
        toUInt16(0) AS nbr_of_control_declare,
        toUInt16(0) AS nbr_of_control_amp,
        toUInt16(0) AS nbr_of_control_300m,
        toUInt16(0) AS nbr_security_visit,
        toUInt8(0) AS is_control_during_security_day,
        toUInt8(0) AS is_seizure_sleeping_fishing_gear,
        toUInt8(0) AS has_diving_during_operation,
        toUInt8(1) AS is_complete_for_stats,
        toNullable(toInt32(0)) AS nbr_of_hours_declared,
        toUInt16(0) AS nb_resources_linked,
        CAST([], 'Array(Int32)') AS resource_ids,
        CAST([], 'Array(String)') AS resource_types,
        'MER' AS terrain_type,
        '' AS establishment_name,
        '' AS establishment_siren,
        '' AS establishment_city,
        f.latitude AS latitude,
        f.longitude AS longitude
    FROM monitorfish.analytics_controls_full_data f
    WHERE (startsWith(upper(f.control_unit), 'ULAM') OR startsWith(upper(f.control_unit), 'PAM'))
      AND f.control_datetime_utc >= toDateTime('2025-01-01 00:00:00')
),

-- ---- Source ENV : contrôles seulement ----
env_infractions_by_action AS (
    SELECT
        env_action_id,
        countIf(coalesce(infraction_type, '') = 'WITH_REPORT') AS nb_infractions_avec_pv,
        countIf(coalesce(infraction_type, '') = 'WITHOUT_REPORT') AS nb_infractions_sans_pv,
        countIf(coalesce(infraction_type, '') = 'WAITING') AS nb_infractions_en_attente,
        groupUniqArray(arrayJoin(natinf)) AS natinf_codes
    FROM monitorenv.actions_infractions
    GROUP BY env_action_id
),
env_rows AS (
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
        toDateTime64(a.action_start_datetime_utc, 6) AS start_datetime_utc,
        toDateTime64(coalesce(a.action_end_datetime_utc, a.action_start_datetime_utc), 6) AS end_datetime_utc,
        toFloat64(0) AS duration_h,
        toString(a.action_type) AS action_type,
        '' AS action_subtype,
        '' AS control_type,
        '' AS cible_label,
        '' AS vessel_type,
        '' AS vessel_size,
        '' AS leisure_type,
        '' AS fishing_gear_type,
        '' AS sector_establishment_type,
        '' AS security_visit_type,
        toString(a.theme_level_1) AS libelle_francais,
        -- ⚠️ EN ATTENTE : pas de mapping theme_level_1 -> politique_publique
        -- construit -- vraie liste des thèmes non trouvée dans le code
        -- (cf. avertissement détaillé en tête de fichier). Laissé vide
        -- plutôt que deviné. thematique brute exposée via
        -- env_theme_level_1/2 ci-dessous en attendant.
        '' AS politique_publique,
        '' AS thematique,
        toString(a.theme_level_1) AS env_theme_level_1,
        toString(a.theme_level_2) AS env_theme_level_2,
        toString(a.plan) AS env_plan,
        toUInt16(1) AS nb_targets,
        toUInt16(1) AS nb_control_types,
        toUInt16(coalesce(a.number_of_controls, 0)) AS nb_controls,
        toUInt16(coalesce(ei.nb_infractions_avec_pv, 0)) AS nb_infractions_avec_pv,
        toUInt16(coalesce(ei.nb_infractions_sans_pv, 0)) AS nb_infractions_sans_pv,
        toUInt8(1) AS nb_infractions_sans_pv_fiable,
        toUInt16(coalesce(ei.nb_infractions_en_attente, 0)) AS nb_infractions_en_attente,
        arrayMap(x -> toString(x), coalesce(ei.natinf_codes, [])) AS natinf_codes,
        toUInt16(0) AS nbr_of_control_declare,
        toUInt16(0) AS nbr_of_control_amp,
        toUInt16(0) AS nbr_of_control_300m,
        toUInt16(0) AS nbr_security_visit,
        toUInt8(0) AS is_control_during_security_day,
        toUInt8(0) AS is_seizure_sleeping_fishing_gear,
        toUInt8(0) AS has_diving_during_operation,
        toUInt8(1) AS is_complete_for_stats,
        toNullable(toInt32(0)) AS nbr_of_hours_declared,
        toUInt16(0) AS nb_resources_linked,
        CAST([], 'Array(Int32)') AS resource_ids,
        CAST([], 'Array(String)') AS resource_types,
        'MER' AS terrain_type,
        '' AS establishment_name,
        '' AS establishment_siren,
        '' AS establishment_city,
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

SELECT *, now() AS updated_at FROM nav_rows
UNION ALL
SELECT *, now() AS updated_at FROM fish_rows
UNION ALL
SELECT *, now() AS updated_at FROM env_rows;
