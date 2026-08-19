-- =====================================================================
-- Alimente rapportnav.fact_action_pam_ulam.
-- Grain : 1 ligne par action/contrôle individuel × unité individuelle,
-- toutes sources confondues (nav + fish + env, colonne `source`) --
-- beaucoup de colonnes vides selon la source. Option non retenue :
-- séparer CONTROLE / AUTRES ACTIONS pour minimiser les colonnes vides.
--
-- Pourquoi 3 sources : mission_action (nav) ne contient que les actions
-- saisies dans RapportNav. FISH/ENV n'ont pas de notion d'activité
-- hors-contrôle -- ils n'apparaissent donc que pour des contrôles, via
-- les tables déjà construites (pas créées pour cette table) :
--   - monitorfish.analytics_controls_full_data (quotidien)
--   - monitorenv.analytics_actions + monitorenv.actions_infractions (horaire)
--
-- Différences entre les 3 modèles :
--   - mission_id est partagé entre les 3 systèmes (MonitorFish n'a pas de
--     notion de mission propre, il interroge l'API MonitorEnv).
--   - control_unit_id EST partagé entre les 3 systèmes : monitorfish.
--     analytics_control_units est une copie directe (même id) de
--     monitorenv.control_units, et monitorenv.analytics_actions vit dans
--     la même base que control_units. Non exploité ici pour l'instant --
--     unit_type/facade FISH/ENV restent dérivés du nom (startsWith PAM/
--     ULAM) plutôt que d'un join sur dim_unit_reference via
--     control_unit_id, ce qui serait plus robuste.
--   - FISH : nb_controls toujours 1 (pas de amount_of_controls côté
--     MonitorFish). analytics_controls_full_data n'expose que la
--     présence d'une infraction WITH_RECORD -- nb_infractions_sans_pv
--     FISH mélange donc WITHOUT_RECORD et PENDING (cf.
--     nb_infractions_sans_pv_fiable = 0 pour FISH).
--   - ENV : infraction_type a 3 valeurs fiables (WAITING/WITH_REPORT/
--     WITHOUT_REPORT).
--   - politique_publique/thematique FISH : fixe 'Pêches maritimes' (pas
--     de classification interne côté MonitorFish), thematique = segment
--     de flotte si disponible.
--   - politique_publique/thematique ENV : vides pour l'instant --
--     theme_level_1 (monitorenv.analytics_actions) n'a pas de liste de
--     valeurs de référence connue, à compléter dès qu'elle est fournie
--     (ne pas deviner les libellés). Bruts exposés en attendant via
--     env_theme_level_1/env_theme_level_2/env_plan.
--
-- Filtre "missions à partir de 2025" sur les 3 sources (sinon ENV
-- récupère tout l'historique à chaque refresh horaire).
--
-- 1 ligne par unité individuelle (pas de concaténation "ULAM 33, ULAM
-- 40") : cohérent avec fish/env qui n'ont qu'une unité par ligne.
-- Doit tourner après dim_unit_reference.sql et après les flows
-- sync_table_with_pandas qui alimentent les tables fish/env ci-dessus.
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
-- Moyens par action, agrégés en tableau (NAV seul -- fish/env n'exposent
-- pas cette notion dans les tables déjà construites qu'on réutilise).
action_resources AS (
    SELECT
        toString(mar.action_id) AS action_id,
        groupArray(mar.resource_id) AS resource_ids,
        groupArray(toString(rd.resource_type_raw)) AS resource_types,
        -- Liste complète (dédupliquée) des terrains associés aux moyens
        -- employés sur l'action -- une action peut mobiliser des moyens de
        -- catégories différentes (ex : véhicule + navire), donc pas de
        -- réduction au 1er trouvé.
        groupUniqArray(rd.terrain_category) AS terrain_types
    FROM rapportnav_proxy.mission_action_resource mar
    LEFT JOIN resource_dim rd ON rd.resource_id = mar.resource_id
    GROUP BY mar.action_id
),
-- Cibles/contrôles/infractions NAV (target_2 -> control_2 -> infraction_2
-- -> infraction_natinf_2). infraction_type a 3 valeurs (WITH_REPORT/
-- WITHOUT_REPORT/WAITING) ; "Nb de ctrl" = SUM(amount_of_controls) des
-- contrôles has_been_done=true, PAS un COUNT de lignes control_2 --
-- contrainte UNIQUE(control_type, target_id) : les contrôles répétés
-- s'accumulent dans amount_of_controls plutôt que sur plusieurs lignes.
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
-- action_type NAV (dictionnaire métier "Types et sous-types d'actions",
-- export CSV du 2026-08-14).
-- CONTROL_NAUTICAL_LEISURE/CONTROL_SLEEPING_FISHING_GEAR/CONTROL_SECTOR/
-- OTHER_CONTROL (4 anciens action_type) sont regroupés sous
-- action_type='CONTROL', différencié par action_subtype (vessel_type/
-- leisure_type/fishing_gear_type/sector_type/control_type ne sont jamais
-- renseignés ensemble sur une action -- un seul champ "détail" par
-- action_type). Un 5e sous-type SHIP couvre le contrôle navire générique
-- (raw action_type='CONTROL' avec vessel_type renseigné -- COMMERCIAL/
-- FISHING/MOTOR/SAILING/...), pour distinguer "contrôle navire" des 4
-- familles ci-dessus. SECURITY_VISIT reste à part (security_visit_type
-- est un vrai enum mais n'a pas de granularité de mapping dédiée, clé '').
-- Clé = (action_type, action_subtype_key) -- seuls CONTROL et
-- UNIT_MANAGEMENT_TRAINING ont un sous-type mappé, le reste utilise ''.
-- TRAINING : action_subtype vient d'un champ texte libre (ma.training_type,
-- des dizaines de valeurs) -- libellé par défaut appliqué quel que soit le
-- texte saisi (clé '', jamais ma.training_type).
--
-- categorie_activite/sous_categorie_activite : taxonomie "8 catégories"
-- de la maquette Bilan opérationnel (Contrôles / Surveillances /
-- Assistances-sauvetages / Autre activité terrain / Préparation et suivi
-- des ctrl / Accueil public-communication / Formations / Vie et gestion
-- de l'unité), dérivée de action_type -- distincte de politique_publique/
-- thematique (classification différente, sur le même référentiel source).
-- ⚠️ Mapping construit par déduction du nom d'action_type, PAS confirmé
-- par le métier : PUBLIC_ORDER -> "Sûreté maritime" (déduit du camembert
-- "Autres activités terrain" vu en maquette, à valider) ; RESCUE non
-- scindé en Assistance/Sauvetage (pas de champ source pour cette
-- distinction, sous_categorie_activite reprend le libellé générique) ;
-- BAAEM_PERMANENCE/NOTE classés en "Vie et gestion de l'unité" par défaut.
action_type_mapping AS (
    SELECT 'ANTI_POLLUTION' AS action_type, '' AS action_subtype_key, 'Opération de lutte anti-pollution' AS libelle_francais, 'Contrôle des activités maritimes' AS politique_publique, 'Environnement marin' AS thematique, 'Autre activité terrain' AS categorie_activite, 'Lutte anti-pollution' AS sous_categorie_activite
    UNION ALL SELECT 'BAAEM_PERMANENCE', '', 'Permanence BAAEM - bureau de l''action de l''Etat en mer', 'Contrôle des activités maritimes', 'Transversal', 'Vie et gestion de l''unité', 'Autre'
    UNION ALL SELECT 'COMMUNICATION', '', 'Communication', 'Contrôle des activités maritimes', 'Transversal', 'Accueil public/communication', 'Communication'
    -- Libellés suffixés "?" dans le dictionnaire source (CONTACT, INQUIRY) :
    -- incertitude du métier sur le nom, reprise telle quelle.
    UNION ALL SELECT 'CONTACT', '', 'Accueil public ?', 'Contrôle des activités maritimes', 'Transversal', 'Accueil public/communication', 'Accueil public'
    -- Famille CONTROL unifiée (cf. avertissement ci-dessus) : 5 lignes,
    -- une par ancien action_type de la famille.
    UNION ALL SELECT 'CONTROL', '', 'Contrôle', 'Contrôle des activités maritimes', 'Transversal', 'Contrôles', 'Contrôle'
    UNION ALL SELECT 'CONTROL', 'NAUTICAL_LEISURE', 'Contrôle de loisirs nautiques', 'Contrôle des activités maritimes', 'Loisirs nautiques', 'Contrôles', 'Loisirs nautiques'
    UNION ALL SELECT 'CONTROL', 'SECTOR', 'Thématique de contrôle', 'Contrôle des activités maritimes', 'Transversal', 'Contrôles', 'Contrôle sectoriel'
    UNION ALL SELECT 'CONTROL', 'SLEEPING_FISHING_GEAR', 'Contrôle d''engin de pêche dormant', 'Contrôle des activités maritimes', 'Pêches maritimes', 'Contrôles', 'Engins de pêche dormant'
    UNION ALL SELECT 'CONTROL', 'OTHER_CONTROL', 'Autre contrôle', 'Contrôle des activités maritimes', 'Transversal', 'Contrôles', 'Autre contrôle'
    UNION ALL SELECT 'CONTROL', 'SHIP', 'Contrôle de navire', 'Contrôle des activités maritimes', 'Transversal', 'Contrôles', 'Contrôle navires'
    UNION ALL SELECT 'HEARING_CONDUCT', '', 'Préparation et conduite d''audition', 'Contrôle des activités maritimes', 'Transversal', 'Préparation et suivi des ctrl', 'Préparation et conduite d''audition'
    UNION ALL SELECT 'ILLEGAL_IMMIGRATION', '', 'Opération de lutte contre l''immigration illégale', 'Contrôle des activités maritimes', 'Flux migratoires', 'Autre activité terrain', 'Lutte contre l''immigration illégale'
    UNION ALL SELECT 'INQUIRY', '', 'Enquête/ préparation de contrôle ?', 'Contrôle des activités maritimes', 'Transversal', 'Préparation et suivi des ctrl', 'Préparation de contrôle'
    UNION ALL SELECT 'LAND_SURVEILLANCE', '', 'Surveillance générale terrestre', 'Contrôle des activités maritimes', 'Transversal', 'Surveillances', 'Surveillance générale terrestre'
    UNION ALL SELECT 'MARITIME_SURVEILLANCE', '', 'Surveillance générale maritime', 'Contrôle des activités maritimes', 'Transversal', 'Surveillances', 'Surveillance générale maritime'
    UNION ALL SELECT 'MEETING', '', 'Réunion', 'Contrôle des activités maritimes', 'Transversal', 'Vie et gestion de l''unité', 'Réunion'
    UNION ALL SELECT 'NAUTICAL_EVENT', '', 'Surveillance de manifestation nautique', 'Contrôle des activités maritimes', 'Occupation du domaine public maritime', 'Surveillances', 'Surveillance de manifestation nautique'
    UNION ALL SELECT 'NOTE', '', 'Note libre', 'Contrôle des activités maritimes', 'Transversal', 'Vie et gestion de l''unité', 'Note libre'
    UNION ALL SELECT 'OTHER', '', 'Autre (vie et gestion de l''unité)', 'Contrôle des activités maritimes', 'Transversal', 'Vie et gestion de l''unité', 'Autre'
    UNION ALL SELECT 'PUBLIC_ORDER', '', 'Ordre public', 'Maintien de l''ordre public', '', 'Autre activité terrain', 'Sûreté maritime'
    UNION ALL SELECT 'PV_DRAFTING', '', 'Rédaction de PV', 'Contrôle des activités maritimes', 'Transversal', 'Préparation et suivi des ctrl', 'Rédaction de PV'
    UNION ALL SELECT 'REPRESENTATION', '', 'Représentation', 'Contrôle des activités maritimes', 'Transversal', 'Autre activité terrain', 'Représentation'
    UNION ALL SELECT 'RESCUE', '', 'Assistance/ sauvetage', 'Assistance/ sauvetage', 'Assistance/ sauvetage', 'Assistances/sauvetages', 'Assistance/sauvetage'
    UNION ALL SELECT 'RESOURCES_MAINTENANCE', '', 'Entretien des moyens', 'Contrôle des activités maritimes', 'Transversal', 'Vie et gestion de l''unité', 'Entretien des moyens'
    UNION ALL SELECT 'SECURITY_VISIT', '', 'Visite sécurité', 'Contrôle des activités maritimes', 'Transversal', 'Autre activité terrain', 'Visite de sécurité'
    UNION ALL SELECT 'TRAINING', '', 'Entraînement', 'Contrôle des activités maritimes', 'Transversal', 'Formations', 'Entraînement'
    UNION ALL SELECT 'UNIT_MANAGEMENT_OTHER', '', 'Gestion de l''unité - autres', 'Contrôle des activités maritimes', 'Transversal', 'Vie et gestion de l''unité', 'Gestion - autres'
    UNION ALL SELECT 'UNIT_MANAGEMENT_PLANNING', '', 'Gestion de l''unité - planning', 'Contrôle des activités maritimes', 'Transversal', 'Vie et gestion de l''unité', 'Gestion - planning'
    UNION ALL SELECT 'UNIT_MANAGEMENT_TRAINING', '', 'Entraînement', 'Contrôle des activités maritimes', 'Transversal', 'Formations', 'Entraînement unité'
    UNION ALL SELECT 'UNIT_MANAGEMENT_TRAINING', 'DIVING', 'Entraînement', 'Contrôle des activités maritimes', 'Transversal', 'Formations', 'Entraînement unité'
    UNION ALL SELECT 'UNIT_MANAGEMENT_TRAINING', 'MAN_OVERBOARD_RECOVERY', 'Formation', 'Assistance/ sauvetage', 'Assistance/ sauvetage', 'Formations', 'Entraînement unité'
    UNION ALL SELECT 'UNIT_MANAGEMENT_TRAINING', 'TECHNICAL_INTERVENTION_SHOOTING', 'Formation', 'Maintien de l''ordre public', 'Gestes Techniques Professionnels d''Intervention', 'Formations', 'Entraînement unité'
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
        -- action_type unifié : CONTROL absorbe les 4 anciens action_type de
        -- la famille contrôle. SECURITY_VISIT n'est pas absorbé.
        toString(multiIf(
            ma.action_type IN ('CONTROL', 'CONTROL_NAUTICAL_LEISURE', 'CONTROL_SLEEPING_FISHING_GEAR', 'CONTROL_SECTOR', 'OTHER_CONTROL'), 'CONTROL',
            toString(ma.action_type)
        )) AS action_type,
        -- action_subtype (niveau 2) : pour CONTROL, dérivé de l'ancien
        -- action_type. Pour le reste, fusionne aussi security_visit_type.
        toString(multiIf(
            ma.action_type = 'CONTROL_NAUTICAL_LEISURE', 'NAUTICAL_LEISURE',
            ma.action_type = 'CONTROL_SLEEPING_FISHING_GEAR', 'SLEEPING_FISHING_GEAR',
            ma.action_type = 'CONTROL_SECTOR', 'SECTOR',
            ma.action_type = 'OTHER_CONTROL', 'OTHER_CONTROL',
            -- Contrôle navire "générique" (raw action_type='CONTROL',
            -- vessel_type renseigné -- COMMERCIAL/FISHING/MOTOR/SAILING/...)
            -- -- sous-type dédié SHIP, même mécanisme que NAUTICAL_LEISURE/
            -- SECTOR/SLEEPING_FISHING_GEAR ci-dessus.
            ma.action_type = 'CONTROL' AND nullIf(ma.vessel_type, '') IS NOT NULL, 'SHIP',
            ma.action_type = 'TRAINING', coalesce(ma.training_type, ''),
            ma.action_type = 'UNIT_MANAGEMENT_TRAINING', coalesce(ma.unit_management_training_type, ''),
            ma.action_type = 'RESOURCES_MAINTENANCE', coalesce(ma.resource_type, ''),
            ma.action_type = 'SECURITY_VISIT', coalesce(ma.security_visit_type, ''),
            coalesce(ma.reason, '')
        )) AS action_subtype,
        -- action_subsubtype (niveau 3) : remplace control_type/vessel_type/
        -- vessel_size/leisure_type/fishing_gear_type (jamais 2 renseignées
        -- à la fois) par un seul coalesce. Significatif seulement pour la
        -- famille CONTROL. Exception : sector_type et
        -- sector_establishment_type sont RENSEIGNÉS ENSEMBLE pour
        -- CONTROL_SECTOR (sector_type = filière -- pêche/plaisance --,
        -- sector_establishment_type = type d'établissement précis dans
        -- cette filière) -- concaténés plutôt que coalescés pour ne pas en
        -- perdre un des deux.
        toString(coalesce(
            nullIf(ma.vessel_type, ''),
            nullIf(ma.leisure_type, ''),
            nullIf(ma.fishing_gear_type, ''),
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
        toString(coalesce(nullIf(atm.libelle_francais, ''), toString(ma.action_type))) AS libelle_francais,
        toString(coalesce(atm.politique_publique, '')) AS politique_publique,
        toString(coalesce(atm.thematique, '')) AS thematique,
        toString(coalesce(atm.categorie_activite, '')) AS categorie_activite,
        toString(coalesce(atm.sous_categorie_activite, '')) AS sous_categorie_activite,
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
        coalesce(ar.terrain_types, []) AS terrain_types,
        ma.latitude AS latitude,
        ma.longitude AS longitude
    FROM rapportnav_proxy.mission_action ma
    -- INNER JOIN (pas LEFT) : filtre aux actions dont la mission a au
    -- moins une unité PAM ou ULAM.
    INNER JOIN mission_unit_pairs mup ON mup.mission_id = ma.mission_id
    LEFT JOIN action_resources ar ON ar.action_id = toString(ma.id)
    LEFT JOIN action_targets atg ON atg.action_id = toString(ma.id)
    LEFT JOIN action_controls acl ON acl.action_id = toString(ma.id)
    -- action_type déjà unifié (CONTROL) : atm.action_type = 'CONTROL'
    -- matche toute la famille. action_subtype_key ne différencie que
    -- CONTROL et UNIT_MANAGEMENT_TRAINING.
    LEFT JOIN action_type_mapping atm
        ON atm.action_type = toString(multiIf(
            ma.action_type IN ('CONTROL', 'CONTROL_NAUTICAL_LEISURE', 'CONTROL_SLEEPING_FISHING_GEAR', 'CONTROL_SECTOR', 'OTHER_CONTROL'), 'CONTROL',
            toString(ma.action_type)
        ))
        AND atm.action_subtype_key = multiIf(
            ma.action_type = 'CONTROL_NAUTICAL_LEISURE', 'NAUTICAL_LEISURE',
            ma.action_type = 'CONTROL_SLEEPING_FISHING_GEAR', 'SLEEPING_FISHING_GEAR',
            ma.action_type = 'CONTROL_SECTOR', 'SECTOR',
            ma.action_type = 'OTHER_CONTROL', 'OTHER_CONTROL',
            ma.action_type = 'CONTROL' AND nullIf(ma.vessel_type, '') IS NOT NULL, 'SHIP',
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
        -- Même hiérarchie que NAV : SEA_CONTROL/LAND_CONTROL/AIR_CONTROL/
        -- AIR_SURVEILLANCE -> action_type='CONTROL', action_subtype='FISH',
        -- action_subsubtype=méthode de contrôle réelle. OBSERVATION reste à
        -- part (pas un contrôle).
        toString(multiIf(f.control_type = 'OBSERVATION', 'OBSERVATION', 'CONTROL')) AS action_type,
        toString(multiIf(f.control_type = 'OBSERVATION', '', 'FISH')) AS action_subtype,
        toString(multiIf(f.control_type = 'OBSERVATION', '', toString(f.control_type))) AS action_subsubtype,
        toString(multiIf(f.control_type = 'OBSERVATION', 'Observation', 'Contrôle de pêche')) AS libelle_francais,
        -- politique_publique fixe (pas de classification interne côté
        -- MonitorFish) ; thematique = segment de flotte si disponible.
        'Pêches maritimes' AS politique_publique,
        toString(coalesce(nullIf(f.segment, ''), 'Pêches maritimes')) AS thematique,
        toString(multiIf(f.control_type = 'OBSERVATION', 'Autre activité terrain', 'Contrôles')) AS categorie_activite,
        toString(multiIf(f.control_type = 'OBSERVATION', 'Observation', 'Contrôle navires (pêche)')) AS sous_categorie_activite,
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
        -- Pas d'équivalent réel côté FISH (completedBy existe sur
        -- mission_actions mais n'est pas exposé par
        -- analytics_controls_full_data). 1 par défaut : la source filtre
        -- déjà "non supprimé", pas "complet" au sens nav.
        toUInt8(1) AS is_complete_for_stats,
        toNullable(toInt32(0)) AS nbr_of_hours_declared,
        toUInt16(0) AS nb_resources_linked,
        CAST([], 'Array(Int32)') AS resource_ids,
        CAST([], 'Array(String)') AS resource_types,
        CAST(['MER'], 'Array(String)') AS terrain_types,
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
        toString(a.theme_level_2) AS action_subtype,
        '' AS action_subsubtype,
        toString(a.theme_level_1) AS libelle_francais,
        -- En attente de la liste de valeurs de theme_level_1 pour mapper
        -- politique_publique/thematique -- laissées vides plutôt que
        -- devinées. Bruts exposés via env_theme_level_1/2 ci-dessous.
        '' AS politique_publique,
        '' AS thematique,
        'Contrôles' AS categorie_activite,
        'Contrôle environnement' AS sous_categorie_activite,
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
        -- 1 justifié : la requête source de analytics_actions filtre déjà
        -- completion='COMPLETED'.
        toUInt8(1) AS is_complete_for_stats,
        toNullable(toInt32(0)) AS nbr_of_hours_declared,
        toUInt16(0) AS nb_resources_linked,
        CAST([], 'Array(Int32)') AS resource_ids,
        CAST([], 'Array(String)') AS resource_types,
        CAST(['MER'], 'Array(String)') AS terrain_types,
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
