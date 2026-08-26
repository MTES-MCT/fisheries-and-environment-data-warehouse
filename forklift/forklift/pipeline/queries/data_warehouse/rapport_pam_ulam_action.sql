-- =====================================================================
-- Alimente rapportnav.fact_action_pam_ulam.
-- Grain : 1 ligne par action/contrôle individuel × unité individuelle,
-- toutes sources confondues (nav + fish + env, colonne `source`) --
-- beaucoup de colonnes vides selon la source. Option non retenue :
-- séparer CONTROLE / AUTRES ACTIONS pour minimiser les colonnes vides.
--
-- Pourquoi 3 sources : mission_action (nav) ne contient que les actions
-- saisies dans RapportNav. FISH n'a pas de notion d'activité hors-
-- contrôle -- il n'apparaît donc que pour des contrôles. ENV EN A UNE
-- (SURVEILLANCE, cf. plus bas) -- les deux apparaissent via les tables
-- déjà construites (pas créées pour cette table) :
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
--     MonitorFish). analytics_controls_full_data expose désormais un
--     comptage exact par InfractionType (WITH_RECORD/WITHOUT_RECORD/
--     PENDING, colonnes infraction_count_* ajoutées à
--     monitorfish_remote/analytics_controls_full_data.sql) -- même
--     fiabilité que NAV/ENV désormais (c'était une approximation 0/1
--     avant ; nb_infractions_sans_pv_fiable, qui marquait cet écart,
--     a été retiré une fois les 3 sources alignées -- devenu toujours
--     à 1, donc sans valeur informative, cf. historique Git).
--   - ENV : infraction_type a 3 valeurs fiables (WAITING/WITH_REPORT/
--     WITHOUT_REPORT). analytics_actions couvre CONTROL ET SURVEILLANCE
--     (déjà filtré ainsi par la requête source monitorenv_remote/
--     analytics_actions.sql) -- surveillance_duration alimente duration_h
--     pour les lignes SURVEILLANCE.
--   - politique_publique fixe pour FISH ("Pêche professionnelle") et ENV
--     ("Environnement / pollution") -- confirmé sur les maquettes
--     Metabase ULAM et PAM (table "politique publique", 7 catégories
--     identiques sur les deux dashboards -- Administratif distinct
--     d'Autres, cf. commentaire sur ADMINISTRATIVE plus bas). thematique FISH = segment de
--     flotte si disponible ; thematique ENV reste en attente de la liste
--     de valeurs de theme_level_1 (partiellement connue, cf. plus bas) --
--     ne pas deviner les libellés. Bruts exposés en attendant via
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
-- Politique publique des contrôles NAV -- confirmée sur les maquettes
-- Metabase (table "Nombre d'actions de contrôle par politique publique",
-- identique sur les dashboards ULAM et PAM) : Pêche professionnelle /
-- Equipement de sécurité / Police de la navigation / Gens de mer /
-- Administratif / Environnement-pollution / Autres. Les 5 dernières
-- viennent de control_2.control_type (ADMINISTRATIVE/GENS_DE_MER/
-- NAVIGATION/SECURITY -- champ DIFFÉRENT de mission_action.control_type,
-- qui lui est du texte libre uniquement pour OTHER_CONTROL) --
-- ADMINISTRATIVE -> "Administratif", distinct d'"Autres" (confirmé :
-- politique propre, "administrative", dans ComputeControlPolicies.kt
-- côté rapportnav2 -- PAS la même chose que la politique "other").
-- Pêche professionnelle et Environnement/pollution viennent des sources
-- FISH/ENV (fixes, cf. fish_rows/env_rows plus bas) -- control_2 est
-- nav-only.
-- ⚠️ Une action peut avoir plusieurs contrôles ; si leurs control_type
-- diffèrent (rare), on garde le plus fréquent (topK) plutôt que d'en
-- perdre un silencieusement.
action_control_policy AS (
    SELECT
        toString(t.action_id) AS action_id,
        arrayElement(topK(1)(toString(c.control_type)), 1) AS control_type_predominant
    FROM rapportnav_proxy.control_2 c
    INNER JOIN rapportnav_proxy.target_2 t ON t.id = c.target_id
    WHERE coalesce(c.has_been_done, false) = true
    GROUP BY t.action_id
),
-- Durée des actions STATUS -- pas de end_datetime_utc propre, la "fin"
-- réelle est le début du prochain STATUS de la même mission (même
-- logique leadInFrame que heures_de_mer dans rapport_pam_ulam_mission.sql,
-- dupliquée ici au niveau action plutôt que mission). STATUS n'est plus
-- exclu de cette table (cf. action_type='STATUS' dans nav_rows) : couvre
-- la maquette PAM "Activité du navire et de l'unité" (Navigation/
-- Mouillage/Présence à quai/Indisponibilité) via action_subtype/
-- action_subsubtype plutôt que des colonnes dédiées sur
-- fact_mission_pam_ulam.
status_action_durations AS (
    SELECT
        ma.id AS action_id,
        dateDiff('second', ma.start_datetime_utc, leadInFrame(
            ma.start_datetime_utc,
            1,
            ifNull(envm.end_datetime_utc, ma.start_datetime_utc)
        ) OVER (
            PARTITION BY ma.mission_id ORDER BY ma.start_datetime_utc
        )) / 3600.0 AS duration_h
    FROM rapportnav_proxy.mission_action ma
    INNER JOIN monitorenv_proxy.missions envm ON envm.id = ma.mission_id
    WHERE ma.action_type = 'STATUS'
),
-- Chronologie des statuts navire par mission (NAVIGATING/ANCHORED/DOCKED/
-- UNAVAILABLE), utilisée pour enrichir CHAQUE action NAV (contrôles ET
-- reste) du statut du navire au moment où elle a eu lieu -- via ASOF JOIN
-- plus bas (le statut le plus récent dont le début est <= au début de
-- l'action). Utile pour croiser n'importe quelle activité avec le statut
-- du navire (ex : "contrôles réalisés à quai"), pas seulement les
-- contrôles.
status_timeline AS (
    SELECT
        mission_id,
        start_datetime_utc,
        status
    FROM rapportnav_proxy.mission_action
    WHERE action_type = 'STATUS'
),
-- "Focus BAAEM -- nb assistance/sauvetage dans le cadre d'une opération
-- BAAEM" (maquette PAM) : BAAEM_PERMANENCE est une action à intervalle
-- réel (start ET end renseignés, vérifié contre
-- mission-action-item-baaem-performance.tsx / dates-schema.ts côté
-- rapportnav2 -- formulaire "plage de dates", pas un simple horodatage)
-- -- même principe que le statut navire plus haut : interpoler cet
-- intervalle sur les AUTRES actions de la même mission pour savoir
-- lesquelles tombent "pendant une permanence BAAEM". Tableau par mission
-- (pas ASOF JOIN, qui ne gère qu'un événement ponctuel) : une mission
-- peut avoir plusieurs permanences, arrayExists vérifie l'appartenance à
-- N'IMPORTE LAQUELLE. ⚠️ Concept a priori PAM uniquement (BAAEM = Bureau
-- de l'Action de l'État en Mer, patrouilleurs) -- présent ici pour
-- couvrir le cas où une unité ULAM y participerait, mais pas confirmé
-- comme un scénario réel.
baaem_permanence_by_mission AS (
    SELECT
        mission_id,
        groupArray(start_datetime_utc) AS permanence_starts,
        groupArray(end_datetime_utc) AS permanence_ends
    FROM rapportnav_proxy.mission_action
    WHERE action_type = 'BAAEM_PERMANENCE' AND end_datetime_utc IS NOT NULL
    GROUP BY mission_id
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
    -- STATUS (rapportnav2 ActionStatusType) : libellés vérifiés contre
    -- mapActionStatusTypeToHumanString (rapportnav2). categorie_activite/
    -- politique_publique/thematique volontairement vides -- STATUS n'est
    -- pas une "activité" au sens de la taxonomie 8 catégories, cf.
    -- avertissement dans nav_rows.
    UNION ALL SELECT 'STATUS', 'NAVIGATING', 'Navigation', '', '', '', ''
    UNION ALL SELECT 'STATUS', 'ANCHORED', 'Mouillage', '', '', '', ''
    UNION ALL SELECT 'STATUS', 'DOCKED', 'Présence à quai', '', '', '', ''
    UNION ALL SELECT 'STATUS', 'UNAVAILABLE', 'Indisponibilité', '', '', '', ''
    -- 4 action_type trouvés dans l'enum ActionType.kt (rapportnav2) mais
    -- absents du dictionnaire "Types et sous-types d'actions" (export CSV
    -- du 2026-08-14) utilisé pour construire ce référentiel -- sans
    -- mapping, ces 4 tombaient silencieusement en repli vide
    -- (libelle_francais=action_type brut, politique_publique/thematique/
    -- categorie_activite vides). Libellés français ET politique_publique/
    -- thematique NON confirmés métier, à valider :
    --   - FISHING_SURVEILLANCE : probablement "surveillance pêche" (cf.
    --     maquette "Surveillance pêche - encadrée CNSP / libre") -- pas de
    --     champ trouvé pour distinguer encadrée/libre à ce stade.
    --   - VIGIMER : dispositif de vigilance/sûreté maritime, groupé avec
    --     BAAEM_PERMANENCE/NAUTICAL_EVENT dans AEMSeaSafety.kt.
    --   - CONDUCT_HEARING : coexiste avec HEARING_CONDUCT (déjà mappé
    --     ci-dessus) dans l'enum ET dans ValidationPolicies.kt -- 2 entrées
    --     distinctes actives, pas un doublon/typo. Mappé identique à
    --     HEARING_CONDUCT faute de mieux comprendre la différence.
    --   - SURVEILLANCE : valeur générique, coexiste avec LAND_SURVEILLANCE/
    --     MARITIME_SURVEILLANCE/FISHING_SURVEILLANCE (surveillances plus
    --     spécifiques) -- rôle exact non clarifié.
    UNION ALL SELECT 'FISHING_SURVEILLANCE', '', 'Surveillance pêche', 'Pêches maritimes', 'Pêches maritimes', 'Surveillances', 'Surveillance pêche'
    UNION ALL SELECT 'VIGIMER', '', 'VIGIMER', 'Maintien de l''ordre public', 'Transversal', 'Autre activité terrain', 'VIGIMER'
    UNION ALL SELECT 'CONDUCT_HEARING', '', 'Préparation et conduite d''audition', 'Contrôle des activités maritimes', 'Transversal', 'Préparation et suivi des ctrl', 'Préparation et conduite d''audition'
    UNION ALL SELECT 'SURVEILLANCE', '', 'Surveillance', 'Contrôle des activités maritimes', 'Transversal', 'Surveillances', 'Surveillance générale'
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
        -- STATUS n'a pas de end_datetime_utc propre -- durée reconstituée
        -- via status_action_durations (leadInFrame sur le prochain STATUS
        -- de la mission), cf. commentaire sur cette CTE plus haut.
        toFloat64(multiIf(
            ma.action_type = 'STATUS', coalesce(sad.duration_h, 0),
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
            -- STATUS (rapportnav2 ActionStatusType, vérifié) :
            -- NAVIGATING/ANCHORED/DOCKED/UNAVAILABLE -- couvre la maquette
            -- PAM "Activité du navire" (Navigation/Mouillage/Présence à
            -- quai/Indisponibilité).
            ma.action_type = 'STATUS', coalesce(ma.status, ''),
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
        -- ma.reason ne rejoint le coalesce que pour STATUS (raison
        -- DOCKED/UNAVAILABLE -- Météo/Maintenance/.../Technique/
        -- Personnel, cf. ActionStatusReason.kt) -- gardé conditionnel
        -- pour ne pas dupliquer la valeur déjà utilisée comme
        -- action_subtype (fallback coalesce(ma.reason,'')) sur les autres
        -- action_type.
        toString(coalesce(
            if(ma.action_type = 'STATUS', nullIf(ma.reason, ''), NULL),
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
        -- politique_publique : control_2.control_type prime sur
        -- action_type_mapping pour les actions ayant un contrôle logué
        -- (cf. action_control_policy) -- repli sur action_type_mapping
        -- (classification générique par action_type) sinon.
        toString(coalesce(
            nullIf(multiIf(
                acp.control_type_predominant = 'NAVIGATION', 'Police de la navigation',
                acp.control_type_predominant = 'GENS_DE_MER', 'Gens de mer',
                acp.control_type_predominant = 'SECURITY', 'Equipement de sécurité',
                -- ADMINISTRATIVE est une politique distincte de la
                -- catégorie fourre-tout "Autres" (confirmé : c'est un
                -- champ séparé, "administrative", dans
                -- ComputeControlPolicies.kt côté rapportnav2, pas la même
                -- chose que "other").
                acp.control_type_predominant = 'ADMINISTRATIVE', 'Administratif',
                ''
            ), ''),
            atm.politique_publique,
            ''
        )) AS politique_publique,
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
        ma.longitude AS longitude,
        -- Statut du navire au moment de CETTE action (toute action, pas
        -- seulement les contrôles) -- cf. status_timeline plus haut.
        -- Vide si l'action a lieu avant le tout premier STATUS de la
        -- mission (aucun statut connu à cet instant).
        toString(coalesce(st.status, '')) AS statut_navire,
        -- Cette action tombe-t-elle pendant une permanence BAAEM de la
        -- même mission (cf. baaem_permanence_by_mission plus haut) --
        -- permet ensuite un simple count(*) WHERE action_type='RESCUE'
        -- AND is_during_baaem_permanence=1 pour "Focus BAAEM -- nb
        -- assistance/sauvetage dans le cadre d'une opération BAAEM".
        toUInt8(arrayExists(
            (s, e) -> ma.start_datetime_utc >= s AND ma.start_datetime_utc <= e,
            coalesce(bpm.permanence_starts, []),
            coalesce(bpm.permanence_ends, [])
        )) AS is_during_baaem_permanence
    FROM rapportnav_proxy.mission_action ma
    -- INNER JOIN (pas LEFT) : filtre aux actions dont la mission a au
    -- moins une unité PAM ou ULAM.
    INNER JOIN mission_unit_pairs mup ON mup.mission_id = ma.mission_id
    LEFT JOIN action_resources ar ON ar.action_id = toString(ma.id)
    LEFT JOIN action_targets atg ON atg.action_id = toString(ma.id)
    LEFT JOIN action_controls acl ON acl.action_id = toString(ma.id)
    LEFT JOIN action_control_policy acp ON acp.action_id = toString(ma.id)
    LEFT JOIN status_action_durations sad ON sad.action_id = ma.id
    -- ASOF : pour chaque action, le STATUS le plus récent démarré à ou
    -- avant le début de l'action (dans la même mission).
    ASOF LEFT JOIN status_timeline st ON st.mission_id = ma.mission_id AND st.start_datetime_utc <= ma.start_datetime_utc
    LEFT JOIN baaem_permanence_by_mission bpm ON bpm.mission_id = ma.mission_id
    -- action_type déjà unifié (CONTROL) : atm.action_type = 'CONTROL'
    -- matche toute la famille. action_subtype_key ne différencie que
    -- CONTROL, UNIT_MANAGEMENT_TRAINING et STATUS.
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
            ma.action_type = 'STATUS', coalesce(ma.status, ''),
            ''
        )
    -- STATUS désormais inclus (cf. action_type='STATUS' ci-dessus) --
    -- couvre "Activité du navire" côté PAM. Reste exclu de la taxonomie 8
    -- catégories (categorie_activite/sous_categorie_activite vides pour
    -- STATUS dans action_type_mapping, cf. plus haut) pour ne pas
    -- gonfler artificiellement les sommes "Répartition des activités 8
    -- catégories" avec des heures de mouillage/à quai/indisponibilité.
    WHERE ma.start_datetime_utc >= toDateTime('2025-01-01 00:00:00')
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
        -- ⚠️ CORRIGÉ (repéré en revue) : AIR_SURVEILLANCE était traité comme
        -- un CONTROL au même titre que SEA_CONTROL/LAND_CONTROL/AIR_CONTROL
        -- -- faux. Vérifié contre MissionActionType.kt/FishActionCard.tsx
        -- (repo monitorfish cloné) : AIR_SURVEILLANCE est un vol de
        -- reconnaissance sur plusieurs pistes ("N pistes survolées"), sans
        -- navire ciblé -- l'app elle-même l'exclut de isControlAction et de
        -- numberOfControls (les regroupe avec OBSERVATION comme "pas un
        -- contrôle"). SEA_CONTROL/LAND_CONTROL/AIR_CONTROL restent seuls
        -- CONTROL ; action_subtype='FISH', action_subsubtype=méthode de
        -- contrôle réelle. AIR_SURVEILLANCE -> action_type='SURVEILLANCE'
        -- (même famille que le SURVEILLANCE ENV plus bas). OBSERVATION
        -- reste à part.
        toString(multiIf(
            f.control_type = 'OBSERVATION', 'OBSERVATION',
            f.control_type = 'AIR_SURVEILLANCE', 'SURVEILLANCE',
            'CONTROL'
        )) AS action_type,
        toString(multiIf(f.control_type = 'OBSERVATION', '', 'FISH')) AS action_subtype,
        toString(multiIf(f.control_type = 'OBSERVATION', '', toString(f.control_type))) AS action_subsubtype,
        toString(multiIf(
            f.control_type = 'OBSERVATION', 'Observation',
            f.control_type = 'AIR_SURVEILLANCE', 'Surveillance aérienne',
            'Contrôle de pêche'
        )) AS libelle_francais,
        -- politique_publique fixe (pas de classification interne côté
        -- MonitorFish) -- libellé exact confirmé sur les maquettes
        -- Metabase ULAM et PAM ("Pêche professionnelle", cf.
        -- action_control_policy plus haut) ; thematique = segment de
        -- flotte si disponible.
        'Pêche professionnelle' AS politique_publique,
        toString(coalesce(nullIf(f.segment, ''), 'Pêches maritimes')) AS thematique,
        toString(multiIf(
            f.control_type = 'OBSERVATION', 'Autre activité terrain',
            f.control_type = 'AIR_SURVEILLANCE', 'Surveillances',
            'Contrôles'
        )) AS categorie_activite,
        toString(multiIf(
            f.control_type = 'OBSERVATION', 'Observation',
            f.control_type = 'AIR_SURVEILLANCE', 'Surveillance pêche (aérienne)',
            'Contrôle navires (pêche)'
        )) AS sous_categorie_activite,
        '' AS env_theme_level_1,
        '' AS env_theme_level_2,
        '' AS env_plan,
        -- nb_targets/nb_control_types/nb_controls : 0 pour OBSERVATION et
        -- AIR_SURVEILLANCE (ni l'un ni l'autre n'est un contrôle -- même
        -- correction que ci-dessus, ces 3 champs étaient à 1 pour TOUTES
        -- les lignes FISH sans condition, gonflant nb_controls sur les
        -- observations/surveillances aériennes).
        toUInt16(if(f.control_type IN ('OBSERVATION', 'AIR_SURVEILLANCE'), 0, 1)) AS nb_targets,
        toUInt16(if(f.control_type IN ('OBSERVATION', 'AIR_SURVEILLANCE'), 0, 1)) AS nb_control_types,
        toUInt16(if(f.control_type IN ('OBSERVATION', 'AIR_SURVEILLANCE'), 0, 1)) AS nb_controls,
        -- Comptage exact désormais disponible : infraction_count_* ajoutés
        -- à la CTE controls_infraction_natinfs_array de
        -- monitorfish_remote/analytics_controls_full_data.sql -- un
        -- COUNT(*) réel par valeur d'InfractionType (monitorfish, 3
        -- valeurs : WITH_RECORD/WITHOUT_RECORD/PENDING), remplace
        -- l'ancienne approximation 0/1 déduite de infraction/
        -- infraction_report qui perdait le vrai nombre d'infractions par
        -- contrôle. PENDING correspond au même concept que WAITING côté
        -- ENV -> nb_infractions_en_attente. Fiable désormais, comme pour
        -- NAV/ENV.
        toUInt16(f.infraction_count_with_record) AS nb_infractions_avec_pv,
        toUInt16(f.infraction_count_without_record) AS nb_infractions_sans_pv,
        toUInt16(f.infraction_count_pending) AS nb_infractions_en_attente,
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
        -- ⚠️ CORRIGÉ (même revue) : était hardcodé ['MER'] pour toutes les
        -- lignes FISH, y compris AIR_CONTROL/AIR_SURVEILLANCE -- faux pour
        -- un contrôle/surveillance aérien. Dérivé de f.control_type comme
        -- terrain_control sur fact_cible_pam_ulam (même mapping déjà
        -- vérifié là-bas).
        arrayFilter(x -> x != '', [multiIf(
            f.control_type = 'SEA_CONTROL', 'MER',
            f.control_type = 'LAND_CONTROL', 'TERRE',
            f.control_type IN ('AIR_CONTROL', 'AIR_SURVEILLANCE'), 'AIR',
            ''
        )]) AS terrain_types,
        f.latitude AS latitude,
        f.longitude AS longitude,
        -- Pas de notion de statut navire côté MonitorFish.
        '' AS statut_navire,
        -- Pas de notion de permanence BAAEM côté MonitorFish (concept
        -- RapportNav uniquement, cf. baaem_permanence_by_mission).
        toUInt8(0) AS is_during_baaem_permanence
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
        -- a.surveillance_duration : calculée uniquement pour action_type=
        -- 'SURVEILLANCE' par la requête source (monitorenv_remote/
        -- analytics_actions.sql) -- NULL pour CONTROL, comme côté FISH.
        toFloat64(coalesce(a.surveillance_duration, 0)) AS duration_h,
        toString(a.action_type) AS action_type,
        toString(a.theme_level_2) AS action_subtype,
        '' AS action_subsubtype,
        toString(a.theme_level_1) AS libelle_francais,
        -- politique_publique fixe -- libellé exact confirmé sur les
        -- maquettes ULAM et PAM ("Environnement / pollution", cf.
        -- action_control_policy plus haut). thematique reste en attente
        -- de la liste de valeurs de theme_level_1 (12 valeurs
        -- partiellement connues via une maquette PAM, certaines
        -- tronquées -- pas encore confirmées) -- laissée vide plutôt que
        -- devinée. Bruts exposés via env_theme_level_1/2 ci-dessous.
        'Environnement / pollution' AS politique_publique,
        '' AS thematique,
        -- ⚠️ analytics_actions.sql (requête source, déjà en place) filtre
        -- action_type IN ('CONTROL', 'SURVEILLANCE') -- les surveillances
        -- ENV étaient donc déjà présentes dans la table déjà construite,
        -- mais exclues ici par un WHERE trop restrictif (corrigé plus
        -- bas). categorie_activite distingue maintenant les deux.
        toString(multiIf(a.action_type = 'SURVEILLANCE', 'Surveillances', 'Contrôles')) AS categorie_activite,
        toString(multiIf(a.action_type = 'SURVEILLANCE', 'Surveillance environnement', 'Contrôle environnement')) AS sous_categorie_activite,
        toString(a.theme_level_1) AS env_theme_level_1,
        toString(a.theme_level_2) AS env_theme_level_2,
        toString(a.plan) AS env_plan,
        -- nb_targets/nb_control_types n'ont de sens que pour un contrôle
        -- (1 cible/1 sous-type par ligne, cf. fish_rows) -- 0 pour une
        -- surveillance.
        toUInt16(if(a.action_type = 'CONTROL', 1, 0)) AS nb_targets,
        toUInt16(if(a.action_type = 'CONTROL', 1, 0)) AS nb_control_types,
        toUInt16(coalesce(a.number_of_controls, 0)) AS nb_controls,
        toUInt16(coalesce(ei.nb_infractions_avec_pv, 0)) AS nb_infractions_avec_pv,
        toUInt16(coalesce(ei.nb_infractions_sans_pv, 0)) AS nb_infractions_sans_pv,
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
        a.longitude AS longitude,
        -- Pas de notion de statut navire côté MonitorEnv.
        '' AS statut_navire,
        -- Pas de notion de permanence BAAEM côté MonitorEnv (concept
        -- RapportNav uniquement, cf. baaem_permanence_by_mission).
        toUInt8(0) AS is_during_baaem_permanence
    FROM monitorenv.analytics_actions a
    LEFT JOIN env_infractions_by_action ei ON ei.env_action_id = a.id
    WHERE a.action_type IN ('CONTROL', 'SURVEILLANCE')
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
