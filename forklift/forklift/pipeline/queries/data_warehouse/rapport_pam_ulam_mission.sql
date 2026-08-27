-- =====================================================================
-- Alimente rapportnav.fact_mission_pam_ulam.
-- Couvre les unités PAM ET ULAM dans une seule table -- unit_types
-- distingue les deux.
-- SELECT pur : le flow générique fait CREATE TABLE ... AS <cette requête>
-- (ddl_script_path laissé vide -> schéma inféré).
-- Doit tourner après dim_unit_reference.sql dans
-- sync_table_from_db_connection.csv (aucune dépendance native entre
-- lignes de ce flow, cf. dim_unit_reference.sql).
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
-- INNER JOIN sur pam_ulam_control_units : filtre aux missions ayant au
-- moins une unité PAM ou ULAM. Une mission conjointe PAM+ULAM expose les
-- deux unités dans unit_names (cf. unit_types ci-dessous).
mission_units AS (
    SELECT
        mcu.mission_id,
        arrayStringConcat(groupArray(cu.name), ', ') AS unit_names,
        COUNT(DISTINCT cu.id) AS nb_unites_distinctes,
        groupArray(cu.id) AS control_unit_ids,
        -- Liste complète (dédupliquée) des façades/types d'unité de la
        -- mission -- une mission conjointe peut mobiliser des unités de
        -- façades ou types différents (ex : PAM+ULAM), donc pas de
        -- réduction à la 1ère trouvée.
        groupUniqArray(uu.facade_ref) AS facades,
        groupUniqArray(uu.unit_type) AS unit_types
    FROM monitorenv_proxy.missions_control_units mcu
    INNER JOIN monitorenv_proxy.control_units cu ON cu.id = mcu.control_unit_id
    INNER JOIN pam_ulam_control_units uu ON uu.control_unit_id = cu.id
    GROUP BY mcu.mission_id
),

-- Heures de mer recalculées (méthode déjà validée sur le rapport AEM,
-- cf. status_actions / heures_de_mer_nav dans
-- query_aem_par_mission_3_bases_clickhouse.sql) -- à préférer à
-- mission_general_info.nb_hour_at_sea qui est une saisie manuelle agent.
status_actions AS (
    SELECT
        ma.mission_id,
        ma.status,
        ma.start_datetime_utc,
        leadInFrame(
            ma.start_datetime_utc,
            1,
            ifNull(envm.end_datetime_utc, ma.start_datetime_utc)
        ) OVER (
            PARTITION BY ma.mission_id ORDER BY ma.start_datetime_utc
        ) AS corrected_end_datetime_utc
    FROM rapportnav_proxy.mission_action ma
    INNER JOIN monitorenv_proxy.missions envm ON envm.id = ma.mission_id
    WHERE ma.action_type = 'STATUS'
),
heures_de_mer AS (
    SELECT
        mission_id,
        sumIf(dateDiff('second', start_datetime_utc, corrected_end_datetime_utc) / 3600.0,
              status IN ('ANCHORED', 'NAVIGATING')) AS computed_hours_at_sea
    FROM status_actions
    GROUP BY mission_id
),
-- "Jour de mer" : vérifié contre GetNbOfDaysAtSeaFromNavigationStatus.kt
-- (rapportnav2) -- un jour calendaire compte comme "jour de mer" dès que
-- le cumul ANCHORED+NAVIGATING ce jour-là dépasse 4h (seuil strict,
-- >4h). Chaque intervalle STATUS est éclaté par jour calendaire
-- (ARRAY JOIN) pour gérer les statuts à cheval sur minuit, comme le fait
-- le code source. ⚠️ Le jour calendaire est calculé dans le fuseau du
-- serveur ClickHouse (UTC), pas explicitement Europe/Paris comme le code
-- rapportnav2 (zoneId=systemDefault()) -- écart possible d'un jour en
-- cas de statut à cheval sur minuit heure de Paris mais pas minuit UTC.
status_day_overlap AS (
    SELECT
        mission_id,
        jour,
        dateDiff('second',
            greatest(start_datetime_utc, toDateTime(jour)),
            least(corrected_end_datetime_utc, toDateTime(jour) + INTERVAL 1 DAY)
        ) / 3600.0 AS heures_ce_jour
    FROM status_actions
    -- ⚠️ CORRIGÉ (crash en prod) : toUInt32(Date - Date) sur une donnée
    -- corrompue/aberrante (corrected_end_datetime_utc avant
    -- start_datetime_utc, ou très loin dans le futur -- leadInFrame ou
    -- envm.end_datetime_utc en cause, pas identifié précisément) fait
    -- déborder toUInt32 par wraparound (diff négative) ou produit un
    -- nombre de jours réellement énorme -> range() plante avec "greater
    -- than the allowed maximum of 500000000". Calcul en Int64 (pas de
    -- wraparound), diff négative ramenée à 0, plafonnée à 400 jours --
    -- aucune permanence/statut légitime ne devrait dépasser ça. La
    -- mission concernée aura un nb_jours_de_mer sous-estimé plutôt que
    -- de faire échouer toute la table.
    ARRAY JOIN arrayMap(
        x -> toDate(start_datetime_utc) + x,
        range(least(toUInt32(greatest(
            toInt64(toDate(corrected_end_datetime_utc) - toDate(start_datetime_utc)),
            0
        )), 400) + 1)
    ) AS jour
    WHERE status IN ('NAVIGATING', 'ANCHORED')
),
jours_de_mer AS (
    SELECT
        mission_id,
        countIf(total_heures_jour > 4) AS nb_jours_de_mer
    FROM (
        SELECT mission_id, jour, sum(heures_ce_jour) AS total_heures_jour
        FROM status_day_overlap
        GROUP BY mission_id, jour
    )
    GROUP BY mission_id
),

-- Missions conjointes / administrations concourantes. Noms résolus via
-- monitorenv_proxy.administrations (déjà synchronisée dans le DWH,
-- cf. ligne "administrations" de sync_table_from_db_connection.csv --
-- contrairement à ce que laissait penser une note de scope antérieure)
-- pour éviter d'exposer des id bruts sur le graphique "Répartition des
-- administrations concourant aux missions conjointes".
intermin AS (
    SELECT
        ims.mission_general_info_id,
        groupArray(ims.administration_id)      AS administration_ids,
        groupArray(coalesce(adm.name, ''))      AS administration_names,
        groupArray(ims.control_unit_id)         AS control_unit_ids,
        COUNT(DISTINCT ims.administration_id)   AS nb_administrations
    FROM rapportnav_proxy.inter_ministerial_service ims
    LEFT JOIN monitorenv_proxy.administrations adm ON adm.id = ims.administration_id
    GROUP BY ims.mission_general_info_id
),

-- Classification mer/terre/air des moyens (même mapping que la requête 1,
-- dupliqué ici pour ne pas dépendre de l'ordre de sync entre les 2 tables)
resource_dim AS (
    SELECT
        id AS resource_id,
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
-- nb_resources_used seul ici désormais -- mission_terrain_types (plus bas
-- dans le SELECT final) ne se déduit plus des moyens employés, cf.
-- commentaire sur cette colonne.
mission_resources AS (
    SELECT
        ma.mission_id,
        uniqExact(mar.resource_id) AS nb_resources_used
    FROM rapportnav_proxy.mission_action_resource mar
    INNER JOIN rapportnav_proxy.mission_action ma ON ma.id = mar.action_id
    GROUP BY ma.mission_id
),

-- 3e définition possible de "heures en mer" (cf. avertissement détaillé
-- dans le SELECT final, colonne heures_moyen_nautique) : durée des
-- actions où AU MOINS UN moyen nautique (MER) a été employé. Dédupliqué
-- par action (max() avant le sumIf externe) pour ne pas compter 2x une
-- action qui mobilise 2 moyens nautiques à la fois.
action_nautical AS (
    SELECT
        ma.id AS action_id,
        ma.mission_id,
        toFloat64(if(
            ma.end_datetime_utc IS NOT NULL AND ma.end_datetime_utc >= ma.start_datetime_utc,
            dateDiff('second', ma.start_datetime_utc, ma.end_datetime_utc) / 3600.0,
            coalesce(toFloat64(ma.nbr_of_hours), 0)
        )) AS duration_h,
        max(rd.terrain_category = 'MER') AS has_mer_resource
    FROM rapportnav_proxy.mission_action ma
    INNER JOIN rapportnav_proxy.mission_action_resource mar ON mar.action_id = ma.id
    LEFT JOIN resource_dim rd ON rd.resource_id = mar.resource_id
    GROUP BY ma.id, ma.mission_id, ma.start_datetime_utc, ma.end_datetime_utc, ma.nbr_of_hours
),
heures_moyen_nautique_par_mission AS (
    SELECT
        mission_id,
        sumIf(duration_h, has_mer_resource) AS heures_moyen_nautique
    FROM action_nautical
    GROUP BY mission_id
),

-- Complétude nav (brique partielle, cf. avertissement déjà documenté
-- dans query_aem_par_mission_3_bases_clickhouse.sql)
nav_completeness AS (
    SELECT
        mission_id,
        countIf(coalesce(is_complete_for_stats, 0) = 0) = 0 AS toutes_actions_nav_completes
    FROM rapportnav_proxy.mission_action
    GROUP BY mission_id
),

-- Durée totale des actions de la mission (hors STATUS, même exclusion que
-- fact_action_pam_ulam.duration_h) -- utilisée pour le temps agent en
-- renfort extérieur ci-dessous.
mission_action_hours AS (
    SELECT
        mission_id,
        sum(toFloat64(if(
            end_datetime_utc IS NOT NULL AND end_datetime_utc >= start_datetime_utc,
            dateDiff('second', start_datetime_utc, end_datetime_utc) / 3600.0,
            coalesce(toFloat64(nbr_of_hours), 0)
        ))) AS total_action_hours
    FROM rapportnav_proxy.mission_action
    WHERE action_type != 'STATUS'
    GROUP BY mission_id
),
-- Nombre d'agents assignés à la mission (rapportnav_proxy.mission_crew).
-- Absences (mission_crew_absence, cf. crew_absences plus bas) non
-- exclues ici -- nb_agents reste l'effectif affecté, pas l'effectif
-- présent ; à affiner si le décompte doit exclure les agents absents sur
-- la période.
mission_crew_counts AS (
    SELECT
        mission_id,
        uniqExact(agent_id) AS nb_agents
    FROM rapportnav_proxy.mission_crew
    WHERE mission_id IS NOT NULL AND agent_id IS NOT NULL
    GROUP BY mission_id
),
-- "Équipage -- arrêt maladie/récupération, stagiaires accueillis"
-- (maquette PAM) : fonctionnalité RapportNav confirmée disponible
-- (schéma + backend + UI, vérifié dans le repo cloné -- crew-absence-
-- form.tsx et consorts, tests inclus) -- pas un stub, contrairement à ce
-- qui avait été noté avant vérification.
-- mission_crew_absence.reason est TEXT libre en base (pas de contrainte
-- CHECK/enum Postgres), mais un seul chemin de saisie existe (le
-- formulaire PAM) qui n'écrit que les 8 valeurs de
-- MissionCrewAbsenceReason (crew-type.ts) -- mappées ci-dessous vers
-- leur libellé français (use-crew-absence-reason.tsx), avec repli sur la
-- valeur brute si jamais une autre apparaît (donnée à surveiller, pas à
-- masquer).
crew_absences AS (
    SELECT
        mc.mission_id,
        uniqExact(mca.mission_crew_id) AS nb_agents_en_absence,
        groupUniqArray(toString(multiIf(
            mca.reason = 'SICK_LEAVE', 'Arrêt maladie',
            mca.reason = 'TRAINING', 'Formation',
            mca.reason = 'RECOVERING', 'Récupération',
            mca.reason = 'HOLIDAYS', 'Congés',
            mca.reason = 'MEETING', 'Réunion',
            mca.reason = 'MEDICAL_APPOINTMENT', 'Visite médicale',
            mca.reason = 'DISPATCHED_ELSEWHERE', 'Renfort extérieur',
            mca.reason = 'OTHER', 'Autre',
            coalesce(mca.reason, '')
        ))) AS absence_reasons
    FROM rapportnav_proxy.mission_crew_absence mca
    INNER JOIN rapportnav_proxy.mission_crew mc ON mc.id = mca.mission_crew_id
    WHERE mc.mission_id IS NOT NULL
    GROUP BY mc.mission_id
),
-- Stagiaires : rapportnav_proxy.mission_passenger, PAS mission_crew --
-- modélisés comme "passagers" avec un flag is_intern (case "Stagiaire"
-- du formulaire passagers PAM), aucun rôle STAGIAIRE dans agent_role.
crew_trainees AS (
    SELECT
        mission_id,
        uniqExact(id) AS nb_stagiaires
    FROM rapportnav_proxy.mission_passenger
    WHERE mission_id IS NOT NULL AND coalesce(is_intern, false) = true
    GROUP BY mission_id
)

SELECT
    -- envm.id (PK monitorenv_proxy.missions, non-nullable) plutôt que
    -- mgi.mission_id (FK côté rapportnav, potentiellement Nullable côté
    -- proxy) : ORDER BY refuse une clé de tri nullable
    -- (allow_nullable_key désactivé), même si l'INNER JOIN garantit déjà
    -- qu'aucune valeur NULL ne peut sortir ici.
    toInt32(envm.id) AS mission_id,
    toString(mgi.mission_id_uuid) AS mission_id_uuid,
    coalesce(mu.unit_names, '') AS unit_names,
    toUInt16(coalesce(mu.nb_unites_distinctes, 0)) AS nb_unites_distinctes,
    coalesce(mu.control_unit_ids, []) AS control_unit_ids,
    -- Liste complète des types d'unité (PAM/ULAM) et façades des unités
    -- engagées sur la mission (cf. mission_units) -- une mission conjointe
    -- PAM+ULAM ou multi-façades expose toutes les valeurs, pas la 1ère
    -- trouvée.
    coalesce(mu.unit_types, []) AS unit_types,
    coalesce(mu.facades, []) AS facades,
    toInt32(coalesce(mgi.service_id, 0)) AS service_id,
    -- Filtre "Bordée" (maquette PAM) : rapportnav_proxy.service.name
    -- (unique) porte cette granularité -- un même control_unit PAM peut
    -- avoir plusieurs services liés via service_control_unit (ex "PAM
    -- Themis A" / "PAM Themis B" pour les 2 bordées), confirmé contre
    -- ServiceModel.kt (rapportnav2). service_id est déjà exposé ci-dessus
    -- mais pas exploitable seul pour filtrer par bordée sans le nom.
    toString(coalesce(svc.name, '')) AS service_name,
    -- "Surveillance pêche encadrée CNSP ou libre" (maquette ULAM) : qui a
    -- ouvert la mission. monitorenv_proxy.missions.open_by est du texte
    -- LIBRE (colonne renommée depuis "author", champ formulaire "Ouvert
    -- par" côté RapportNav/MonitorEnv -- vérifié contre
    -- GeneralInformationsForm.tsx et MissionModel.kt, PAS un enum
    -- CACEM/CNSP/unité). Exposé brut plutôt que classé : vérifier les
    -- vraies valeurs saisies avant de construire un regroupement
    -- CACEM/CNSP/unité côté Metabase (mêmes précautions que
    -- env_theme_level_1). Rejoindre sur mission_id pour l'attacher aux
    -- actions/cibles de la mission si besoin.
    toString(coalesce(envm.open_by, '')) AS mission_open_by,
    -- assumeNotNull : envm.start_datetime_utc est Nullable côté proxy, mais
    -- le WHERE en fin de requête (>= 2025-01-01) exclut déjà toute ligne
    -- NULL avant le SELECT -- nécessaire pour servir de clé ORDER BY
    -- (même contrainte allow_nullable_key que mission_id plus haut).
    toDateTime64(assumeNotNull(envm.start_datetime_utc), 6) AS start_datetime_utc,
    toDateTime64(envm.end_datetime_utc, 6) AS end_datetime_utc,
    -- "Nombre d'heures de mission" (maquette PAM, Infos générales) :
    -- durée de la mission ENTIÈRE, du début à la fin (envm.start/end_
    -- datetime_utc), toutes actions confondues -- PAS une somme des
    -- durées d'actions individuelles (les actions ne couvrent pas
    -- forcément tout le temps de mission sans trou, et se chevauchent
    -- parfois). 0 tant que la mission n'est pas terminée
    -- (end_datetime_utc NULL).
    toFloat64(if(
        envm.end_datetime_utc IS NOT NULL,
        dateDiff('second', envm.start_datetime_utc, envm.end_datetime_utc) / 3600.0,
        0
    )) AS mission_duration_h,
    toString(multiIf(
        envm.end_datetime_utc IS NULL OR envm.start_datetime_utc IS NULL, 'UNAVAILABLE',
        envm.start_datetime_utc < now() AND envm.end_datetime_utc > now(), 'IN_PROGRESS',
        envm.end_datetime_utc <= now(), 'ENDED',
        envm.start_datetime_utc >= now(), 'UPCOMING',
        'UNAVAILABLE'
    )) AS mission_status,
    toString(coalesce(mgi.mission_report_type, '')) AS mission_report_type,
    toUInt8(coalesce(mgi.mission_report_type, '') = 'FIELD_REPORT') AS is_field_mission,
    toUInt8(coalesce(mgi.mission_report_type, '') = 'EXTERNAL_REINFORCEMENT_TIME_REPORT') AS is_external_reinforcement,
    toString(coalesce(mgi.reinforcement_type, '')) AS reinforcement_type,
    toString(coalesce(mgi.jdp_type, '')) AS jdp_type,
    toUInt8(coalesce(mgi.reinforcement_type, '') = 'JDP' OR mgi.jdp_type IS NOT NULL) AS is_jdp,
    -- "Temps en renfort extérieur" = durée des actions de la mission
    -- (mission_action_hours, hors STATUS) × nombre d'agents assignés à la
    -- mission (mission_crew_counts), pour toute mission dont
    -- reinforcement_type est renseigné.
    toFloat64(if(
        mgi.reinforcement_type IS NOT NULL,
        coalesce(mah.total_action_hours, 0) * coalesce(mcc.nb_agents, 0),
        0
    )) AS heures_renfort_exterieur,
    toUInt16(coalesce(mcc.nb_agents, 0)) AS nb_agents,
    -- "Équipage -- arrêt maladie/récupération, stagiaires accueillis"
    -- (maquette PAM) -- cf. crew_absences/crew_trainees plus haut.
    toUInt16(coalesce(ca.nb_agents_en_absence, 0)) AS nb_agents_en_absence,
    coalesce(ca.absence_reasons, []) AS absence_reasons,
    toUInt16(coalesce(ct.nb_stagiaires, 0)) AS nb_stagiaires,
    -- "Temps en JDP" / "nombre de missions JDP" : même principe, mais le
    -- cas EXTERNAL_REINFORCEMENT_TIME_REPORT exige en plus un
    -- reinforcement_type renseigné (pas nécessairement 'JDP' -- tel que
    -- spécifié). Périmètre distinct de is_jdp ci-dessus (qui exige
    -- reinforcement_type='JDP' précisément OU jdp_type renseigné, sans
    -- FIELD_REPORT) -- à vérifier que les deux définitions sont bien
    -- voulues comme différentes avant de les consolider.
    toFloat64(if(
        (coalesce(mgi.mission_report_type, '') = 'EXTERNAL_REINFORCEMENT_TIME_REPORT' AND mgi.reinforcement_type IS NOT NULL)
        OR coalesce(mgi.mission_report_type, '') = 'FIELD_REPORT'
        OR mgi.jdp_type IS NOT NULL,
        coalesce(mgi.nb_hour_at_sea, 0),
        0
    )) AS heures_jdp,
    toUInt8(
        (coalesce(mgi.mission_report_type, '') = 'EXTERNAL_REINFORCEMENT_TIME_REPORT' AND mgi.reinforcement_type IS NOT NULL)
        OR coalesce(mgi.mission_report_type, '') = 'FIELD_REPORT'
        OR mgi.jdp_type IS NOT NULL
    ) AS is_jdp_mission_reportable,
    toUInt8(coalesce(mgi.is_mission_armed, 0)) AS is_mission_armed,
    toUInt8(coalesce(mgi.is_with_interministerial_service, 0)) AS is_with_interministerial_service,
    toUInt16(coalesce(im.nb_administrations, 0)) AS nb_intermin_administrations,
    coalesce(im.administration_ids, []) AS intermin_administration_ids,
    -- Libellés résolus via monitorenv_proxy.administrations pour le
    -- graphique "Répartition des administrations concourant aux missions
    -- conjointes" (maquette v2) -- éviter d'afficher des id bruts.
    coalesce(im.administration_names, []) AS intermin_administration_names,
    coalesce(im.control_unit_ids, []) AS intermin_control_unit_ids,
    -- ⚠️ "Heures en mer" : 3 définitions distinctes coexistent dans cette
    -- table, à ne pas confondre ni sommer entre elles.
    --   1. declared_hours_at_sea : saisie DÉCLARATIVE de l'agent dans
    --      mission_general_info.nb_hour_at_sea. Fiable pour "ce que
    --      l'agent affirme", pas pour un calcul automatisé cohérent
    --      (dépend de la rigueur de saisie, pas recalculable/vérifiable).
    --   2. computed_hours_at_sea : durée reconstituée à partir des statuts
    --      nav ANCHORED/NAVIGATING de la mission (cf. CTE heures_de_mer,
    --      méthode validée sur l'AEM) -- mesure la présence en mer DU
    --      NAVIRE PORTEUR de la mission, indépendamment des moyens
    --      réellement mobilisés par chaque action.
    --   3. heures_moyen_nautique (plus bas) : durée des ACTIONS où au
    --      moins un moyen nautique (MER, cf. resource_dim) a été
    --      explicitement employé -- mesure l'usage déclaré des moyens,
    --      pas la présence en mer du navire. Peut diverger de (2) : une
    --      mission peut avoir des heures ANCHORED/NAVIGATING sans qu'aucune
    --      action n'y déclare de moyen nautique (mauvaise saisie), ou
    --      l'inverse (moyen nautique sur une action pendant que le statut
    --      nav mission dit autre chose).
    -- computed_hours_at_sea (2) reste la référence recommandée pour "temps
    -- passé en mer" côté dashboard (cf. commentaire déjà en place dans
    -- heures_de_mer) ; (3) répond à une question différente ("le moyen
    -- nautique a-t-il été utilisé"), pas un remplacement de (2).
    toFloat64(coalesce(mgi.nb_hour_at_sea, 0)) AS declared_hours_at_sea,
    toFloat64(coalesce(hm.computed_hours_at_sea, 0)) AS computed_hours_at_sea,
    toFloat64(coalesce(hmn.heures_moyen_nautique, 0)) AS heures_moyen_nautique,
    -- heures_navigation_hypothese_moteur : laissé à 0 volontairement.
    -- Alimentait auparavant une hypothèse non confirmée (statut NAVIGATING
    -- seul = "moteur en action") -- en cours de définition côté métier
    -- (implémentation propre pas encore livrée). Ne pas réutiliser cette
    -- colonne tant qu'elle reste à 0 ; à repeupler une fois la définition
    -- confirmée.
    toFloat64(0) AS heures_navigation_hypothese_moteur,
    toUInt16(coalesce(jdm.nb_jours_de_mer, 0)) AS nb_jours_de_mer,
    toFloat64(coalesce(mgi.distance_in_nautical_miles, 0)) AS distance_nm,
    toFloat64(coalesce(mgi.consumed_fuel_in_liters, 0)) AS consumed_fuel_liters,
    toFloat64(coalesce(mgi.consumed_go_in_liters, 0)) AS consumed_go_liters,
    toUInt16(coalesce(mr.nb_resources_used, 0)) AS nb_resources_used,
    -- mission_terrain_types : au niveau mission, monitorenv_proxy.
    -- missions.mission_types est LA source (text[], valeurs AIR/LAND/SEA --
    -- MissionTypeEnum.kt côté monitorenv, migration V0.072). Remplace
    -- l'ancienne déduction à partir des moyens employés sur les actions de
    -- la mission (mission_action_resource -> control_unit_resources.type),
    -- qui n'était qu'une approximation. Une mission NAV sans sortie
    -- terrain (aucun véhicule/navire de l'unité engagé) a mission_types
    -- vide/NULL -- vu et attendu, pas une anomalie.
    -- ⚠️ Ceci reste au niveau MISSION (1 tableau par mission). Une
    -- ventilation au niveau ACTION (comme le fait déjà terrain_control
    -- sur fact_action_pam_ulam/fact_cible_pam_ulam pour les contrôles)
    -- n'a PAS le même niveau de confiance pour les 3 sources :
    --   - FISH : mission_action.action_type LAND_CONTROL/SEA_CONTROL se
    --     mappe directement sur TERRE/MER (mêmes valeurs que control_type
    --     côté MonitorFish, déjà exploité par terrain_control).
    --   - ENV : renseigné seulement parfois, via vehicle_type sur l'action
    --     -- pas un champ fiable à 100%, comportement à vérifier données en
    --     main avant de l'utiliser.
    --   - NAV : pas de champ identifié à date pour un mer/terre/air par
    --     action (à la différence du niveau mission via mission_types) --
    --     à documenter/creuser si ce niveau de détail est demandé.
    -- arraySort() : mission_types est un ensemble non ordonné côté
    -- monitorenv (vérifié -- tout le backend le lit par appartenance,
    -- "MissionTypeEnum.X in missionTypes", jamais par position ; seule
    -- règle de validation = non-vide). ['LAND','SEA'] et ['SEA','LAND']
    -- sont donc le même fait métier avec un ordre de saisie différent --
    -- trié pour qu'ils se groupent ensemble en rapport plutôt que comme
    -- 2 combinaisons distinctes.
    arraySort(arrayMap(
        x -> multiIf(x = 'SEA', 'MER', x = 'LAND', 'TERRE', x = 'AIR', 'AIR', toString(x)),
        coalesce(envm.mission_types, [])
    )) AS mission_terrain_types,
    toUInt8(envm.end_datetime_utc IS NOT NULL AND envm.end_datetime_utc < now()) AS is_mission_finished,
    toUInt8(coalesce(nc.toutes_actions_nav_completes, 0)) AS nav_toutes_actions_completes,
    -- Règle "Missions rapportées" (maquette v2, tooltip) : "Seules les
    -- missions terminées et complètes dans RPN sont comptabilisées ici".
    -- nav_toutes_actions_completes agrège déjà is_complete_for_stats au
    -- niveau action (cf. CTE nav_completeness), donc ce flag couvre bien
    -- les deux conditions -- pas besoin d'un 3e critère séparé. Exposé en
    -- colonne dédiée plutôt que laissé à chaque carte Metabase de faire le
    -- AND des deux champs séparément (risque d'application incohérente).
    toUInt8(
        (envm.end_datetime_utc IS NOT NULL AND envm.end_datetime_utc < now())
        AND coalesce(nc.toutes_actions_nav_completes, 0) = 1
    ) AS is_mission_reportable,
    'rapportnav' AS source_system,
    now() AS updated_at
FROM rapportnav_proxy.mission_general_info mgi
INNER JOIN monitorenv_proxy.missions envm ON envm.id = mgi.mission_id
LEFT JOIN rapportnav_proxy.service svc ON svc.id = mgi.service_id
-- INNER JOIN (pas LEFT) : mission_units ne contient que les missions
-- avec au moins une unité PAM ou ULAM (cf. pam_ulam_control_units plus
-- haut) -- c'est ce qui filtre le rapport aux missions PAM et ULAM.
INNER JOIN mission_units mu    ON mu.mission_id = mgi.mission_id
LEFT JOIN intermin im           ON im.mission_general_info_id = mgi.id
LEFT JOIN heures_de_mer hm      ON hm.mission_id = mgi.mission_id
LEFT JOIN jours_de_mer jdm      ON jdm.mission_id = mgi.mission_id
LEFT JOIN mission_resources mr  ON mr.mission_id = mgi.mission_id
LEFT JOIN heures_moyen_nautique_par_mission hmn ON hmn.mission_id = mgi.mission_id
LEFT JOIN nav_completeness nc   ON nc.mission_id = mgi.mission_id
LEFT JOIN mission_action_hours mah ON mah.mission_id = mgi.mission_id
LEFT JOIN mission_crew_counts mcc  ON mcc.mission_id = mgi.mission_id
LEFT JOIN crew_absences ca         ON ca.mission_id = mgi.mission_id
LEFT JOIN crew_trainees ct         ON ct.mission_id = mgi.mission_id
-- ⚠️ même filtre de date codé en dur que query_aem_par_mission_3_bases_clickhouse.sql
-- (portée jamais expliquée dans le code source) -- à confirmer avec Alexandre,
-- ou à retirer si le rapport PAM+ULAM doit couvrir tout l'historique.
WHERE envm.start_datetime_utc >= toDateTime('2025-01-01 00:00:00');
