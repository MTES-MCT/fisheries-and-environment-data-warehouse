-- =====================================================================
-- Alimente rapportnav.fact_mission_ulam (query_filepath pour la ligne
-- "fact_mission_ulam" de sync_table_from_db_connection.csv).
-- SELECT pur : le flow générique fait CREATE TABLE ... AS <cette requête>
-- (ddl_script_path laissé vide -> schéma inféré, cf. discussion en chat).
-- =====================================================================
WITH
-- Référentiel façade par unité (monitorenv, control_unit_id) -- fourni
-- manuellement (pas une vraie table monitorenv), repris de la CTE
-- dim_unit_reference_by_id de missions_aem.sql. Dupliqué ici pour la
-- même raison que les autres référentiels de ce fichier (requêtes
-- indépendantes, pas de vue/macro partagée possible dans ce repo) --
-- si la liste évolue, penser à la répercuter dans les 4 fichiers
-- (missions_aem.sql + les 3 requêtes ULAM).
dim_unit_reference_by_id AS (
    SELECT 10194 AS control_unit_id, 'MED' AS facade_ref
    UNION ALL SELECT 10039, 'MED'
    UNION ALL SELECT 10452, 'MEMN'
    UNION ALL SELECT 10204, 'NAMO'
    UNION ALL SELECT 10457, 'NAMO'  -- Brest
    UNION ALL SELECT 10288, 'NAMO'  -- Douarnenez
    UNION ALL SELECT 10074, 'MED'   -- 2A
    UNION ALL SELECT 10192, 'MED'   -- 2B
    UNION ALL SELECT 10225, 'SA'
    UNION ALL SELECT 10255, 'SA'
    UNION ALL SELECT 10420, 'MED'
    UNION ALL SELECT 10176, 'NAMO'
    UNION ALL SELECT 10428, 'NAMO'
    UNION ALL SELECT 10210, 'MEMN'
    UNION ALL SELECT 10449, 'NAMO'
    UNION ALL SELECT 10050, 'MEMN'
    UNION ALL SELECT 10318, 'MEMN'
    UNION ALL SELECT 10364, 'SA'
    UNION ALL SELECT 10303, 'MED'
    UNION ALL SELECT 10423, 'MEMN'
    UNION ALL SELECT 10166, 'MED'
    UNION ALL SELECT 10171, 'NAMO'
    UNION ALL SELECT 10169, 'Antilles'
    UNION ALL SELECT 10327, 'Antilles'
    UNION ALL SELECT 10265, 'Guyane'
    UNION ALL SELECT 10183, 'Sud de l''Océan indien'
    UNION ALL SELECT 10430, 'Saint-Pierre et Miquelon'
    UNION ALL SELECT 10047, 'Sud de l''Océan indien'
    UNION ALL SELECT 10080, 'NAMO'                    -- PAM Themis
    UNION ALL SELECT 10121, 'MEMN'                    -- PAM Jeanne Barret
    UNION ALL SELECT 10141, 'MED'                     -- PAM Gyptis
    UNION ALL SELECT 10404, 'SA'                      -- PAM Iris
    UNION ALL SELECT 10345, 'Sud de l''Océan indien'  -- PAM Osiris II
    UNION ALL SELECT 10519, 'Guyane'                  -- PAM Cayenne
),
-- Référentiel "unité" VALIDÉ sur le rapport AEM : monitorenv_proxy
-- missions_control_units + control_units (PAS rapportnav_proxy.service --
-- la fiche ULAM d'origine a été écrite avant le travail AEM et n'en tenait
-- pas compte). Repris tel quel de la CTE mission_units de
-- query_aem_par_mission_3_bases_clickhouse.sql.
mission_units AS (
    SELECT
        mcu.mission_id,
        arrayStringConcat(groupArray(cu.name), ', ') AS unit_names,
        COUNT(DISTINCT cu.id) AS nb_unites_distinctes,
        groupArray(cu.id) AS control_unit_ids,
        -- Approximation : mission conjointe entre unités de façades
        -- différentes -> on ne garde que la 1ère façade trouvée.
        arrayElement(groupUniqArray(uref.facade_ref), 1) AS facade
    FROM monitorenv_proxy.missions_control_units mcu
    INNER JOIN monitorenv_proxy.control_units cu ON cu.id = mcu.control_unit_id
    LEFT JOIN dim_unit_reference_by_id uref ON uref.control_unit_id = cu.id
    GROUP BY mcu.mission_id
),
-- ⚠️ Classification PAM/ULAM (unit_type) : dim_unit_reference_by_name
-- (nécessaire pour PAM, en attendant l'export control_unit_id côté PAM)
-- toujours pas rebranchée -- seule la façade (dim_unit_reference_by_id,
-- ULAM) l'est désormais. unit_type reste vide plus bas.

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
              status IN ('ANCHORED', 'NAVIGATING')) AS computed_hours_at_sea,
        -- ⚠️ HYPOTHÈSE À VÉRIFIER : "heures moteur" ≈ heures où le statut
        -- mission est NAVIGATING seul (bateau qui avance), à l'exclusion
        -- d'ANCHORED (mouillage, moteur probablement coupé ou au ralenti).
        -- Personne n'a confirmé ce mapping côté métier -- à valider avant
        -- toute utilisation en dashboard. Ne peut pas être ventilé par
        -- moyen (les actions STATUS n'ont pas de resource_id).
        sumIf(dateDiff('second', start_datetime_utc, corrected_end_datetime_utc) / 3600.0,
              status = 'NAVIGATING') AS heures_navigation_hypothese_moteur
    FROM status_actions
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
mission_resources AS (
    SELECT
        ma.mission_id,
        groupUniqArray(toString(rd.terrain_category)) AS mission_terrain_types,
        uniqExact(mar.resource_id) AS nb_resources_used
    FROM rapportnav_proxy.mission_action_resource mar
    INNER JOIN rapportnav_proxy.mission_action ma ON ma.id = mar.action_id
    LEFT JOIN resource_dim rd ON rd.resource_id = mar.resource_id
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
    -- TODO : classification PAM/ULAM toujours pas rebranchée
    -- (dim_unit_reference_by_name, nécessite l'export control_unit_id
    -- côté PAM) -- laissé vide plutôt que de réintroduire l'heuristique
    -- service.name.
    '' AS unit_type,
    coalesce(mu.facade, '') AS facade,
    toInt32(coalesce(mgi.service_id, 0)) AS service_id,
    -- assumeNotNull : envm.start_datetime_utc est Nullable côté proxy, mais
    -- le WHERE en fin de requête (>= 2025-01-01) exclut déjà toute ligne
    -- NULL avant le SELECT -- nécessaire pour servir de clé ORDER BY
    -- (même contrainte allow_nullable_key que mission_id plus haut).
    toDateTime64(assumeNotNull(envm.start_datetime_utc), 6) AS start_datetime_utc,
    toDateTime64(envm.end_datetime_utc, 6) AS end_datetime_utc,
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
    -- "Temps en renfort extérieur" : le champ "temps agent individualisé"
    -- n'existe toujours pas (cf. gap déjà documenté) -- approximation
    -- retenue : heures DÉCLARATIVES de la mission (mission_general_info.
    -- nb_hour_at_sea, PAS computed_hours_at_sea/heures_moyen_nautique
    -- plus bas) pour toute mission EXTERNAL_REINFORCEMENT_TIME_REPORT,
    -- FIELD_REPORT, ou avec un jdp_type renseigné. Périmètre volontairement
    -- plus large que le seul flag is_external_reinforcement ci-dessus.
    toFloat64(if(
        coalesce(mgi.mission_report_type, '') IN ('EXTERNAL_REINFORCEMENT_TIME_REPORT', 'FIELD_REPORT')
        OR mgi.jdp_type IS NOT NULL,
        coalesce(mgi.nb_hour_at_sea, 0),
        0
    )) AS heures_renfort_exterieur,
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
    toFloat64(coalesce(hm.heures_navigation_hypothese_moteur, 0)) AS heures_navigation_hypothese_moteur,
    toFloat64(coalesce(mgi.distance_in_nautical_miles, 0)) AS distance_nm,
    toFloat64(coalesce(mgi.consumed_fuel_in_liters, 0)) AS consumed_fuel_liters,
    toFloat64(coalesce(mgi.consumed_go_in_liters, 0)) AS consumed_go_liters,
    toUInt16(coalesce(mr.nb_resources_used, 0)) AS nb_resources_used,
    coalesce(mr.mission_terrain_types, []) AS mission_terrain_types,
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
LEFT JOIN mission_units mu      ON mu.mission_id = mgi.mission_id
LEFT JOIN intermin im           ON im.mission_general_info_id = mgi.id
LEFT JOIN heures_de_mer hm      ON hm.mission_id = mgi.mission_id
LEFT JOIN mission_resources mr  ON mr.mission_id = mgi.mission_id
LEFT JOIN heures_moyen_nautique_par_mission hmn ON hmn.mission_id = mgi.mission_id
LEFT JOIN nav_completeness nc   ON nc.mission_id = mgi.mission_id
-- ⚠️ même filtre de date codé en dur que query_aem_par_mission_3_bases_clickhouse.sql
-- (portée jamais expliquée dans le code source) -- à confirmer avec Alexandre,
-- ou à retirer si le rapport ULAM doit couvrir tout l'historique.
WHERE envm.start_datetime_utc >= toDateTime('2025-01-01 00:00:00');
