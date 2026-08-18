-- =====================================================================
-- Alimente rapportnav.fact_action_pam_ulam (query_filepath pour la ligne
-- "fact_action_pam_ulam" de sync_table_from_db_connection.csv).
-- Couvre les unités PAM ET ULAM dans une seule table (cf.
-- pam_ulam_control_units plus bas) -- action_type_mapping et le référentiel
-- de contrôle sont identiques des deux côtés, seul le référentiel d'unités
-- (rapportnav.dim_unit_reference) distingue PAM/ULAM via unit_type.
-- ⚠️ Ce fichier DOIT tourner après dim_unit_reference.sql dans
-- sync_table_from_db_connection.csv (aucune dépendance native entre
-- lignes de ce flow -- cf. commentaire détaillé dans dim_unit_reference.sql).
-- =====================================================================
WITH
-- Filtre unités PAM + ULAM : service_type via service_control_unit, repli
-- sur le nom si le lien n'est pas renseigné -- constaté non peuplé en
-- pratique (aucune fixture de test ne renseigne service_control_unit),
-- donc le repli par nom est la voie principale, pas un simple filet de
-- sécurité. Même convention de nommage pour les 2 (nom d'unité préfixé
-- PAM/ULAM, confirmé côté métier).
pam_ulam_control_units AS (
    SELECT DISTINCT cu.id AS control_unit_id
    FROM monitorenv_proxy.control_units cu
    LEFT JOIN rapportnav_proxy.service_control_unit scu ON scu.control_unit_id = cu.id
    LEFT JOIN rapportnav_proxy.service s ON s.id = scu.service_id AND s.deleted_at IS NULL
    WHERE s.service_type IN ('PAM', 'ULAM')
       OR startsWith(upper(cu.name), 'ULAM')
       OR startsWith(upper(cu.name), 'PAM')
),
-- Référentiel unité VALIDÉ AEM (idem requête 2) -- rapportnav_proxy.service
-- sert uniquement au filtre PAM/ULAM ci-dessus, pas de référentiel concurrent.
-- INNER JOIN sur pam_ulam_control_units : filtre les actions dont la
-- mission n'a aucune unité PAM ni ULAM associée.
mission_units AS (
    SELECT
        mcu.mission_id,
        arrayStringConcat(groupArray(cu.name), ', ') AS unit_names,
        -- Approximation : mission conjointe entre unités de façades
        -- différentes -> on ne garde que la 1ère façade trouvée (même
        -- limitation que terrain_type_first plus bas).
        arrayElement(groupUniqArray(uref.facade_ref), 1) AS facade,
        -- unit_type : priorité à rapportnav.dim_unit_reference (référentiel
        -- unique, cf. discussion en chat), repli sur le nom en direct pour
        -- toute unité PAM/ULAM pas encore ajoutée à ce référentiel manuel
        -- (le filtre pam_ulam_control_units ci-dessus les inclut déjà via
        -- son propre repli par nom -- sans ce 2e repli ici, ces unités
        -- ressortiraient avec unit_type vide). Approximation "1er trouvé"
        -- identique à facade ci-dessus pour les (rares) missions conjointes
        -- PAM+ULAM.
        arrayElement(groupUniqArray(coalesce(nullIf(uref.unit_type, ''), multiIf(
            startsWith(upper(cu.name), 'PAM'), 'PAM',
            startsWith(upper(cu.name), 'ULAM'), 'ULAM',
            'AUTRE'
        ))), 1) AS unit_type
    FROM monitorenv_proxy.missions_control_units mcu
    INNER JOIN monitorenv_proxy.control_units cu ON cu.id = mcu.control_unit_id
    INNER JOIN pam_ulam_control_units uu ON uu.control_unit_id = cu.id
    LEFT JOIN rapportnav.dim_unit_reference uref ON uref.control_unit_id = cu.id
    GROUP BY mcu.mission_id
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
-- Un moyen (ou plusieurs) par action -> agrégés en tableau, une ligne par action.
action_resources AS (
    SELECT
        toString(mar.action_id) AS action_id,
        groupArray(mar.resource_id) AS resource_ids,
        groupArray(toString(rd.resource_type_raw)) AS resource_types,
        -- ⚠️ approximation : si l'action mobilise des moyens de catégories
        -- différentes (ex: un bateau + un véhicule sur la même sortie),
        -- on ne garde que le 1er trouvé. À signaler si ça arrive en pratique
        -- (cf. GROUP BY ci-dessous, arrayElement sur un groupUniqArray).
        arrayElement(groupUniqArray(rd.terrain_category), 1) AS terrain_type_first
    FROM rapportnav_proxy.mission_action_resource mar
    LEFT JOIN resource_dim rd ON rd.resource_id = mar.resource_id
    GROUP BY mar.action_id
),
-- Référentiel libellé français / politique publique / thématique par
-- action_type, repris du dictionnaire de données métier "Types et
-- sous-types d'actions" (export CSV du 2026-08-14). Clé = action_type
-- seul, sauf UNIT_MANAGEMENT_TRAINING où action_subtype distingue des
-- activités de nature différente (DIVING / MAN_OVERBOARD_RECOVERY /
-- TECHNICAL_INTERVENTION_SHOOTING -- valeurs d'un champ contrôlé côté
-- rapportnav, pas du texte libre).
-- ⚠️ TRAINING (le "vrai", pas UNIT_MANAGEMENT_TRAINING) : action_subtype
-- vient d'un champ texte libre saisi par l'utilisateur (ma.training_type).
-- Le dictionnaire source recense des dizaines de valeurs distinctes
-- (fautes de frappe, variantes de casse/accents...), impossible à mapper
-- ligne à ligne de façon fiable et pérenne. On applique donc à toute
-- action TRAINING le libellé par défaut de l'action_type (Entraînement /
-- Contrôle des activités maritimes / Transversal), quel que soit le
-- texte saisi -- si un besoin de reporting plus fin sur les formations
-- émerge, il faudra un vrai référentiel de training_type côté rapportnav
-- plutôt qu'un mapping texte libre ici.
action_type_mapping AS (
    SELECT 'ANTI_POLLUTION' AS action_type, '' AS action_subtype_key, 'Opération de lutte anti-pollution' AS libelle_francais, 'Contrôle des activités maritimes' AS politique_publique, 'Environnement marin' AS thematique
    UNION ALL SELECT 'BAAEM_PERMANENCE', '', 'Permanence BAAEM - bureau de l''action de l''Etat en mer', 'Contrôle des activités maritimes', 'Transversal'
    UNION ALL SELECT 'COMMUNICATION', '', 'Communication', 'Contrôle des activités maritimes', 'Transversal'
    -- Libellés suffixés "?" dans le dictionnaire source (CONTACT, INQUIRY) :
    -- incertitude du métier sur le nom, reprise telle quelle -- à ne pas
    -- "corriger" sans validation métier.
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
    -- RESOURCES_MAINTENANCE : mêmes libellé/politique publique/thématique
    -- quel que soit action_subtype (NAUTICAL/TERRESTRIAL) ou terrain_type
    -- (MER/TERRE/AIR) dans le dictionnaire source -> clé action_type seul.
    UNION ALL SELECT 'RESOURCES_MAINTENANCE', '', 'Entretien des moyens', 'Contrôle des activités maritimes', 'Transversal'
    UNION ALL SELECT 'SECURITY_VISIT', '', 'Visite sécurité', 'Contrôle des activités maritimes', 'Transversal'
    UNION ALL SELECT 'TRAINING', '', 'Entraînement', 'Contrôle des activités maritimes', 'Transversal'
    UNION ALL SELECT 'UNIT_MANAGEMENT_OTHER', '', 'Gestion de l''unité - autres', 'Contrôle des activités maritimes', 'Transversal'
    UNION ALL SELECT 'UNIT_MANAGEMENT_PLANNING', '', 'Gestion de l''unité - planning', 'Contrôle des activités maritimes', 'Transversal'
    UNION ALL SELECT 'UNIT_MANAGEMENT_TRAINING', '', 'Entraînement', 'Contrôle des activités maritimes', 'Transversal'
    UNION ALL SELECT 'UNIT_MANAGEMENT_TRAINING', 'DIVING', 'Entraînement', 'Contrôle des activités maritimes', 'Transversal'
    UNION ALL SELECT 'UNIT_MANAGEMENT_TRAINING', 'MAN_OVERBOARD_RECOVERY', 'Formation', 'Assistance/ sauvetage', 'Assistance/ sauvetage'
    UNION ALL SELECT 'UNIT_MANAGEMENT_TRAINING', 'TECHNICAL_INTERVENTION_SHOOTING', 'Formation', 'Maintien de l''ordre public', 'Gestes Techniques Professionnels d''Intervention'
)

SELECT
    toString(ma.id) AS action_id,
    ma.mission_id AS mission_id,
    coalesce(mu.unit_names, '') AS unit_names,
    coalesce(mu.facade, '') AS facade,
    toString(coalesce(mu.unit_type, '')) AS unit_type,
    toString(ma.action_type) AS action_type,
    toString(multiIf(
        ma.action_type = 'TRAINING', coalesce(ma.training_type, ''),
        ma.action_type = 'UNIT_MANAGEMENT_TRAINING', coalesce(ma.unit_management_training_type, ''),
        ma.action_type = 'RESOURCES_MAINTENANCE', coalesce(ma.resource_type, ''),
        coalesce(ma.reason, '')
    )) AS action_subtype,
    ma.resource_type AS resource_type_declared,
    toDateTime64(ma.start_datetime_utc, 6) AS start_datetime_utc,
    toDateTime64(ma.end_datetime_utc, 6) AS end_datetime_utc,
    toFloat64(if(
        ma.end_datetime_utc IS NOT NULL AND ma.end_datetime_utc >= ma.start_datetime_utc,
        dateDiff('second', ma.start_datetime_utc, ma.end_datetime_utc) / 3600.0,
        coalesce(toFloat64(ma.nbr_of_hours), 0)
    )) AS duration_h,
    ma.nbr_of_hours AS nbr_of_hours_declared,
    toUInt8(coalesce(ma.is_complete_for_stats, 0)) AS is_complete_for_stats,
    toUInt16(length(coalesce(ar.resource_ids, []))) AS nb_resources_linked,
    coalesce(ar.resource_ids, []) AS resource_ids,
    coalesce(ar.resource_types, []) AS resource_types,
    toString(coalesce(ar.terrain_type_first, 'INDETERMINE')) AS terrain_type,
    -- Mapping métier (cf. action_type_mapping ci-dessus) : coalesce sur
    -- action_type en repli si l'action_type n'a pas de ligne dans le
    -- référentiel (ne devrait pas arriver -- tous les action_type connus,
    -- hors STATUS déjà exclu, sont couverts).
    toString(coalesce(nullIf(atm.libelle_francais, ''), toString(ma.action_type))) AS libelle_francais,
    toString(coalesce(atm.politique_publique, '')) AS politique_publique,
    toString(coalesce(atm.thematique, '')) AS thematique,
    now() AS updated_at
FROM rapportnav_proxy.mission_action ma
-- INNER JOIN (pas LEFT) : filtre aux actions dont la mission a au moins
-- une unité PAM ou ULAM (cf. pam_ulam_control_units plus haut).
INNER JOIN mission_units mu ON mu.mission_id = ma.mission_id
LEFT JOIN action_resources ar ON ar.action_id = toString(ma.id)
-- action_subtype_key : ne différencie que UNIT_MANAGEMENT_TRAINING (seul
-- action_type dont action_subtype est un champ contrôlé, pas du texte
-- libre -- cf. commentaire sur action_type_mapping).
LEFT JOIN action_type_mapping atm
    ON atm.action_type = toString(ma.action_type)
    AND atm.action_subtype_key = multiIf(
        ma.action_type = 'UNIT_MANAGEMENT_TRAINING', coalesce(ma.unit_management_training_type, ''),
        ''
    )
-- STATUS = marqueurs de changement d'état nav (ANCHORED/NAVIGATING/...),
-- déjà exploités dans fact_mission_pam_ulam.computed_hours_at_sea -- pas
-- une "activité" au sens métier du rapport.
WHERE ma.action_type != 'STATUS';
