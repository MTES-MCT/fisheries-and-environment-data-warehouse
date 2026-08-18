-- =====================================================================
-- Alimente rapportnav.dim_unit_reference (query_filepath pour la ligne
-- "dim_unit_reference" de sync_table_from_db_connection.csv).
-- Référentiel unité PAM/ULAM (nom d'origine, façade, zone maritime,
-- classification unit_type) -- SOURCE UNIQUE, remplace les 4 copies
-- historiquement dupliquées dans missions_aem.sql + les 3 requêtes
-- rapport_pam_ulam_*.sql (cf. discussion en chat -- une seule liste à
-- maintenir plutôt que 4).
--
-- ⚠️ DÉPENDANCE D'ORDONNANCEMENT (assumée en connaissance de cause, cf.
-- discussion en chat) : ce flow n'a pas de notion de dépendances entre
-- lignes de sync_table_from_db_connection.csv (chaque ligne est un
-- CronClock indépendant, cf. flows_config.py). Cette table DOIT être
-- planifiée nettement avant missions_aem.sql et les 3 requêtes
-- rapport_pam_ulam_*.sql qui la joignent -- sinon elles tournent sur une
-- version vide/périmée sans erreur visible. Cf. marge choisie dans
-- sync_table_from_db_connection.csv (28 4 * * *, avant tout le reste).
--
-- control_unit_id : liste manuellement maintenue des ~30 unités PAM/ULAM
-- connues (pas une vraie table monitorenv -- ce référentiel n'existe nulle
-- part côté source). Si une nouvelle unité PAM/ULAM apparaît côté
-- monitorenv AVANT d'être ajoutée ici, elle sera quand même incluse dans
-- les rapports (le filtre d'inclusion pam_ulam_control_units scanne TOUTES
-- les control_units en direct, indépendamment de cette liste), mais avec
-- facade/nom_ou_ville_origine/zone_maritime vides -- seul unit_type reste
-- disponible via le repli sur le nom fait par les requêtes consommatrices
-- (cf. leur commentaire "coalesce(nullIf(uref.unit_type, ''), ...)").
-- =====================================================================
WITH
manual_reference AS (
    SELECT 10194 AS control_unit_id, 'DDTM 06' AS nom_ou_ville_origine, 'MED' AS facade_ref, 'Méditerranée' AS zone_maritime
    UNION ALL SELECT 10039, 'DDTM 13',              'MED',  'Méditerranée'
    UNION ALL SELECT 10452, 'DDTM 14',              'MEMN', 'Manche-Mer du Nord'
    UNION ALL SELECT 10204, 'DDTM 22',              'NAMO', 'Atlantique'
    UNION ALL SELECT 10457, 'DDTM 29 Nord',         'NAMO', 'Atlantique'  -- Brest
    UNION ALL SELECT 10288, 'DDTM 29 Sud',          'NAMO', 'Atlantique'  -- Douarnenez
    UNION ALL SELECT 10074, 'DMLC',                 'MED',  'Méditerranée' -- 2A
    UNION ALL SELECT 10192, 'DMLC',                 'MED',  'Méditerranée' -- 2B
    UNION ALL SELECT 10225, 'DDTM 33',              'SA',   'Atlantique'
    UNION ALL SELECT 10255, 'DDTM 17',              'SA',   'Atlantique'
    UNION ALL SELECT 10420, 'DDTM 34/30',           'MED',  'Méditerranée'
    UNION ALL SELECT 10176, 'DDTM 35',              'NAMO', 'Atlantique'
    UNION ALL SELECT 10428, 'DDTM 44',              'NAMO', 'Atlantique'
    UNION ALL SELECT 10210, 'DDTM 50',              'MEMN', 'Manche-Mer du Nord'
    UNION ALL SELECT 10449, 'DDTM 56',              'NAMO', 'Atlantique'
    UNION ALL SELECT 10050, 'DDTM 59',              'MEMN', 'Manche-Mer du Nord'
    UNION ALL SELECT 10318, 'DDTM 62/80',           'MEMN', 'Manche-Mer du Nord'
    UNION ALL SELECT 10364, 'DDTM 64/40',           'SA',   'Atlantique'
    UNION ALL SELECT 10303, 'DDTM 66/11',           'MED',  'Méditerranée'
    UNION ALL SELECT 10423, 'DDTM 76/27',           'MEMN', 'Manche-Mer du Nord'
    UNION ALL SELECT 10166, 'DDTM 83',              'MED',  'Méditerranée'
    UNION ALL SELECT 10171, 'DDTM 85',              'NAMO', 'Atlantique'
    UNION ALL SELECT 10169, 'DM Guadeloupe (971)',  'Guadeloupe', 'Antilles'
    UNION ALL SELECT 10327, 'DM Martinique (972)',  'Martinique', 'Antilles'
    UNION ALL SELECT 10265, 'DGTM Guyane (973)',    'Guyane', 'Guyane'
    UNION ALL SELECT 10183, 'DM SOI (974)',         'La Réunion', 'Sud de l''Océan indien'
    UNION ALL SELECT 10430, 'DTAM St Pierre et Miquelon (975)', 'Saint-Pierre et Miquelon', 'Saint-Pierre et Miquelon'
    UNION ALL SELECT 10047, 'DEALM Mayotte (976)',  'Mayotte', 'Sud de l''Océan indien'
    -- Ajoute ici tout ULAM supplémentaire découvert en base.
    -- PAM : pas de bordée A/B côté MonitorEnv, une seule entrée par navire.
    UNION ALL SELECT 10080, 'DIRM NAMO',            'NAMO', 'Atlantique'          -- PAM Themis
    UNION ALL SELECT 10121, 'DIRM MEMN',            'MEMN', 'Manche-Mer du Nord'  -- PAM Jeanne Barret
    UNION ALL SELECT 10141, 'DIRM MED',             'MED',  'Méditerranée'        -- PAM Gyptis
    UNION ALL SELECT 10404, 'DIRM SA',              'SA',   'Atlantique'          -- PAM Iris
    UNION ALL SELECT 10345, 'DM SOI (974)',         'La Réunion', 'Sud de l''Océan indien'  -- PAM Osiris II
    UNION ALL SELECT 10519, 'DGTM Guyane (973)',    'Guyane', 'Guyane'             -- PAM Cayenne : agrégée avec l'ULAM Guyane
)

SELECT
    manual.control_unit_id AS control_unit_id,
    -- Nom monitorenv à date, résolu ici pour information/debug seulement
    -- (les requêtes consommatrices utilisent leur propre control_units.name
    -- au moment du join, pas cette copie qui peut dater de la dernière
    -- exécution de CE flow).
    toString(coalesce(cu.name, '')) AS control_unit_name_snapshot,
    toString(coalesce(nullIf(manual.nom_ou_ville_origine, ''), coalesce(cu.name, ''))) AS nom_ou_ville_origine,
    toString(coalesce(manual.facade_ref, '')) AS facade_ref,
    toString(coalesce(manual.zone_maritime, '')) AS zone_maritime,
    toString(multiIf(
        startsWith(upper(coalesce(cu.name, '')), 'PAM'), 'PAM',
        startsWith(upper(coalesce(cu.name, '')), 'ULAM'), 'ULAM',
        'AUTRE'
    )) AS unit_type,
    now() AS updated_at
FROM manual_reference manual
LEFT JOIN monitorenv_proxy.control_units cu ON cu.id = manual.control_unit_id;
