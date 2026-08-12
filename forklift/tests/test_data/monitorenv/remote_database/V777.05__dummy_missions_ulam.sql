-- =====================================================================
-- Fixtures MonitorEnv pour les tests du rapport ULAM.
-- ⚠️ IMPORTANT : contrairement à V777.01/V777.02, ce fichier N'EST PAS
-- précédé d'un DELETE FROM -- ces tables (missions, control_units,
-- missions_control_units, control_unit_resources) n'ont pas encore de
-- fixtures de test existantes (aucune ne référence l'ID 999001 vu dans
-- les prints de debug de test_sync_table_from_db_connection.py, qui
-- semble correspondre à un travail en cours non encore committé). On
-- reste donc en INSERT pur, avec un ID de test dédié (999100, distinct
-- de tout ID réel ou d'un éventuel futur 999001) pour ne rien écraser.
--
-- administrations 1 ('Affaires Maritimes') et 2 ('Douane') sont déjà
-- seedées par les vraies migrations monitorenv (V0.060), pas besoin
-- d'en rejouer ici. Vérifié contre le schéma réel (github.com/MTES-MCT/monitorenv) :
-- - missions.unit a été DROP (V0.046) ; mission_type (singulier) a été
--   remplacé par mission_types text[] (V0.072) ; mission_source est
--   NOT NULL sans défaut (V0.054), enum mission_source_type depuis
--   V0.064.1, valeur 'RAPPORT_NAV' ajoutée en V0.168 -- utilisée ici
--   puisque cette mission fictive représente une mission rapportnav.
-- - control_unit_resources.base_id est NOT NULL depuis V0.097 -> ligne
--   bases ajoutée ci-dessous pour satisfaire la FK.
-- =====================================================================

-- Mission de test : 10h en mer du 2025-06-02 08:00 au 18:00 UTC.
INSERT INTO public.missions (id, start_datetime_utc, end_datetime_utc, mission_types, mission_source, facade)
VALUES (999100, '2025-06-02 08:00:00', '2025-06-02 18:00:00', ARRAY['SEA'], 'RAPPORT_NAV', 'MED');

-- Unité de contrôle rattachée à Affaires Maritimes (administration_id=1).
INSERT INTO public.control_units (id, administration_id, name, archived)
VALUES (999100, 1, 'ULAM TEST 999100', false);

INSERT INTO public.missions_control_units (mission_id, control_unit_id)
VALUES (999100, 999100);

-- Base de test, requise par la FK control_unit_resources.base_id (NOT NULL).
INSERT INTO public.bases (id, latitude, longitude, name)
VALUES (999100, 48.39, -4.49, 'Base TEST 999100');

-- 2 moyens de test rattachés à cette unité : un nautique (MER) et un
-- terrestre (TERRE), pour exercer le mapping terrain_category.
INSERT INTO public.control_unit_resources (id, base_id, control_unit_id, name, type)
VALUES
    (999100, 999100, 999100, 'Semi-rigide TEST', 'RIGID_HULL'),
    (999101, 999100, 999100, 'Véhicule TEST', 'CAR');
