-- =====================================================================
-- Fixture MonitorEnv pour le rapport ULAM : base de test, requise par la
-- FK control_unit_resources.base_id (NOT NULL depuis V0.097). INSERT
-- pur, isolé (cf. commentaire de V777.05).
-- =====================================================================
INSERT INTO public.bases (id, latitude, longitude, name)
VALUES (999100, 48.39, -4.49, 'Base TEST 999100');
