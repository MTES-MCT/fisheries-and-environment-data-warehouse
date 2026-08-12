-- =====================================================================
-- Fixture MonitorEnv pour le rapport ULAM : unité de contrôle de test
-- rattachée à Affaires Maritimes (administration_id=1, déjà seedée par
-- V0.060). INSERT pur, isolé de V777.05 (cf. commentaire de ce fichier).
-- =====================================================================
INSERT INTO public.control_units (id, administration_id, name, archived)
VALUES (999100, 1, 'ULAM TEST 999100', false);
