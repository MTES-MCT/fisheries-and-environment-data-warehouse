-- =====================================================================
-- Fixture MonitorEnv pour le rapport ULAM : rattachement mission <-> unité
-- de contrôle de test. Nécessite V777.05 (missions) et V777.06
-- (control_units). INSERT pur, isolé (cf. commentaire de V777.05).
-- =====================================================================
INSERT INTO public.missions_control_units (mission_id, control_unit_id)
VALUES (999100, 999100);
