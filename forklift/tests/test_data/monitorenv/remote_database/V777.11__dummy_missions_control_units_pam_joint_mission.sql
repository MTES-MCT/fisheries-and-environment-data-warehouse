-- =====================================================================
-- Fixture MonitorEnv pour le rapport PAM+ULAM : rattachement de l'unité
-- PAM de test (V777.10, control_unit_id=999102) à la mission de test
-- 999100 -- en plus de l'unité ULAM déjà rattachée par V777.07, pour
-- simuler une mission conjointe PAM+ULAM. Nécessite V777.05 (missions)
-- et V777.10 (control_units). INSERT pur, additif.
-- =====================================================================
INSERT INTO public.missions_control_units (mission_id, control_unit_id)
VALUES (999100, 999102);
