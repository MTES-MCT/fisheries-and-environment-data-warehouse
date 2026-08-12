-- =====================================================================
-- Fixture MonitorEnv pour le rapport ULAM : 2 moyens de test rattachés à
-- l'unité de test, un nautique (MER) et un terrestre (TERRE), pour
-- exercer le mapping terrain_category. Nécessite V777.06 (control_units)
-- et V777.08 (bases). INSERT pur, isolé (cf. commentaire de V777.05).
-- =====================================================================
INSERT INTO public.control_unit_resources (id, base_id, control_unit_id, name, type)
VALUES
    (999100, 999100, 999100, 'Semi-rigide TEST', 'RIGID_HULL'),
    (999101, 999100, 999100, 'Véhicule TEST', 'CAR');
