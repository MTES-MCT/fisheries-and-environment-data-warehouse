-- =====================================================================
-- Fixture MonitorEnv pour le rapport PAM+ULAM (cf. généralisation des 3
-- requêtes rapport_ulam_*.sql -> rapport_pam_ulam_*.sql, discussion en
-- chat) : une 2e unité de contrôle, nommée PAM, rattachée par V777.11 à
-- la MÊME mission de test 999100 que l'unité ULAM TEST 999100
-- (V777.06/07) -- objectif : exercer le cas "mission conjointe PAM+ULAM"
-- (pam_ulam_control_units doit inclure les 2 unités ; mission_units.unit_type
-- retient l'un des 2 types trouvés, cf. commentaire "approximation 1er
-- trouvé" dans les requêtes). Fichier séparé de V777.11 (même précaution
-- d'isolation par table que V777.05/06/07). ID dédié (999102) distinct
-- de tout ID déjà utilisé dans ce scénario (999100/999101 déjà pris par
-- control_units/control_unit_resources). INSERT pur, additif.
-- =====================================================================
INSERT INTO public.control_units (id, administration_id, name, archived)
VALUES (999102, 1, 'PAM TEST 999102', false);
