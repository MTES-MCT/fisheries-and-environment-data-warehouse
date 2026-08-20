-- =====================================================================
-- Fixture rapportnav pour les tests du rapport ULAM : mission_action_resource.
--
-- ⚠️ RISQUE CONNU (déjà signalé) : cette table a été créée le 28/05/2026
-- côté rapportnav2 (V1.2026.05.28.10.00__create_mission_action_resource_table.sql).
-- Si le schéma de test rapportnav_remote utilisé en CI est construit à
-- partir d'une version de rapportnav2 antérieure à cette date, cette
-- fixture échouera avec une erreur "relation does not exist". À vérifier
-- avant d'ajouter ce fichier -- pas de contournement SQL possible, il
-- faut que la migration réelle soit présente dans le schéma de test.
--
-- Scénario :
--   - l'action CONTROL (...0004) est volontairement liée aux 2 moyens
--     (bateau + véhicule) -> nb_resources_on_action=2 pour cette action,
--     donc 2 lignes dans fact_moyen_pam_ulam portant CHACUNE la durée
--     complète de l'action (1h) -- exactement le cas de double comptage
--     documenté dans le commentaire SQL de rapport_pam_ulam_moyen.sql.
--     Un SUM(action_duration_h) naïf sur ces 2 lignes donnerait 2h alors
--     que l'action ne dure réellement qu'1h.
--   - RESOURCES_MAINTENANCE bateau (...0006) -> 1 seul moyen (bateau)
--   - RESOURCES_MAINTENANCE véhicule (...0007) -> 1 seul moyen (véhicule)
--   - TRAINING (...0005) volontairement SANS moyen lié (cas normal, pas
--     toutes les actions mobilisent un moyen)
-- =====================================================================
INSERT INTO public.mission_action_resource (action_id, resource_id)
VALUES
    ('99910000-0000-0000-0000-000000000004', 999100), -- CONTROL <-> bateau
    ('99910000-0000-0000-0000-000000000004', 999101), -- CONTROL <-> véhicule (double comptage volontaire)
    ('99910000-0000-0000-0000-000000000006', 999100), -- entretien bateau <-> bateau
    ('99910000-0000-0000-0000-000000000007', 999101); -- entretien véhicule <-> véhicule
