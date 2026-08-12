-- =====================================================================
-- Fixture rapportnav pour les tests du rapport ULAM : mission_action pour
-- la mission de test 999100.
-- ⚠️ INSERT pur, PAS de DELETE FROM (contrairement à V777.05 qui vide
-- toute la table) -- sinon on écraserait les actions des missions 12,
-- 14, 761 déjà en place. IDs choisis pour ne rien écraser.
--
-- Scénario construit pour donner des totaux faciles à vérifier :
--   3 x STATUS  -> NAVIGATING 08h-10h, ANCHORED 10h-14h, NAVIGATING 14h-fin (18h)
--                  => computed_hours_at_sea (ANCHORED+NAVIGATING) = 10h
--                  => heures_navigation_hypothese_moteur (NAVIGATING seul) = 6h
--   1 x CONTROL           09h-10h (1h)
--   1 x TRAINING          11h-14h (3h), training_type='SURVIE'
--   1 x RESOURCES_MAINTENANCE (bateau) 15h-17h (2h), resource_type='RIGID_HULL'
--   1 x RESOURCES_MAINTENANCE (véhicule) 15h30-17h (1.5h), resource_type='CAR'
--
-- Seules les colonnes utiles aux requêtes ULAM sont renseignées ; le
-- reste (colonnes métier non utilisées ici) part à NULL par défaut,
-- comme dans V777.05.
-- =====================================================================
INSERT INTO public.mission_action (
    id,
    mission_id,
    action_type,
    start_datetime_utc,
    end_datetime_utc,
    status,
    training_type,
    resource_type,
    nbr_of_hours,
    is_complete_for_stats,
    reason
) VALUES
    ('99910000-0000-0000-0000-000000000001', 999100, 'STATUS', '2025-06-02 08:00:00', NULL, 'NAVIGATING', NULL, NULL, NULL, true, NULL),
    ('99910000-0000-0000-0000-000000000002', 999100, 'STATUS', '2025-06-02 10:00:00', NULL, 'ANCHORED',   NULL, NULL, NULL, true, NULL),
    ('99910000-0000-0000-0000-000000000003', 999100, 'STATUS', '2025-06-02 14:00:00', NULL, 'NAVIGATING', NULL, NULL, NULL, true, NULL),
    ('99910000-0000-0000-0000-000000000004', 999100, 'CONTROL', '2025-06-02 09:00:00', '2025-06-02 10:00:00', NULL, NULL, NULL, NULL, true, NULL),
    ('99910000-0000-0000-0000-000000000005', 999100, 'TRAINING', '2025-06-02 11:00:00', '2025-06-02 14:00:00', NULL, 'SURVIE', NULL, NULL, true, NULL),
    ('99910000-0000-0000-0000-000000000006', 999100, 'RESOURCES_MAINTENANCE', '2025-06-02 15:00:00', '2025-06-02 17:00:00', NULL, NULL, 'RIGID_HULL', NULL, true, NULL),
    ('99910000-0000-0000-0000-000000000007', 999100, 'RESOURCES_MAINTENANCE', '2025-06-02 15:30:00', '2025-06-02 17:00:00', NULL, NULL, 'CAR', NULL, true, NULL);
