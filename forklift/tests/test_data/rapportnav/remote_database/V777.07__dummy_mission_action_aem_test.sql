-- =====================================================================
-- Fixture de test AEM — mission fictive 999001, une action par
-- indicateur nav pour vérifier le remplissage complet de missions_aem.sql.
-- Colonnes copiées de V777.05__dummy_mission_actions.sql.
-- Générée programmatiquement (chaque valeur mappée par nom de colonne)
-- pour éviter tout décalage de position -- une version manuelle
-- précédente avait plusieurs lignes désalignées (VALUES lists must all
-- be the same length), détecté par test local.
-- Pas de DELETE : additif uniquement, id 999001 réservé aux tests AEM.
-- =====================================================================

INSERT INTO public.mission_action (
    id, mission_id, action_type, start_datetime_utc, end_datetime_utc, observations, is_complete_for_stats, latitude, longitude, detected_pollution, pollution_observed_by_authorized_agent, diversion_carried_out, simple_brewing_operation, anti_pol_device_deployed, control_method, vessel_identifier, vessel_type, vessel_size, identity_controlled_person, nb_of_intercepted_vessels, nb_of_intercepted_migrants, nb_of_suspected_smugglers, is_vessel_rescue, is_person_rescue, is_vessel_noticed, is_vessel_towed, is_in_srr_or_followed_by_cross_mrcc, number_persons_rescued, number_of_deaths, operation_follows_defrep, location_description, is_migration_rescue, nb_vessels_tracked_without_intervention, nb_assisted_vessels_returning_to_shore, status, reason
) VALUES
    -- 1.1 : sauvetage hors migration
    ('99999901-0000-0000-0000-000000000001', 999001, 'RESCUE', '2025-06-10 09:00:00+00', '2025-06-10 11:00:00+00', 'Test AEM 1.1', true, 47.2, -3.0, null, null, null, null, null, null, null, null, null, null, null, null, null, false, true, null, null, null, 3, 0, null, 'Large de Lorient', false, null, null, null, null),

    -- 1.2 : sauvetage migration (SAR migrants)
    ('99999901-0000-0000-0000-000000000002', 999001, 'RESCUE', '2025-06-10 12:00:00+00', '2025-06-10 14:00:00+00', 'Test AEM 1.2', true, 43.3, 5.4, null, null, null, null, null, null, null, null, null, null, null, null, null, false, true, null, null, null, 5, 0, null, 'Large de Marseille', true, 1, 1, null, null),

    -- 2 : assistance navire (ANED)
    ('99999901-0000-0000-0000-000000000003', 999001, 'RESCUE', '2025-06-10 15:00:00+00', '2025-06-10 16:30:00+00', 'Test AEM 2', true, 47.2, -3.0, null, null, null, null, null, null, null, null, null, null, null, null, null, true, false, true, true, null, null, null, null, null, false, null, null, null, null),

    -- 3.4 : immigration illégale
    ('99999901-0000-0000-0000-000000000004', 999001, 'ILLEGAL_IMMIGRATION', '2025-06-10 17:00:00+00', '2025-06-10 19:00:00+00', 'Test AEM 3.4', true, 43.3, 5.4, null, null, null, null, null, null, null, null, null, null, 1, 4, 1, null, null, null, null, null, null, null, null, null, null, null, null, null, null),

    -- 4.2 : pollution (côté nav)
    ('99999901-0000-0000-0000-000000000005', 999001, 'ANTI_POLLUTION', '2025-06-11 08:00:00+00', '2025-06-11 10:00:00+00', 'Test AEM 4.2', true, 47.2, -3.0, true, true, true, true, true, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null),

    -- 5.1 : sûreté maritime (VIGIMER)
    ('99999901-0000-0000-0000-000000000006', 999001, 'VIGIMER', '2025-06-11 11:00:00+00', '2025-06-11 13:00:00+00', 'Test AEM 5.1a', true, 47.2, -3.0, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null),

    -- 5.1 : sûreté maritime (BAAEM_PERMANENCE)
    ('99999901-0000-0000-0000-000000000007', 999001, 'BAAEM_PERMANENCE', '2025-06-11 14:00:00+00', '2025-06-11 16:00:00+00', 'Test AEM 5.1b', true, 47.2, -3.0, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null),

    -- 5.3/5.4 : ordre public (PUBLIC_ORDER)
    ('99999901-0000-0000-0000-000000000008', 999001, 'PUBLIC_ORDER', '2025-06-11 17:00:00+00', '2025-06-11 18:00:00+00', 'Test AEM 5.3a', true, 47.2, -3.0, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null),

    -- 5.3/5.4 : ordre public (NAUTICAL_EVENT)
    ('99999901-0000-0000-0000-000000000009', 999001, 'NAUTICAL_EVENT', '2025-06-11 18:30:00+00', '2025-06-11 19:30:00+00', 'Test AEM 5.3b', true, 47.2, -3.0, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null),

    -- 7.1 : heures de mer -- séquence STATUS (NAVIGATING puis ANCHORED)
    ('99999901-0000-0000-0000-000000000010', 999001, 'STATUS', '2025-06-12 06:00:00+00', null, 'Test AEM 7.1 debut navigation', true, 47.2, -3.0, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, 'NAVIGATING', null),

    -- 
    ('99999901-0000-0000-0000-000000000011', 999001, 'STATUS', '2025-06-12 10:00:00+00', null, 'Test AEM 7.1 ancrage', true, 47.2, -3.0, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, 'ANCHORED', null),

    -- 7.4 : contrôle en mer (chaîne target_2/control_2/infraction_2 dans le fichier rapportnav_V777.09)
    ('99999901-0000-0000-0000-000000000012', 999001, 'CONTROL', '2025-06-12 12:00:00+00', '2025-06-12 13:30:00+00', 'Test AEM 7.4', true, 47.2, -3.0, null, null, null, null, null, 'SEA', null, 'FISHING', null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null)
;
