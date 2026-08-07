-- =====================================================================
-- Fixture de test AEM — mission_general_info pour la mission fictive
-- 999001 (nbr_of_recognized_vessel alimente l'indicateur 7.3).
-- Colonnes copiées de V777.06__dummy_mission_general_info.sql.
-- Pas de DELETE : additif uniquement.
-- =====================================================================

INSERT INTO public.mission_general_info (
    id, mission_id, distance_in_nautical_miles, consumed_go_in_liters,
    consumed_fuel_in_liters, service_id, nbr_of_recognized_vessel,
    is_with_interministerial_service, is_mission_armed, mission_report_type,
    reinforcement_type, nb_hour_at_sea, jdp_type, mission_id_uuid,
    created_at, updated_at, created_by, updated_by
) VALUES
    ('999001', 999001, 42.0, 100.0, 150.0, 999001, 7,
     false, false, 'TypeTestAEM', 'ReinforcementTestAEM', 30, 'JDP-TEST',
     '99999999-9999-9999-9999-999999999999',
     '2025-06-10 08:00:00', '2025-06-12 18:00:00', 999001, 999001)
;
