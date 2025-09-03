DELETE FROM public.mission_general_info;

INSERT INTO public.mission_general_info (
    id,
    mission_id,
    distance_in_nautical_miles,
    consumed_go_in_liters,
    consumed_fuel_in_liters,
    service_id,
    nbr_of_recognized_vessel,
    is_with_interministerial_service,
    is_mission_armed,
    mission_report_type,
    reinforcement_type,
    nb_hour_at_sea,
    jdp_type,
    mission_id_uuid,
    created_at,
    updated_at,
    created_by,
    updated_by
) VALUES
    ('1', 11, 120.5, 300.0, 500.0, 10, 2, true, false, 'TypeA', 'ReinforcementA', 24, 'JDP-A', '11111111-1111-1111-1111-111111111111', '2025-08-01 10:00:00', '2025-08-01 12:00:00', 1, 2),
    ('2', 102, 80.0, 200.0, 350.0, 11, 1, false, true, 'TypeB', 'ReinforcementB', 12, 'JDP-B', '22222222-2222-2222-2222-222222222222', '2025-08-02 09:00:00', '2025-08-02 11:00:00', 2, 3),
    ('3', 103, 150.75, 400.0, 600.0, 12, 3, true, true, 'TypeC', 'ReinforcementC', 36, 'JDP-C', '33333333-3333-3333-3333-333333333333', '2025-08-03 08:00:00', '2025-08-03 10:00:00', 3, 4);