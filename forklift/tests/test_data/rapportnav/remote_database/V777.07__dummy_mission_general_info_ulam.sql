-- =====================================================================
-- Fixture rapportnav pour les tests du rapport ULAM : mission_general_info
-- pour la mission de test 999100 (cf. monitorenv/V777.03__dummy_missions_ulam.sql).
-- ⚠️ INSERT pur, PAS de DELETE FROM (contrairement à V777.06 qui, lui,
-- vide toute la table) -- sinon on écraserait les 3 lignes déjà en place
-- (mission_id 12, 13, 20).
--
-- Valeurs choisies pour exercer les indicateurs "sortie terrain",
-- "renfort", "JDP", "mission armée", "mission conjointe" :
--   mission_report_type = FIELD_REPORT (compte comme "avec sortie terrain")
--   reinforcement_type  = JDP (compte comme "participation JDP")
--   jdp_type            = ONBOARD
--   is_mission_armed    = true
--   is_with_interministerial_service = true (complété par
--     inter_ministerial_service, cf. V777.10)
-- =====================================================================
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
) VALUES (
    999100,
    999100,
    42.5,
    100.0,
    80.0,
    NULL,
    1,
    true,
    true,
    'FIELD_REPORT',
    'JDP',
    9,
    'ONBOARD',
    '99910000-0000-0000-0000-000000000000',
    '2025-06-02 07:00:00',
    '2025-06-02 19:00:00',
    1,
    1
);
