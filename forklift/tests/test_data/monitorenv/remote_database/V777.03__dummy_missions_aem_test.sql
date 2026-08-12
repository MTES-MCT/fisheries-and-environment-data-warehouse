-- =====================================================================
-- Fixture de test AEM — mission fictive 999001 côté MonitorEnv (source
-- d'unité utilisée par missions_aem.sql), avec une ULAM fictive.
-- ⚠️ Schéma confirmé via les fixtures propres au pipeline monitorenv
-- (missions : id, mission_types, open_by, observations_cacem, facade,
-- start_datetime_utc, end_datetime_utc, created_at_utc, updated_at_utc,
-- completed_by, deleted, mission_source, geom ; control_units : id,
-- administration_id, name).
-- Pas de DELETE : additif uniquement, ids 999001 réservés aux tests AEM,
-- geom = NULL (non utilisée par missions_aem.sql).
-- =====================================================================

INSERT INTO public.missions (
    id, mission_types, open_by, observations_cacem, facade,
    start_datetime_utc, end_datetime_utc, created_at_utc, updated_at_utc,
    completed_by, deleted, mission_source, geom
) VALUES (
    999001, '{SEA}', 'Test AEM', 'Mission fictive de test AEM', 'NAMO',
    '2025-06-10 08:00:00', '2025-06-12 18:00:00',
    '2025-06-10 08:00:00', '2025-06-12 18:00:00',
    'Test AEM', false, 'MONITORENV', NULL
);

-- Unité fictive de test, préfixe 'ULAM' pour être classée service_type
-- = 'ULAM' par missions_aem.sql (multiIf sur startsWith(upper(name),...)).
INSERT INTO public.control_units (
    id, administration_id, name
) VALUES (
    999001, 1005, 'ULAM 99 TEST AEM'
);

INSERT INTO public.missions_control_units (
    mission_id, control_unit_id
) VALUES (
    999001, 999001
);
