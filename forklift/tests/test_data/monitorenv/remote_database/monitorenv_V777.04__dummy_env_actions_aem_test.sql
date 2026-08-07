-- =====================================================================
-- Fixture de test AEM — env_actions + themes_env_actions pour la mission
-- fictive 999001, couvrant 3.3 (espèces protégées, thème 103), 4.1
-- (surveillance hors rejets illicites), 4.2 (pollution, thème 19/102),
-- 4.4 (biens culturels, thème 104/165). Structure JSON de `value`
-- reprise de celle exploitée par missions_aem.sql (vehicleType,
-- actionNumberOfControls, infractions[{natinf, infractionType}]).
-- ⚠️ Colonnes NOT NULL de env_actions non revérifiées indépendamment
-- dans cette session -- à ajuster si Flyway échoue.
-- Pas de DELETE : additif uniquement.
-- =====================================================================

INSERT INTO public.env_actions (
    id, mission_id, action_type, action_start_datetime_utc,
    action_end_datetime_utc, value
) VALUES
    -- 3.3 : trafic espèces protégées (thème 103)
    ('99999905-0000-0000-0000-000000000001', 999001, 'CONTROL',
     '2025-06-10 20:00:00+00', '2025-06-10 22:00:00+00',
     '{"vehicleType": "VESSEL", "actionNumberOfControls": 1, "infractions": []}'),

    -- 4.1 : surveillance hors rejets illicites (sans thème 19/102)
    ('99999905-0000-0000-0000-000000000002', 999001, 'SURVEILLANCE',
     '2025-06-11 06:00:00+00', '2025-06-11 07:00:00+00',
     '{"vehicleType": "VESSEL"}'),

    -- 4.2 côté env : pollution (thème 19), avec infraction + PV
    ('99999905-0000-0000-0000-000000000003', 999001, 'CONTROL',
     '2025-06-11 20:00:00+00', '2025-06-11 21:00:00+00',
     '{"vehicleType": "VESSEL", "actionNumberOfControls": 1, "infractions": [{"natinf": [1234], "infractionType": "WITH_REPORT"}]}'),

    -- 4.4 : biens culturels maritimes, police (thème 104)
    ('99999905-0000-0000-0000-000000000004', 999001, 'CONTROL',
     '2025-06-12 07:00:00+00', '2025-06-12 07:30:00+00',
     '{"vehicleType": "VESSEL"}'),

    -- 4.4 : biens culturels maritimes, opération scientifique (thème 165)
    ('99999905-0000-0000-0000-000000000005', 999001, 'SURVEILLANCE',
     '2025-06-12 08:00:00+00', '2025-06-12 09:00:00+00',
     '{}')
;

INSERT INTO public.themes_env_actions (
    env_actions_id, themes_id
) VALUES
    ('99999905-0000-0000-0000-000000000001', 103),
    ('99999905-0000-0000-0000-000000000003', 19),
    ('99999905-0000-0000-0000-000000000004', 104),
    ('99999905-0000-0000-0000-000000000005', 165)
;
