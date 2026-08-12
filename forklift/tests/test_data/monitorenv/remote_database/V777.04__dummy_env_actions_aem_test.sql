-- =====================================================================
-- Fixture de test AEM — env_actions + themes_env_actions pour la mission
-- fictive 999001, couvrant 3.3 (espèces protégées, thème 999103), 4.1
-- (surveillance hors rejets illicites), 4.2 (pollution, thème 99919/102),
-- 4.4 (biens culturels, thème 999104/999165). Structure JSON de `value`
-- reprise de celle exploitée par missions_aem.sql (vehicleType,
-- actionNumberOfControls, infractions[{natinf, infractionType}]).
--
-- ⚠️ IMPORTANT : themes.id N'EST PAS une valeur stable de schéma --
-- cette table est peuplée dynamiquement à partir de control_plan_themes/
-- control_plan_sub_themes (données de plan de contrôle, réimportées
-- chaque année, cf. V666.0__Reset_themes_and_tags.sql côté monitorenv).
-- Les ids 999103/99919/102/999104/999165 utilisés dans missions_aem.sql ne sont
-- donc PAS garantis stables entre environnements ni dans le temps --
-- à signaler à l'équipe : la requête de production devrait idéalement
-- résoudre ces thèmes par nom plutôt que par id en dur.
-- Pour un test isolé et reproductible, on insère ici nos propres lignes
-- `themes` avec ces ids explicites plutôt que de dépendre de données
-- dynamiquement importées (potentiellement absentes en environnement
-- de test frais).
-- =====================================================================

INSERT INTO public.themes (id, name, started_at, ended_at) VALUES
    (999103, 'Test AEM - Espèces protégées', '2023-01-01 00:00:00', '2099-12-31 23:59:59'),
    (99919,  'Test AEM - Pollution 99919', '2023-01-01 00:00:00', '2099-12-31 23:59:59'),
    (999102, 'Test AEM - Pollution 102', '2023-01-01 00:00:00', '2099-12-31 23:59:59'),
    (999104, 'Test AEM - Biens culturels', '2023-01-01 00:00:00', '2099-12-31 23:59:59'),
    (999165, 'Test AEM - Biens culturels (opérations scientifiques)', '2023-01-01 00:00:00', '2099-12-31 23:59:59')
;
SELECT setval('themes_id_seq', (SELECT MAX(id) FROM public.themes));

INSERT INTO public.env_actions (
    id, mission_id, action_type, action_start_datetime_utc,
    action_end_datetime_utc, value
) VALUES
    -- 3.3 : trafic espèces protégées (thème 999103)
    ('99999905-0000-0000-0000-000000000001', 999001, 'CONTROL',
     '2025-06-10 20:00:00+00', '2025-06-10 22:00:00+00',
     '{"vehicleType": "VESSEL", "actionNumberOfControls": 1, "infractions": []}'),

    -- 4.1 : surveillance hors rejets illicites (sans thème 99919/102)
    ('99999905-0000-0000-0000-000000000002', 999001, 'SURVEILLANCE',
     '2025-06-11 06:00:00+00', '2025-06-11 07:00:00+00',
     '{"vehicleType": "VESSEL"}'),

    -- 4.2 côté env : pollution (thème 99919), avec infraction + PV
    ('99999905-0000-0000-0000-000000000003', 999001, 'CONTROL',
     '2025-06-11 20:00:00+00', '2025-06-11 21:00:00+00',
     '{"vehicleType": "VESSEL", "actionNumberOfControls": 1, "infractions": [{"natinf": [1234], "infractionType": "WITH_REPORT"}]}'),

    -- 4.4 : biens culturels maritimes, police (thème 999104)
    ('99999905-0000-0000-0000-000000000004', 999001, 'CONTROL',
     '2025-06-12 07:00:00+00', '2025-06-12 07:30:00+00',
     '{"vehicleType": "VESSEL"}'),

    -- 4.4 : biens culturels maritimes, opération scientifique (thème 999165)
    ('99999905-0000-0000-0000-000000000005', 999001, 'SURVEILLANCE',
     '2025-06-12 08:00:00+00', '2025-06-12 09:00:00+00',
     '{}')
;

INSERT INTO public.themes_env_actions (
    env_actions_id, themes_id
) VALUES
    ('99999905-0000-0000-0000-000000000001', 999103),
    ('99999905-0000-0000-0000-000000000003', 99919),
    ('99999905-0000-0000-0000-000000000004', 999104),
    ('99999905-0000-0000-0000-000000000005', 999165)
;
