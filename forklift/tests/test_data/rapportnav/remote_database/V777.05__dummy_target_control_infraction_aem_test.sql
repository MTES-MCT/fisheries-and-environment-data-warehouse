-- =====================================================================
-- Fixture de test AEM — chaîne cible/contrôle/infraction pour l'action
-- CONTROL de la mission fictive 999001 (schéma confirmé dans
-- ControlModel.kt/InfractionModel.kt/TargetModel.kt côté rapportnav2,
-- plus tôt dans la conversation).
-- ⚠️ Colonnes reconstruites de mémoire à partir des modèles JPA,
-- PAS testées contre une vraie base -- si Flyway échoue sur une colonne,
-- comparer avec target2/v2/TargetModel.kt, control/v2/ControlModel.kt,
-- infraction/v2/InfractionModel.kt dans rapportnav2.
-- Pas de DELETE : additif uniquement.
-- =====================================================================

INSERT INTO public.target_2 (
    id, action_id, target_type, vessel_name, vessel_type, vessel_size,
    status, main_agent, vessel_identifier, identity_controlled_person,
    start_datetime_utc, end_datetime_utc, source, external_id
) VALUES (
    '99999902-0000-0000-0000-000000000001',
    '99999901-0000-0000-0000-000000000012',
    'VEHICLE', 'Navire Test AEM', 'FISHING', 12,
    'CONTROLLED', 'Agent Test', null, null,
    '2025-06-12 12:00:00+00', '2025-06-12 13:30:00+00', 'TEST', null
);

INSERT INTO public.control_2 (
    id, control_type, amount_of_controls, observations, target_id,
    has_been_done
) VALUES (
    '99999903-0000-0000-0000-000000000001',
    'ADMINISTRATIVE', 1, 'Contrôle test AEM',
    '99999902-0000-0000-0000-000000000001', true
);

INSERT INTO public.infraction_2 (
    id, control_id, infraction_type, observations
) VALUES (
    '99999904-0000-0000-0000-000000000001',
    '99999903-0000-0000-0000-000000000001',
    'WITH_REPORT', 'Infraction test AEM'
);

INSERT INTO public.infraction_natinf_2 (
    infraction_id, natinf_code
) VALUES (
    '99999904-0000-0000-0000-000000000001', 23588
);
