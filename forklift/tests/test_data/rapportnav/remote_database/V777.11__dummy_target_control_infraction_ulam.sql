-- =====================================================================
-- Fixture rapportnav pour le rapport PAM+ULAM : chaîne cible/contrôle/
-- infraction (target_2 -> control_2 -> infraction_2 -> infraction_natinf_2)
-- sur l'action CONTROL de la mission de test 999100
-- ('99910000-0000-0000-0000-000000000004', cf. V777.08).
-- Même schéma que V777.05__dummy_target_control_infraction_aem_test.sql
-- (déjà marqué non vérifié contre une vraie base) -- réutilisé ici pour
-- exercer action_infractions dans rapport_pam_ulam_action.sql.
-- ⚠️ 'WITHOUT_REPORT' (infraction sans PV) n'est PAS confirmé contre le
-- vrai enum infraction_type -- seul 'WITH_REPORT' l'est (cf. V777.05).
-- Utilisé ici uniquement pour exercer la branche "!= 'WITH_REPORT'" de
-- action_infractions, pas comme une valeur métier garantie exacte.
--
-- Scénario :
--   - 1 cible (target_2) sur l'action CONTROL -> nb_targets = 1
--   - 2 contrôles (control_2) sur cette cible : 1 has_been_done=true,
--     1 has_been_done=false -> nb_controls = 1 (le false est exclu),
--     nb_controls_amount = 1 (amount_of_controls du contrôle fait)
--   - 2 infractions sur le contrôle fait : 1 WITH_REPORT (avec PV),
--     1 WITHOUT_REPORT (traité comme sans PV) -> nb_infractions_avec_pv=1,
--     nb_infractions_sans_pv=1
--   - 1 code NATINF sur l'infraction avec PV -> natinf_codes=[23588]
-- Pas de DELETE : additif uniquement.
-- =====================================================================

INSERT INTO public.target_2 (
    id, action_id, target_type, vessel_name, vessel_type, vessel_size,
    status, main_agent, vessel_identifier, identity_controlled_person,
    start_datetime_utc, end_datetime_utc, source, external_id
) VALUES (
    '99910100-0000-0000-0000-000000000001',
    '99910000-0000-0000-0000-000000000004',
    'VEHICLE', 'Navire Test ULAM', 'FISHING', 10,
    'CONTROLLED', 'Agent Test ULAM', null, null,
    '2025-06-02 09:00:00+00', '2025-06-02 10:00:00+00', 'TEST', null
);

INSERT INTO public.control_2 (
    id, control_type, amount_of_controls, observations, target_id,
    has_been_done
) VALUES
    ('99910200-0000-0000-0000-000000000001', 'ADMINISTRATIVE', 1, 'Contrôle test ULAM fait',
     '99910100-0000-0000-0000-000000000001', true),
    ('99910200-0000-0000-0000-000000000002', 'ADMINISTRATIVE', 1, 'Contrôle test ULAM non fait',
     '99910100-0000-0000-0000-000000000001', false);

INSERT INTO public.infraction_2 (
    id, control_id, infraction_type, observations
) VALUES
    ('99910300-0000-0000-0000-000000000001', '99910200-0000-0000-0000-000000000001',
     'WITH_REPORT', 'Infraction test ULAM avec PV'),
    ('99910300-0000-0000-0000-000000000002', '99910200-0000-0000-0000-000000000001',
     'WITHOUT_REPORT', 'Infraction test ULAM sans PV');

INSERT INTO public.infraction_natinf_2 (
    infraction_id, natinf_code
) VALUES (
    '99910300-0000-0000-0000-000000000001', 23588
);
