-- =====================================================================
-- Fixture rapportnav pour le rapport PAM+ULAM : chaîne cible/contrôle/
-- infraction (target_2 -> control_2 -> infraction_2 -> infraction_natinf_2)
-- sur l'action CONTROL de la mission de test 999100
-- ('99910000-0000-0000-0000-000000000004', cf. V777.08).
-- Schéma ET logique VÉRIFIÉS contre rapportnav2 (repo cloné et inspecté --
-- migration V1.2025.03.18.16.14__target_2_control_2_infraction_2_table.sql,
-- InfractionTypeEnum, CountInfractions.kt) -- cf. commentaire détaillé sur
-- la CTE control_infraction_flags dans rapport_pam_ulam_action.sql.
--
-- Scénario, construit pour distinguer nb_control_types (nb de LIGNES
-- control_2) de nb_controls (SUM(amount_of_controls) -- le champ qui
-- reflète le nombre réel de contrôles, cf. contrainte
-- UNIQUE(control_type, target_id) empêchant 2 lignes du même type sur la
-- même cible) :
--   - 1 cible (target_2) sur l'action CONTROL -> nb_targets = 1
--   - 2 contrôles (control_2) sur cette cible, de types DIFFÉRENTS
--     (contrainte UNIQUE(control_type, target_id)) :
--       - ADMINISTRATIVE, has_been_done=true, amount_of_controls=2
--         (2 contrôles administratifs faits sur cette sortie)
--       - SECURITY, has_been_done=false (jamais fait -> exclu de tout)
--     -> nb_control_types = 1 (seul le fait est compté), nb_controls = 2
--        (la vraie logique métier, PAS un COUNT de lignes)
--   - 3 infractions sur le contrôle ADMINISTRATIVE (fait) : WITH_REPORT
--     (avec PV), WITHOUT_REPORT (sans PV), WAITING (en attente) ->
--     nb_infractions_avec_pv = 2 (amount_of_controls du contrôle, compté
--     une fois par type d'infraction présent -- pas une fois par ligne
--     infraction_2), nb_infractions_sans_pv = 2, nb_infractions_en_attente = 2
--   - 1 code NATINF sur l'infraction avec PV -> natinf_codes=['23588']
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
    ('99910200-0000-0000-0000-000000000001', 'ADMINISTRATIVE', 2, 'Contrôle test ULAM fait (x2)',
     '99910100-0000-0000-0000-000000000001', true),
    ('99910200-0000-0000-0000-000000000002', 'SECURITY', 1, 'Contrôle test ULAM non fait',
     '99910100-0000-0000-0000-000000000001', false);

INSERT INTO public.infraction_2 (
    id, control_id, infraction_type, observations
) VALUES
    ('99910300-0000-0000-0000-000000000001', '99910200-0000-0000-0000-000000000001',
     'WITH_REPORT', 'Infraction test ULAM avec PV'),
    ('99910300-0000-0000-0000-000000000002', '99910200-0000-0000-0000-000000000001',
     'WITHOUT_REPORT', 'Infraction test ULAM sans PV'),
    ('99910300-0000-0000-0000-000000000003', '99910200-0000-0000-0000-000000000001',
     'WAITING', 'Infraction test ULAM en attente');

INSERT INTO public.infraction_natinf_2 (
    infraction_id, natinf_code
) VALUES (
    '99910300-0000-0000-0000-000000000001', '23588'
);
