-- =====================================================================
-- Fixture rapportnav pour le rapport PAM+ULAM : rattachement service <->
-- unité de contrôle (rapport_pam_ulam_controle_croise.sql -- la CTE
-- service_units INNER JOIN rapportnav_proxy.service_control_unit ->
-- monitorenv_proxy.control_units -> dim_unit_reference).
-- Schéma confirmé contre le repo cloné rapportnav2 : migration
-- V1.2023.12.07.14.54__service_control_units.sql
-- (service_control_unit(service_id INT, control_unit_id INT), FK sur
-- service_id uniquement -- control_unit_id vit dans une autre base,
-- pas de FK cross-DB).
--
-- Réutilise le service ULAM existant (999001 'ULAM 999', V777.02),
-- rattaché à l'unité ULAM de test (999100, monitorenv V777.06). Ajoute
-- un 2e service pour l'unité PAM de test (999102, monitorenv V777.10) --
-- absent jusqu'ici, aucun service PAM n'existait en fixture.
-- Pas de DELETE : additif uniquement.
-- =====================================================================

INSERT INTO public.service (
    id, name, service_type, created_at, updated_at, created_by, updated_by,
    deleted_at
) VALUES (
    999002, 'PAM TEST 999', 'PAM', '2025-06-10 08:00:00', '2025-06-10 08:00:00',
    999001, 999001, null
);

INSERT INTO public.service_control_unit (service_id, control_unit_id)
VALUES
    (999001, 999100), -- service ULAM 999 <-> unité ULAM TEST 999100
    (999002, 999102);  -- service PAM TEST 999 <-> unité PAM TEST 999102
