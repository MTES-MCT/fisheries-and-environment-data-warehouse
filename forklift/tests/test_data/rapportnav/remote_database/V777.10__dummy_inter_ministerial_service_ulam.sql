-- =====================================================================
-- Fixture rapportnav pour les tests du rapport ULAM : inter_ministerial_service.
-- Simule une mission conjointe entre Affaires Maritimes (administration_id=1,
-- déjà seedée côté monitorenv V0.060) et Douane (administration_id=2),
-- pour exercer nb_intermin_administrations = 2 sur la mission 999100.
-- =====================================================================
INSERT INTO public.inter_ministerial_service (id, administration_id, control_unit_id, mission_general_info_id)
VALUES
    (999100, 1, 999100, 999100),
    (999101, 2, 999100, 999100);
