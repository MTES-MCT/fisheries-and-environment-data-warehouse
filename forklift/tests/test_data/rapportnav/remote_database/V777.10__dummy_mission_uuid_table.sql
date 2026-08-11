-- =====================================================================
-- Fixture — table `mission` (UUID) côté rapportnav, pour la mission
-- fictive de test AEM (999001). Nécessaire au filtre
-- rm.is_complete_for_stats = 1 de query_aem_par_mission_avec_referentiel_unites.sql
-- (rm.external_id = toString(monitorenv.missions.id)).
-- Colonnes confirmées via MissionModel.kt (rapportnav2, table "mission").
-- Pas de DELETE : additif uniquement.
-- =====================================================================

INSERT INTO public.mission (
    id, service_id, open_by, completed_by, external_id,
    start_datetime_utc, end_datetime_utc, is_deleted, mission_source,
    observations_by_unit, is_complete_for_stats, sources_of_missing_data,
    created_at, updated_at, created_by, updated_by
) VALUES (
    '99999999-9999-9999-9999-999999999999', 999001, 'Test AEM', 'Test AEM',
    '999001',
    '2025-06-10 08:00:00+00', '2025-06-12 18:00:00+00', false, 'MONITORENV',
    'Mission fictive de test AEM', true, null,
    '2025-06-10 08:00:00', '2025-06-12 18:00:00', 999001, 999001
);
