-- =====================================================================
-- Fixture — table `mission` (UUID), sans rapport avec missions_aem.sql.
-- ⚠️ Aucun fichier existant ne peuplait cette table (rapportnav n'a pas
-- de données vendorées) : le test de sync
-- rapportnav_proxy.mission -> rapportnav.mission échouait probablement
-- déjà sur main, faute de ligne à synchroniser (len(df) > 0 jamais
-- vérifié). Colonnes confirmées via MissionModel.kt (rapportnav2, table
-- "mission" -- à ne pas confondre avec "mission_action").
-- Pas de DELETE : additif uniquement.
-- =====================================================================

INSERT INTO public.mission (
    id, service_id, open_by, completed_by, external_id,
    start_datetime_utc, end_datetime_utc, is_deleted, mission_source,
    observations_by_unit, is_complete_for_stats, sources_of_missing_data,
    created_at, updated_at, created_by, updated_by
) VALUES (
    '99999999-9999-9999-9999-999999999999', 999001, 'Test AEM', 'Test AEM', null,
    '2025-06-10 08:00:00+00', '2025-06-12 18:00:00+00', false, 'MONITORENV',
    'Mission fictive de test AEM', true, null,
    '2025-06-10 08:00:00', '2025-06-12 18:00:00', 999001, 999001
);

