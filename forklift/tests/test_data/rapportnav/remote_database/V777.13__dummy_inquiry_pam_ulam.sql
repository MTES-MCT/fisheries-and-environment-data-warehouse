-- =====================================================================
-- Fixture rapportnav pour le rapport PAM+ULAM : rapportnav_proxy.inquiry
-- (rapport_pam_ulam_controle_croise.sql -- alimente
-- fact_controle_croise_pam_ulam, INNER JOIN sur service_control_unit,
-- cf. V777.12). Schéma confirmé via InquiryModel.kt (rapportnav2) +
-- migration V1.2025.07.02.08.56__update_cross_control_table_to_inquiry_table.sql
-- (ex-table cross_control, renommée -- cf. commentaire détaillé dans
-- rapport_pam_ulam_controle_croise.sql).
--
-- establishment_id volontairement NULL sur les 3 lignes : colonne en
-- @OneToOne avec FK réelle vers establishment (contrairement à
-- vessel_id, simple INT sans FK) -- pas de fixture establishment créée
-- pour ce chantier, donc type_cible='ETABLISSEMENT' n'est pas couvert
-- ici (seul 'NAVIRE' l'est, via vessel_id). Couverture à compléter si
-- besoin, pas un bloquant CI.
-- type volontairement NULL : colonne non exploitée par
-- rapport_pam_ulam_controle_croise.sql, et aucune valeur d'enum connue
-- avec certitude (pas trouvée dans InquiryModel.kt ni les migrations
-- cross_control/inquiry disponibles côté repo cloné) -- ne pas deviner.
--
-- Scénario, 1 ULAM + 2 PAM pour varier statut/origine/conclusion :
--   - ULAM (service 999001), CLOSED, OPPORTUNITY_CONTROL, conclu WITH_REPORT
--   - PAM (service 999002), CLOSED, FOLLOW_UP_CONTROL, conclu NO_FOLLOW_UP
--   - PAM (service 999002), IN_PROGRESS, CNSP_REPORTING, pas encore conclu
--     (end_datetime_utc et conclusion NULL -- contrôle croisé toujours ouvert)
-- Pas de DELETE : additif uniquement.
-- =====================================================================

INSERT INTO public.inquiry (
    id, type, status, origin, agent_id, vessel_id, service_id,
    start_datetime_utc, end_datetime_utc, conclusion, mission_id,
    mission_id_uuid, is_signed_by_inspector, establishment_id,
    created_at, updated_at, created_by, updated_by
) VALUES
    ('99910500-0000-0000-0000-000000000001', null, 'CLOSED', 'OPPORTUNITY_CONTROL',
     '999001', 999100, 999001,
     '2025-06-02 09:00:00+00', '2025-06-02 10:30:00+00', 'WITH_REPORT', 999100,
     '99910000-0000-0000-0000-000000000000', true, null,
     '2025-06-02 10:30:00+00', '2025-06-02 10:30:00+00', 999001, 999001),
    ('99910500-0000-0000-0000-000000000002', null, 'CLOSED', 'FOLLOW_UP_CONTROL',
     '999002', 999102, 999002,
     '2025-06-03 08:00:00+00', '2025-06-03 09:00:00+00', 'NO_FOLLOW_UP', null,
     null, true, null,
     '2025-06-03 09:00:00+00', '2025-06-03 09:00:00+00', 999002, 999002),
    ('99910500-0000-0000-0000-000000000003', null, 'IN_PROGRESS', 'CNSP_REPORTING',
     '999002', 999103, 999002,
     '2025-06-04 07:00:00+00', null, null, null,
     null, null, null,
     '2025-06-04 07:00:00+00', '2025-06-04 07:00:00+00', 999002, 999002);
