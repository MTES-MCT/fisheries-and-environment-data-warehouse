-- =====================================================================
-- Fixture de test AEM — mission_actions pour la mission fictive 999001,
-- couvre l'indicateur 4.3 (police des pêches). completion='COMPLETED'
-- requis par missions_aem.sql (filtre explicite).
-- ⚠️ Colonnes NOT NULL non revérifiées indépendamment dans cette
-- session -- à ajuster si Flyway échoue.
-- Pas de DELETE : additif uniquement.
-- =====================================================================

INSERT INTO public.mission_actions (
    id, mission_id, action_type, action_datetime_utc, action_end_datetime_utc,
    completion, infractions, seizure_and_diversion, species_quantity_seized, flag_state, user_trigram
) VALUES (
    999001, 999001, 'SEA_CONTROL',
    '2025-06-12 14:00:00+00', '2025-06-12 15:00:00+00',
    'COMPLETED',
    '[{"natinf": 27692, "infractionType": "WITH_RECORD"}]'::jsonb,
    true, 42.5, 'FRA', 'ABC'
);
