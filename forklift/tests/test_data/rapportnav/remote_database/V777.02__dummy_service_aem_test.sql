-- =====================================================================
-- Fixture — table `service`, absente du repo jusqu'ici (vérifié sur
-- main ET sur cette branche). Nécessaire pour analytics_missions_full_data.sql
-- (mission_analytics) : la CTE service_detailed dérive entièrement de
-- cette table, et le WHERE final (unite is not null and unite != '')
-- élimine tout si elle est vide -- ce qui était le cas jusqu'ici,
-- indépendamment de ce chantier AEM.
-- Schéma confirmé via ServiceModel.kt (rapportnav2, table "service").
-- Nom choisi pour matcher la regex ULAM de la requête
-- ((?i)(?:ULAM|ulam)[_ ](\d+)) et produire une valeur "unite" non vide.
-- Pas de DELETE : additif uniquement.
-- =====================================================================

INSERT INTO public.service (
    id, name, service_type, created_at, updated_at, created_by, updated_by,
    deleted_at
) VALUES (
    999001, 'ULAM 999', 'ULAM', '2025-06-10 08:00:00', '2025-06-10 08:00:00',
    999001, 999001, null
);
