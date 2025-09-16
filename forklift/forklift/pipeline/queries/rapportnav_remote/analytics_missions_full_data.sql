
WITH all_actions AS (
    SELECT rapportnav_proxy.mission_action.mission_id::Int32 as mission_id,
        'from_rapportnav' as source
    FROM rapportnav_proxy.mission_action
    UNION ALL 
    SELECT monitorfish_proxy.mission_actions.mission_id::Int32 as mission_id,
        'from_monitorfish' as source
    FROM monitorfish_proxy.mission_actions
),
aggregated_actions_by_missions AS (
    SELECT mission_id, COUNT(*) as cmpt
    FROM all_actions
    GROUP BY mission_id
),
service_detailed AS (
    SELECT
        id,
        name,
        CASE
            WHEN LOWER(name) LIKE '%pam%'  THEN 'PAM'
            ELSE 'ULAM'
        END as service_type,
        CASE
            WHEN name LIKE '% A%'  THEN 'A'
            WHEN name LIKE '% B%'  THEN 'B'
            ELSE null
        END as "bordee",
        IF(
            extract(name, 'PAM\\s+(.+?)\\s+[AB]') != '',
            extract(name, 'PAM\\s+(.+?)\\s+[AB]'),
            extract(name, '(?i)(?:ULAM|ulam)[_ ](\\d+)')
        ) AS "unite"
    FROM rapportnav_proxy.service
)


SELECT monitorenv_proxy.missions.id as "id", 
    rapportnav_proxy.mission_general_info.consumed_fuel_in_liters as "consommation_fuel",
    aggregated_actions_by_missions.cmpt as "nombre_action_par_mission",
    service_detailed.service_type,
    service_detailed.bordee,
    service_detailed.unite,
    service_detailed.name
FROM monitorenv_proxy.missions
LEFT JOIN rapportnav_proxy.mission_general_info on rapportnav_proxy.mission_general_info.mission_id = monitorenv_proxy.missions.id
LEFT JOIN aggregated_actions_by_missions on aggregated_actions_by_missions.mission_id = monitorenv_proxy.missions.id
LEFT JOIN service_detailed on service_detailed.id = rapportnav_proxy.mission_general_info.service_id
;