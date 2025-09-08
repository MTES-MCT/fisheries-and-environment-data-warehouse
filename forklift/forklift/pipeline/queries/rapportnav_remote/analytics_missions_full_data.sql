
WITH all_actions AS (
    SELECT rapportnav_proxy.mission_action.id, 
        rapportnav_proxy.mission_action.mission_id as mission_id,
        'from_rapportnav' as source
    FROM rapportnav_proxy.mission_action
    UNION ALL 
    SELECT monitorfish_proxy.mission_actions.id, 
        monitorfish_proxy.mission_actions.mission_id as mission_id,
        'from_monitorfish' as source
    FROM monitorfish_proxy.mission_actions
    UNION ALL
    SELECT monitorenv_proxy.mission_actions.id,
        monitorenv_proxy.mission_actions.id as mission_id,
        'from_monitorenv' as source
    FROM monitorenv_proxy.mission_actions
),
aggregated_actions_by_missions AS (
    SELECT COUNT(*)
    FROM all_actions
    GROUP BY mission_id
)


SELECT rapportnav_proxy.mission_general_info.id, rapportnav_proxy.mission_general_info.mission_id, rapportnav_proxy.mission_general_info.consumed_fuel_in_liters
FROM rapportnav_proxy.mission_general_info
LEFT JOIN aggregated_actions_by_missions on aggregated_actions_by_missions.mission_id = rapportnav_proxy.mission_general_info.mission_id
;