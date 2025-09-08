
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
)


SELECT rapportnav_proxy.mission_general_info.mission_id::Int32 as "id", rapportnav_proxy.mission_general_info.consumed_fuel_in_liters::Int32,aggregated_actions_by_missions.cmpt
FROM rapportnav_proxy.mission_general_info
LEFT JOIN aggregated_actions_by_missions on aggregated_actions_by_missions.mission_id = rapportnav_proxy.mission_general_info.mission_id
;