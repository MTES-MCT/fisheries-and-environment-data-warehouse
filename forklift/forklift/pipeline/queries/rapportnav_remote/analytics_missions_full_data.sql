SELECT rapportnav_proxy.mission_action.mission_id,  
FROM rapportnav_proxy.mission_action
LEFT JOIN rapportnav_proxy.mission_general_info on rapportnav_proxy.mission_general_info.mission_id = rapportnav_proxy.mission_action.mission_id
WHERE rapportnav_proxy.mission_action.is_complete_for_stats::int = 1;