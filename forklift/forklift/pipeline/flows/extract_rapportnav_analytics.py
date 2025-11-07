from pathlib import Path

import prefect
from prefect import Flow, case, unmapped

from forklift.pipeline.shared_tasks.rapportnav import fetch_rapportnav_api, extract_missions_ids
from forklift.pipeline.shared_tasks.control_flow import check_flow_not_running
from forklift.pipeline.shared_tasks.generic import (
    create_database_if_not_exists,
)


with Flow("RapportNavAnalytics") as flow:    
    logger = prefect.context.get("logger")

    flow_not_running = check_flow_not_running()
    with case(flow_not_running, True):
    
        create_database = create_database_if_not_exists("rapportnav")
        mission_ids = extract_missions_ids()
        for report_type in ['aem', 'patrol']:
            df = fetch_rapportnav_api.map(
                path=f'analytics/v1/{report_type}',
                missions_ids=unmapped(extract_missions_ids)
            )


flow.file_name = Path(__file__).name
