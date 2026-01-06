from pathlib import Path

import prefect
from prefect import Flow, case, unmapped

from forklift.pipeline.shared_tasks import rapportnav
from forklift.pipeline.shared_tasks.control_flow import check_flow_not_running
from forklift.pipeline.shared_tasks.generic import (
    create_database_if_not_exists,
    drop_table_if_exists,
    load_df_to_data_warehouse,
    run_ddl_scripts,
)

with Flow("RapportNavHistorical") as flow:
    logger = prefect.context.get("logger")
    report_types = ["aem"]

    flow_not_running = check_flow_not_running()
    with case(flow_not_running, True):
        mission_ids = rapportnav.extract_missions_ids()

        # Chunk mission ids at runtime using a Prefect task so we can map over batches
        mission_ids_batches = rapportnav.chunk_missions(mission_ids, 100)

        for report_type in report_types:
            # Map fetch_rapportnav_api over the batches produced by chunk_missions
            df_batch = rapportnav.fetch_rapportnav_api.map(
                report_type=unmapped(report_type), missions_ids=mission_ids_batches
            )

            # Concatenate mapped DataFrames at runtime
            # If dataframe is empty, stopping the flow here
            df = rapportnav.concat_dfs(df_batch)

            destination_database = "rapportnav"
            create_database = create_database_if_not_exists("rapportnav")

            drop_table = drop_table_if_exists(
                destination_database,
                report_type,
                upstream_tasks=[create_database],
            )
            created_table = run_ddl_scripts(
                f"rapportnav/create_{report_type}_if_not_exists.sql",
                database=destination_database,
                table=report_type,
                upstream_tasks=[drop_table],
            )

            loaded_df = load_df_to_data_warehouse(
                df,
                destination_database=destination_database,
                destination_table=report_type,
                upstream_tasks=[created_table],
            )


flow.file_name = Path(__file__).name
