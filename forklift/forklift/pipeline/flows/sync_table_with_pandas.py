from pathlib import Path

import pandas as pd
from prefect import Flow, Parameter, case, task

from forklift.pipeline.entities.databases import Database
from forklift.pipeline.helpers.generic import extract
from forklift.pipeline.shared_tasks.control_flow import check_flow_not_running
from forklift.pipeline.shared_tasks.generic import (
    create_database_if_not_exists,
    create_table_from_ddl_script,
    drop_table_if_exists,
    load_df_to_data_warehouse,
)


@task(checkpoint=False)
def extract_df(source_database: str, query_filepath: str) -> pd.DataFrame:
    return extract(db_name=source_database, query_filepath=query_filepath)


with Flow("Sync table with pandas") as flow:
    flow_not_running = check_flow_not_running()
    with case(flow_not_running, True):
        source_database = Parameter("source_database")
        query_filepath = Parameter("query_filepath", default=None)
        destination_database = Parameter("destination_database")
        destination_table = Parameter("destination_table")
        ddl_script_path = Parameter("ddl_script_path", default=None)

        df = extract_df(
            source_database=source_database,
            query_filepath=query_filepath,
        )

        create_database = create_database_if_not_exists(Database(destination_database))

        drop_table = drop_table_if_exists(
            destination_database,
            destination_table,
            upstream_tasks=[create_database],
        )
        created_table = create_table_from_ddl_script(
            ddl_script_path,
            database=destination_database,
            table=destination_table,
            upstream_tasks=[drop_table],
        )
        load_df_to_data_warehouse(
            df,
            destination_database=destination_database,
            destination_table=destination_table,
            upstream_tasks=[created_table],
        )


flow.file_name = Path(__file__).name
