from pathlib import Path

from prefect import Flow, Parameter, case

from forklift.config import DATA_WAREHOUSE_PWD, DATA_WAREHOUSE_USER
from forklift.pipeline.shared_tasks.control_flow import check_flow_not_running
from forklift.pipeline.shared_tasks.generic import (
    create_database_if_not_exists,
    drop_dictionary_if_exists,
    run_ddl_scripts,
)

with Flow("Reset dictionary") as flow:
    flow_not_running = check_flow_not_running()
    with case(flow_not_running, True):
        database = Parameter("database")
        dictionary = Parameter("dictionary")
        ddl_script_path = Parameter("ddl_script_path")

        create_database = create_database_if_not_exists(database)

        drop_dict = drop_dictionary_if_exists(
            database,
            dictionary,
            upstream_tasks=[create_database],
        )
        run_ddl_scripts(
            ddl_script_path,
            python_bind_parameters=dict(
                user=DATA_WAREHOUSE_USER,
                password=DATA_WAREHOUSE_PWD,
            ),
            upstream_tasks=[drop_dict],
        )


flow.file_name = Path(__file__).name
