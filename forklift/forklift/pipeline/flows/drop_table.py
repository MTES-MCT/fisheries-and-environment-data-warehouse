from pathlib import Path

from prefect import Flow, Parameter, case

from forklift.pipeline.shared_tasks.control_flow import check_flow_not_running
from forklift.pipeline.shared_tasks.generic import drop_table_if_exists

with Flow("Drop table") as flow:
    flow_not_running = check_flow_not_running()
    with case(flow_not_running, True):
        database = Parameter("database")
        table = Parameter("table")
        drop_table = drop_table_if_exists(database, table)

flow.file_name = Path(__file__).name
