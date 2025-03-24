from datetime import date
from pathlib import Path

import pandas as pd
import prefect
from dateutil.relativedelta import relativedelta
from prefect import Flow, Parameter, case, task, unmapped

from forklift.db_engines import create_datawarehouse_client
from forklift.pipeline.helpers.generic import run_sql_script
from forklift.pipeline.shared_tasks.control_flow import check_flow_not_running
from forklift.pipeline.shared_tasks.dates import get_months_starts, get_utcnow
from forklift.pipeline.shared_tasks.generic import (
    create_database_if_not_exists,
    run_ddl_script,
)


@task(checkpoint=False)
def extract_load_vms(month_start: date) -> pd.DataFrame:
    logger = prefect.context.get("logger")
    min_date = month_start
    max_date = month_start + relativedelta(months=1)

    partition = f"{month_start.year}{month_start.month:0>2}"
    client = create_datawarehouse_client()
    logger.info(f"Droppping vms partition '{ partition }' from data warehouse.")
    client.command(
        "ALTER TABLE monitorfish.vms DROP PARTITION {partition:String}",
        parameters={"partition": partition},
    )

    logger.info(f"Loading vms positions of month { month_start } into data warehouse.")
    run_sql_script(
        sql_script_filepath=Path("data_flows/monitorfish/vms.sql"),
        parameters={"min_date": min_date, "max_date": max_date},
    )


with Flow("VMS") as flow:
    flow_not_running = check_flow_not_running()
    with case(flow_not_running, True):
        start_months_ago = Parameter("start_months_ago", default=0)
        end_months_ago = Parameter("end_months_ago", default=0)

        now = get_utcnow()
        months_starts = get_months_starts(
            now,
            start_months_ago=start_months_ago,
            end_months_ago=end_months_ago,
        )

        create_database = create_database_if_not_exists("monitorfish")
        created_table = run_ddl_script(
            "monitorfish/create_vms_if_not_exists.sql",
            upstream_tasks=[create_database],
        )

        extract_load_vms.map(months_starts, upstream_tasks=[unmapped(created_table)])

flow.file_name = Path(__file__).name
