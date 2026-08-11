from datetime import date
from pathlib import Path

import pandas as pd
import prefect
from dateutil.relativedelta import relativedelta
from prefect import Flow, Parameter, case, task, unmapped

from forklift.db_engines import create_datawarehouse_client
from forklift.pipeline.helpers.generic import extract, load_to_data_warehouse
from forklift.pipeline.shared_tasks.control_flow import check_flow_not_running
from forklift.pipeline.shared_tasks.dates import get_months_starts, get_utcnow
from forklift.pipeline.shared_tasks.generic import (
    create_database_if_not_exists,
    run_ddl_scripts,
)


@task(checkpoint=False)
def extract_eofs(month_start: date) -> pd.DataFrame:
    min_date = month_start
    max_date = month_start + relativedelta(months=1)

    return extract(
        db_name="monitorfish_remote",
        query_filepath="monitorfish_remote/eofs.sql",
        params={"min_date": min_date, "max_date": max_date},
    )


@task(checkpoint=False)
def load_eofs(eofs: pd.DataFrame, month_start: date):
    logger = prefect.context.get("logger")
    partition = f"{month_start.year}{month_start.month:0>2}"
    client = create_datawarehouse_client()
    logger.info(f"Droppping eofs partition '{partition}' data warehouse.")
    client.command(
        "ALTER TABLE monitorfish.eofs DROP PARTITION {partition:String}",
        parameters={"partition": partition},
    )
    logger.info(f"Loading {len(eofs)} eofs of month {month_start} data warehouse.")
    load_to_data_warehouse(
        eofs,
        table_name="eofs",
        database="monitorfish",
        logger=logger,
        datetime_cols_to_clip=["end_of_fishing_datetime_utc"],
    )


with Flow("EOFs") as flow:
    flow_not_running = check_flow_not_running()
    with case(flow_not_running, True):
        start_months_ago = Parameter("start_months_ago", default=2)
        end_months_ago = Parameter("end_months_ago", default=0)

        now = get_utcnow()
        months_starts = get_months_starts(
            now,
            start_months_ago=start_months_ago,
            end_months_ago=end_months_ago,
        )

        create_database = create_database_if_not_exists("monitorfish")
        created_table = run_ddl_scripts(
            "monitorfish/create_eofs_if_not_exists.sql",
            upstream_tasks=[create_database],
        )

        eofs = extract_eofs.map(months_starts)

        load_eofs.map(eofs, months_starts, upstream_tasks=[unmapped(created_table)])

flow.file_name = Path(__file__).name
