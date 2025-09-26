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
def extract_pnos(month_start: date) -> pd.DataFrame:
    min_date = month_start
    max_date = month_start + relativedelta(months=1)

    return extract(
        db_name="monitorfish_remote",
        query_filepath="monitorfish_remote/pnos.sql",
        params={"min_date": min_date, "max_date": max_date},
    )


@task(checkpoint=False)
def extract_manual_pnos(month_start: date) -> pd.DataFrame:
    min_date = month_start
    max_date = month_start + relativedelta(months=1)

    return extract(
        db_name="monitorfish_remote",
        query_filepath="monitorfish_remote/manual_pnos.sql",
        params={"min_date": min_date, "max_date": max_date},
    )


@task(checkpoint=False)
def concat(
    pnos: pd.DataFrame,
    manual_pnos: pd.DataFrame,
) -> pd.DataFrame:
    return pd.concat(
        [
            pnos,
            manual_pnos,
        ],
        ignore_index=True,
    )


@task(checkpoint=False)
def load_pnos(pnos: pd.DataFrame, month_start: date):
    logger = prefect.context.get("logger")
    partition = f"{month_start.year}{month_start.month:0>2}"
    client = create_datawarehouse_client()
    logger.info(f"Droppping pnos partition '{partition}' data warehouse.")
    client.command(
        "ALTER TABLE monitorfish.pnos DROP PARTITION {partition:String}",
        parameters={"partition": partition},
    )
    logger.info(f"Loading {len(pnos)} pnos of month {month_start} data warehouse.")
    load_to_data_warehouse(
        pnos,
        table_name="pnos",
        database="monitorfish",
        logger=logger,
        datetime_cols_to_clip=["predicted_arrival_datetime_utc", "trip_start_date"],
    )


with Flow("PNOs") as flow:
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
            "monitorfish/create_pnos_if_not_exists.sql",
            upstream_tasks=[create_database],
        )

        pnos = extract_pnos.map(months_starts)
        manual_pnos = extract_manual_pnos.map(months_starts)

        all_pnos = concat.map(pnos, manual_pnos)
        load_pnos.map(all_pnos, months_starts, upstream_tasks=[unmapped(created_table)])

flow.file_name = Path(__file__).name
