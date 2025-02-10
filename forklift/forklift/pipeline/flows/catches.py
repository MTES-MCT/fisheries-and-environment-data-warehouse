from datetime import date, datetime
from pathlib import Path
from typing import List

import pandas as pd
import prefect
from dateutil.relativedelta import relativedelta
from prefect import Flow, Parameter, case, task, unmapped

from forklift.db_engines import create_datawarehouse_client
from forklift.pipeline.helpers.generic import extract
from forklift.pipeline.shared_tasks.control_flow import check_flow_not_running
from forklift.pipeline.shared_tasks.dates import get_utcnow
from forklift.pipeline.shared_tasks.generic import (
    create_database_if_not_exists,
    run_ddl_script,
)


@task(checkpoint=False)
def get_months_starts(
    now: datetime, start_months_ago: int, end_months_ago: int
) -> List[date]:
    assert start_months_ago >= end_months_ago
    start_date = now.date().replace(day=1) - relativedelta(months=start_months_ago)
    end_date = now.date() - relativedelta(months=end_months_ago)
    months_starts = (
        pd.date_range(start=start_date, end=end_date, freq="MS")
        .to_pydatetime()
        .tolist()
    )
    logger = prefect.context.get("logger")
    months_list = ", ".join(m.strftime("%Y-%m") for m in months_starts)
    logger.info(f"Catches will be synced for months { months_list }.")
    return months_starts


@task(checkpoint=False)
def extract_catches(month_start: date) -> pd.DataFrame:
    min_date = month_start
    max_date = month_start + relativedelta(months=1)

    return extract(
        db_name="monitorfish_remote",
        query_filepath="monitorfish_remote/catches.sql",
        params={"min_date": min_date, "max_date": max_date},
    )


@task(checkpoint=False)
def extract_bft_catches(month_start: date) -> pd.DataFrame:
    min_date = month_start
    max_date = month_start + relativedelta(months=1)

    return extract(
        db_name="monitorfish_remote",
        query_filepath="monitorfish_remote/bft_catches.sql",
        params={"min_date": min_date, "max_date": max_date},
    )


@task(checkpoint=False)
def load_catches(catches: pd.DataFrame, month_start: date):
    logger = prefect.context.get("logger")
    partition = f"{month_start.year}{month_start.month:0>2}"
    client = create_datawarehouse_client()
    logger.info(f"Droppping catches partition '{ partition }' data warehouse.")
    client.command(
        "ALTER TABLE monitorfish.catches DROP PARTITION {partition:String}",
        parameters={"partition": partition},
    )
    logger.info(
        f"Loading { len(catches) } catches of month { month_start } data warehouse."
    )
    client.insert_df(table="catches", df=catches, database="monitorfish")


@task(checkpoint=False)
def concat(catches: pd.DataFrame, bft_catches: pd.DataFrame) -> pd.DataFrame:
    return pd.concat([catches, bft_catches])


with Flow("Catches") as flow:
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
            "monitorfish/create_catches_if_not_exists.sql",
            upstream_tasks=[create_database],
        )

        catches = extract_catches.map(months_starts)
        bft_catches = extract_bft_catches.map(months_starts)
        all_catches = concat.map(catches, bft_catches)
        load_catches.map(
            all_catches, months_starts, upstream_tasks=[unmapped(created_table)]
        )

flow.file_name = Path(__file__).name
