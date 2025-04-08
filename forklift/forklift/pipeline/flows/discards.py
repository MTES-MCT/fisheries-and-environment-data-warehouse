from datetime import date
from pathlib import Path

import pandas as pd
import prefect
from dateutil.relativedelta import relativedelta
from prefect import Flow, Parameter, case, task, unmapped

from forklift.db_engines import create_datawarehouse_client
from forklift.pipeline.helpers.generic import extract
from forklift.pipeline.shared_tasks.control_flow import check_flow_not_running
from forklift.pipeline.shared_tasks.dates import get_months_starts, get_utcnow
from forklift.pipeline.shared_tasks.generic import (
    create_database_if_not_exists,
    run_ddl_script,
)


@task(checkpoint=False)
def extract_discards(month_start: date) -> pd.DataFrame:
    min_date = month_start
    max_date = month_start + relativedelta(months=1)

    discards = extract(
        db_name="monitorfish_remote",
        query_filepath="monitorfish_remote/discards.sql",
        params={"min_date": min_date, "max_date": max_date},
    )
    # Build a unique `discard_id` of the form YYYYMM000000000, YYYYMM000000001...
    discard_id_prefix = 10**9 * (month_start.year * 100 + month_start.month)
    discards["id"] = range(discard_id_prefix, discard_id_prefix + len(discards))

    return discards


@task(checkpoint=False)
def load_discards(discards: pd.DataFrame, month_start: date):
    logger = prefect.context.get("logger")
    partition = f"{month_start.year}{month_start.month:0>2}"
    client = create_datawarehouse_client()
    logger.info(f"Droppping discards partition '{partition}' data warehouse.")
    client.command(
        "ALTER TABLE monitorfish.discards DROP PARTITION {partition:String}",
        parameters={"partition": partition},
    )
    logger.info(
        f"Loading {len(discards)} discards of month {month_start} data warehouse."
    )
    client.insert_df(table="discards", df=discards, database="monitorfish")


with Flow("Discards") as flow:
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
            "monitorfish/create_discards_if_not_exists.sql",
            upstream_tasks=[create_database],
        )

        catches = extract_discards.map(months_starts)
        load_discards.map(
            catches, months_starts, upstream_tasks=[unmapped(created_table)]
        )

flow.file_name = Path(__file__).name
