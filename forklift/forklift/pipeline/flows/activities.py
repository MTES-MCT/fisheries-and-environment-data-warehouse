from datetime import date
from pathlib import Path

import pandas as pd
import prefect
from dateutil.relativedelta import relativedelta
from prefect import Flow, Parameter, case, task, unmapped
from sqlalchemy import text

from forklift.db_engines import create_datawarehouse_client, create_engine
from forklift.pipeline.helpers.generic import read_saved_query
from forklift.pipeline.shared_tasks.control_flow import check_flow_not_running
from forklift.pipeline.shared_tasks.dates import get_months_starts, get_utcnow
from forklift.pipeline.shared_tasks.generic import (
    create_database_if_not_exists,
    run_ddl_scripts,
)


@task(checkpoint=False)
def extract_load_activities(month_start: date) -> pd.DataFrame:
    logger = prefect.context.get("logger")

    min_date = month_start
    max_date = month_start + relativedelta(months=1)

    logger.info(f"Extracting activities for from {min_date} to {max_date}.")
    engine = create_engine("monitorfish_remote")
    with engine.begin() as con:
        savepoint = con.begin_nested()
        con.execute(text("SET jit=off"))
        activities = read_saved_query(
            sql_filepath="monitorfish_remote/activities.sql",
            con=con,
            params={
                "min_date": min_date,
                "max_date": max_date,
            },
        )
        savepoint.rollback()

    partition = f"{month_start.year}{month_start.month:0>2}"
    client = create_datawarehouse_client()
    logger.info(f"Droppping activities partition '{partition}'.")
    client.command(
        "ALTER TABLE monitorfish.activities DROP PARTITION {partition:String}",
        parameters={"partition": partition},
    )
    logger.info(f"Loading {len(activities)} activities of month {month_start}.")
    client.insert_df(table="activities", df=activities, database="monitorfish")


with Flow("Activities") as flow:
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

        created_database = create_database_if_not_exists("monitorfish")
        created_table = run_ddl_scripts(
            "monitorfish/create_activities_if_not_exists.sql",
            upstream_tasks=[created_database],
        )

        activities = extract_load_activities.map(
            months_starts, upstream_tasks=[unmapped(created_table)]
        )

flow.file_name = Path(__file__).name
