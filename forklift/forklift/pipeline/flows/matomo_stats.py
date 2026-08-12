from datetime import date
from pathlib import Path

import pandas as pd
import prefect
import requests
from prefect import Flow, Parameter, case, task

from forklift.config import (
    MATOMO_API_TOKEN,
    MATOMO_URL,
    MONITORFISH_MATOMO_SITE_ID,
    PROXIES,
)
from forklift.db_engines import create_datawarehouse_client
from forklift.pipeline.helpers.generic import load_to_data_warehouse
from forklift.pipeline.shared_tasks.control_flow import check_flow_not_running
from forklift.pipeline.shared_tasks.generic import (
    create_database_if_not_exists,
    run_ddl_scripts,
)

matomo_site_ids = {"monitorfish": MONITORFISH_MATOMO_SITE_ID}


@task(checkpoint=False)
def fetch_daily_unique_visitors(start_date: str, application: str) -> pd.DataFrame:
    end_date = date.today().isoformat()
    date_range = f"{start_date},{end_date}"

    try:
        site_id = matomo_site_ids[application]
    except KeyError as e:
        raise ValueError(f"Unknwon application {application}") from e

    params = {
        "module": "API",
        "method": "VisitsSummary.getUniqueVisitors",
        "idSite": site_id,
        "period": "day",
        "date": date_range,
        "format": "JSON",
        "token_auth": MATOMO_API_TOKEN,
    }

    response = requests.post(
        f"{MATOMO_URL}/index.php", params=params, timeout=30, proxies=PROXIES
    )
    response.raise_for_status()
    data = response.json()

    daily_unique_visitors = pd.Series(data)
    daily_unique_visitors.index = pd.to_datetime(daily_unique_visitors.index)
    daily_unique_visitors = daily_unique_visitors.rename_axis("day")
    daily_unique_visitors = daily_unique_visitors.rename("unique_visitors")
    daily_unique_visitors = daily_unique_visitors.reset_index()
    daily_unique_visitors["application"] = application

    return daily_unique_visitors[["application", "day", "unique_visitors"]]


@task(checkpoint=False)
def fetch_monthly_users(start_date: str, application: str) -> pd.DataFrame:
    end_date = date.today().isoformat()
    date_range = f"{start_date},{end_date}"

    try:
        site_id = matomo_site_ids[application]
    except KeyError as e:
        raise ValueError(f"Unknwon application {application}") from e

    params = {
        "module": "API",
        "method": "VisitsSummary.getUsers",
        "idSite": site_id,
        "period": "month",
        "date": date_range,
        "format": "JSON",
        "token_auth": MATOMO_API_TOKEN,
    }

    response = requests.post(
        f"{MATOMO_URL}/index.php", params=params, timeout=30, proxies=PROXIES
    )
    response.raise_for_status()
    data = response.json()

    users = pd.Series(data)
    users.index = pd.to_datetime(users.index)
    users = users.rename_axis("month")
    users = users.rename("users")
    users = users.reset_index()
    users["application"] = application

    return users[["application", "month", "users"]]


@task(checkpoint=False)
def load_monthly_users(monthly_users: pd.DataFrame, application: str):
    logger = prefect.context.get("logger")
    client = create_datawarehouse_client()
    logger.info(f"Droppping monthly_users partition '{application}'.")
    client.command(
        "ALTER TABLE matomo.monthly_users DROP PARTITION {application:String}",
        parameters={"application": application},
    )
    logger.info(
        f"Loading {len(monthly_users)} lines to monthly_users of application {application}."
    )
    load_to_data_warehouse(
        monthly_users,
        table_name="monthly_users",
        database="matomo",
        logger=logger,
    )


@task(checkpoint=False)
def load_daily_unique_visitors(daily_unique_visitors: pd.DataFrame, application: str):
    logger = prefect.context.get("logger")
    client = create_datawarehouse_client()
    logger.info(f"Droppping daily_unique_visitors partition '{application}'.")
    client.command(
        "ALTER TABLE matomo.daily_unique_visitors DROP PARTITION {application:String}",
        parameters={"application": application},
    )
    logger.info(
        f"Loading {len(daily_unique_visitors)} lines to daily_unique_visitors of application {application}."
    )
    load_to_data_warehouse(
        daily_unique_visitors,
        table_name="daily_unique_visitors",
        database="matomo",
        logger=logger,
    )


with Flow("Matomo stats") as flow:
    flow_not_running = check_flow_not_running()
    with case(flow_not_running, True):
        start_date = Parameter("start_date", default="2025-01-01")
        application = Parameter("application", default="monitorfish")
        daily_unique_visitors = fetch_daily_unique_visitors(start_date, application)
        monthly_users = fetch_monthly_users(start_date, application)

        create_database = create_database_if_not_exists("matomo")
        daily_unique_visitors_created_table = run_ddl_scripts(
            "matomo/create_daily_unique_visitors_if_not_exists.sql",
            upstream_tasks=[create_database],
        )
        monthly_users_created_table = run_ddl_scripts(
            "matomo/create_monthly_users_if_not_exists.sql",
            upstream_tasks=[create_database],
        )

        load_daily_unique_visitors(
            daily_unique_visitors,
            application,
            upstream_tasks=[
                daily_unique_visitors_created_table,
                monthly_users_created_table,
            ],
        )

        load_monthly_users(
            monthly_users,
            application,
            upstream_tasks=[
                daily_unique_visitors_created_table,
                monthly_users_created_table,
            ],
        )


flow.file_name = Path(__file__).name
