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


@task(checkpoint=False)
def fetch_unique_visitors_per_month(start_date: str, application: str) -> pd.DataFrame:
    end_date = date.today().isoformat()
    date_range = f"{start_date},{end_date}"

    matomo_site_ids = {"monitorfish": MONITORFISH_MATOMO_SITE_ID}

    try:
        site_id = matomo_site_ids[application]
    except KeyError as e:
        raise ValueError(f"Unknwon application {application}") from e

    params = {
        "module": "API",
        "method": "VisitsSummary.getUniqueVisitors",
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

    unique_visitors = pd.Series(data)
    unique_visitors.index = pd.to_datetime(unique_visitors.index)
    unique_visitors = unique_visitors.rename_axis("month")
    unique_visitors = unique_visitors.rename("unique_visitors")
    unique_visitors = unique_visitors.reset_index()
    unique_visitors["application"] = application

    return unique_visitors[["application", "month", "unique_visitors"]]


@task(checkpoint=False)
def load_monthly_unique_visitors(
    monthly_unique_visitors: pd.DataFrame, application: str
):
    logger = prefect.context.get("logger")
    client = create_datawarehouse_client()
    logger.info(f"Droppping monthly_unique_visitors partition '{application}'.")
    client.command(
        "ALTER TABLE matomo.monthly_unique_visitors DROP PARTITION {application:String}",
        parameters={"application": application},
    )
    logger.info(
        f"Loading {len(monthly_unique_visitors)} lines to monthly_unique_visitors of application {application}."
    )
    load_to_data_warehouse(
        monthly_unique_visitors,
        table_name="monthly_unique_visitors",
        database="matomo",
        logger=logger,
    )


with Flow("Matomo stats") as flow:
    flow_not_running = check_flow_not_running()
    with case(flow_not_running, True):
        start_date = Parameter("start_date", default="2025-01-01")
        application = Parameter("application", default="monitorfish")
        unique_visitors_per_month = fetch_unique_visitors_per_month(
            start_date, application
        )

        create_database = create_database_if_not_exists("matomo")
        created_table = run_ddl_scripts(
            "matomo/create_monthly_unique_visitors_if_not_exists.sql",
            upstream_tasks=[create_database],
        )

        load_monthly_unique_visitors(
            unique_visitors_per_month, application, upstream_tasks=[created_table]
        )


flow.file_name = Path(__file__).name
