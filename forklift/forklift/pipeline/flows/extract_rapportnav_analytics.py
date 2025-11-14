import pandas as pd
import requests
from pathlib import Path
import prefect
from prefect import Flow, case, unmapped, task
from forklift.pipeline.shared_tasks.control_flow import check_flow_not_running
from forklift.pipeline.shared_tasks.generic import create_database_if_not_exists, load_df_to_data_warehouse
from forklift.pipeline.helpers.generic import extract
from forklift.config import (
    RAPPORTNAV_API_ENDPOINT,
    RAPPORTNAV_API_KEY
)


def _default_to_df(json_page: dict) -> pd.DataFrame:
    """Converter from JSON page to DataFrame using pandas.json_normalize."""
    return pd.json_normalize(json_page)


@task(checkpoint=False)
def extract_missions_ids() -> list:
    logger = prefect.context.get("logger")

    mission_ids = extract(
        db_name="monitorenv_remote",
        query_filepath="monitorenv_remote/missions.sql",
    )

    logger.info(
        (
            f"Found {len(mission_ids)} missions. "
        )
    )
    return mission_ids

@task(checkpoint=False)
def fetch_rapportnav_api(
    path: str,
    missions_ids: list
):
    """Fetch results from a RapportNav API and returns it as a DataFrame.

    Args:
        path (str): API path to call (e.g. '/api/v1/missions')
    Returns:
        int: number of rows loaded
    """
    logger = prefect.context.get("logger")
    url = RAPPORTNAV_API_ENDPOINT.rstrip("/") + ("/" + path.lstrip("/") if path else "")

    logger.info(f"Fetching data from {url}")
    resp = requests.post(
            url,
            headers={
                "x-api-key": RAPPORTNAV_API_KEY,
                "Accept": 'application/json'
            },
            json={
                "missionIds": missions_ids
            }
        )

    try:
        resp.raise_for_status()
    except Exception as e:
        logger.error(f"Error fetching RapportNav API: {e}")
        raise

    json_payload = resp.json()

    # Convert payload to DataFrame
    df = _default_to_df(json_payload["results"])

    if not isinstance(df, pd.DataFrame):
        raise ValueError("`_default_to_df` must return a pandas.DataFrame")

    n_rows = len(df)
    logger.info(f"Fetched {n_rows} rows")
    return df

with Flow("RapportNavAnalytics") as flow:    
    logger = prefect.context.get("logger")

    flow_not_running = check_flow_not_running()
    with case(flow_not_running, True):
    
        create_database = create_database_if_not_exists("rapportnav")
        mission_ids = extract_missions_ids()
        for report_type in ['aem', 'patrol']:
            df = fetch_rapportnav_api(
                path=f'analytics/v1/{report_type}',
                missions_ids=extract_missions_ids
            )

            load_df_to_data_warehouse(df, 'rapportnav', report_type)
            
flow.file_name = Path(__file__).name
