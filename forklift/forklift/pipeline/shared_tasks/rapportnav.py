
import pandas as pd
import prefect
import requests
from prefect import task
from forklift.pipeline.helpers.generic import extract


from forklift.config import (
    PROXIES,
    RAPPORTNAV_API_ENDPOINT,
    RAPPORTNAV_API_TOKEN
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
                "x-api-key": RAPPORTNAV_API_TOKEN,
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
        raise ValueError("`to_df` must return a pandas.DataFrame")

    n_rows = len(df)
    logger.info(f"Fetched {n_rows} rows")
    return df
