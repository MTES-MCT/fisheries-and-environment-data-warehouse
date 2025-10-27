from typing import Optional, Callable

import pandas as pd
import prefect
import requests
from prefect import task

from forklift.config import (
    PROXIES,
    RAPPORTNAV_API_ENDPOINT
)
from forklift.pipeline.helpers.generic import load_to_data_warehouse


def _default_to_df(json_page: dict) -> pd.DataFrame:
    """Converter from JSON page to DataFrame using pandas.json_normalize."""
    return pd.json_normalize(json_page)


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
    resp = requests.get(
            url,
            proxies=PROXIES
        )

    try:
        resp.raise_for_status()
    except Exception as e:
        logger.error(f"Error fetching RapportNav API: {e}")
        raise

    json_payload = resp.json()

    # Convert payload to DataFrame
    df = _default_to_df(json_payload)

    if not isinstance(df, pd.DataFrame):
        raise ValueError("`to_df` must return a pandas.DataFrame")

    n_rows = len(df)
    logger.info(f"Fetched {n_rows} rows")
    return df
