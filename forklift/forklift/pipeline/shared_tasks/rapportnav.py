from typing import Optional, Callable

import pandas as pd
import prefect
import requests
from prefect import task

from forklift.config import PROXIES
from forklift.pipeline.helpers.generic import load_to_data_warehouse


def _default_to_df(json_page: dict) -> pd.DataFrame:
    """Default converter from JSON page to DataFrame using pandas.json_normalize."""
    # If the API returns an object with a `results` or `data` key, try to find it
    if isinstance(json_page, dict):
        for key in ("results", "data", "items", "rows"):
            if key in json_page and isinstance(json_page[key], (list, dict)):
                return pd.json_normalize(json_page[key])
    # Fallback: try to normalize the whole payload
    return pd.json_normalize(json_page)


@task(checkpoint=False)
def fetch_rapportnav_api(
    api_base: str,
    path: str,
    destination_database: str,
    destination_table: str,
    params: dict = None,
    headers: dict = None,
):
    """Fetch results from a RapportNav API and load them into the data warehouse.

    This task performs a GET request to fetch data (without pagination) from the RapportNav API.
    It converts the JSON response into a pandas DataFrame (using `to_df` if provided, else a
    default heuristic) and writes the data to the ClickHouse data warehouse via
    `load_to_data_warehouse`.

    Environment / config expectations:
    - `api_base` should be the base URL for the RapportNav API (e.g. "https://rapportnav.example.com")
    - `path` is appended to the base to form the full URL (leading slash allowed)

    Args:
        api_base (str): base URL for the API (no trailing slash required)
        path (str): API path to call (e.g. '/api/v1/missions')
        destination_database (str): ClickHouse database name to load into
        destination_table (str): ClickHouse table name to load into
        params (dict): extra query params to include on the request
        headers (dict): extra headers to pass (e.g. API key header)
    Returns:
        int: number of rows loaded
    """
    logger = prefect.context.get("logger")

    url = api_base.rstrip("/") + ("/" + path.lstrip("/") if path else "")
    params = params.copy() if params else {}

    session = requests.Session()
    session.proxies.update(PROXIES)

    if headers:
        session.headers.update(headers)


    logger.info(f"Fetching data from {url}")
    resp = session.get(url, params=params)
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

    if not n_rows:
        logger.info("No data fetched from RapportNav API. Nothing to load.")
        return 0

    # Load into data warehouse
    logger.info(f"Loading {len(df)} rows into {destination_database}.{destination_table}")
    load_to_data_warehouse(
        df,
        table_name=destination_table,
        database=destination_database,
        logger=logger,
    )

    return len(df)
