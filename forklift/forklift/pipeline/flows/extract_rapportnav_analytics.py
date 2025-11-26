import pandas as pd
import prefect
import requests
from pathlib import Path
from prefect import Flow, case, task
from forklift.pipeline.shared_tasks.control_flow import check_flow_not_running
from forklift.pipeline.shared_tasks.generic import create_database_if_not_exists, load_df_to_data_warehouse
from forklift.pipeline.helpers.generic import extract
from forklift.config import (
    RAPPORTNAV_API_ENDPOINT,
    RAPPORTNAV_API_KEY
)
from forklift.pipeline.shared_tasks.generic import (
    create_database_if_not_exists,
    drop_table_if_exists,
    load_df_to_data_warehouse,
    run_ddl_scripts,
)


def _default_to_df(json_page: dict) -> pd.DataFrame:
    """Converter from JSON page to DataFrame using pandas.json_normalize."""
    return pd.json_normalize(json_page)

def _process_data(df: pd.DataFrame) -> pd.DataFrame:
    # Temporary : filter out operational summary and control policies fields
    cols_to_remove = [
        c for c in df.columns
        if any(substr in c for substr in ("operationalSummary.", "controlPolicies."))
    ]
    if cols_to_remove:
        logger.info(f"Removing temporary fields from DataFrame: {cols_to_remove}")
        df = df.drop(columns=cols_to_remove, errors="ignore")

    df.columns = df.columns.str.replace('.', '_')
    
    df['controlUnitsIds'] = df['controlUnits'].apply(lambda x: [y['id'] for y in x], 1)
    del df["controlUnits"]

    df['startDateTimeUtc'] = pd.to_datetime(df['startDateTimeUtc']) 
    df['endDateTimeUtc'] = pd.to_datetime(df['endDateTimeUtc']) 

    return df


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
    return list(mission_ids.id)

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
    df = _process_data(df)
    return df

with Flow("RapportNavAnalytics") as flow:    
    logger = prefect.context.get("logger")

    flow_not_running = check_flow_not_running()
    with case(flow_not_running, True):

        
        mission_ids = extract_missions_ids()
        for report_type in ['patrol']:
            df = fetch_rapportnav_api(
                path=f'analytics/v1/{report_type}',
                missions_ids=extract_missions_ids
            )

            destination_database = 'rapportnav'
            create_database = create_database_if_not_exists("rapportnav")

            drop_table = drop_table_if_exists(
                destination_database,
                report_type,
                upstream_tasks=[create_database],
            )
            created_table = run_ddl_scripts(
                f'rapportnav/create_{report_type}_if_not_exists.sql',
                database=destination_database,
                table=report_type,
                upstream_tasks=[drop_table],
            )

            loaded_df = load_df_to_data_warehouse(
                df,
                destination_database=destination_database,
                destination_table=report_type,
                upstream_tasks=[created_table],
            )

            
flow.file_name = Path(__file__).name
