from pathlib import Path

import pandas as pd
import prefect
import requests
from unidecode import unidecode
from pathlib import Path
from prefect import Flow, case, task, unmapped
from prefect.engine.signals import SKIP

from forklift.config import RAPPORTNAV_API_ENDPOINT, RAPPORTNAV_API_KEY
from forklift.pipeline.helpers.generic import extract
from forklift.pipeline.shared_tasks.control_flow import check_flow_not_running
from forklift.pipeline.shared_tasks.generic import (
    create_database_if_not_exists,
    drop_table_if_exists,
    load_df_to_data_warehouse,
    run_ddl_scripts,
)


def chunk_list(items, batch_size):
    """Split list into batches"""
    for i in range(0, len(items), batch_size):
        yield items[i : i + batch_size]

def _process_data(df: pd.DataFrame, report_type:str) -> pd.DataFrame:
    if report_type == 'patrol':
        df = _process_data_patrol(df)
    elif report_type == 'aem':
        df = _process_data_aem(df)
    else:
        logger.error('Invalid report type')
        return pd.DataFrame()
    
    df['controlUnitsIds'] = df['controlUnits'].apply(lambda x: [y['id'] for y in x], 1)
    del df["controlUnits"]
    
    df.columns = df.columns.str.replace('.', '_').str.replace(' ', '_')

    df['startDateTimeUtc'] = pd.to_datetime(df['startDateTimeUtc']) 
    df['endDateTimeUtc'] = pd.to_datetime(df['endDateTimeUtc']) 

    # Deal with potential null values
    df['facade'] = df['facade'].fillna('NON_RESEIGNE')
    return df


def _process_data_patrol(df: pd.DataFrame) -> pd.DataFrame:
    # Temporary : filter out operational summary and control policies fields
    cols_to_remove = [
        c
        for c in df.columns
        if any(substr in c for substr in ("operationalSummary.", "controlPolicies."))
        and "total" not in c
    ]
    if cols_to_remove:
        logger.info("Removing temporary fields from DataFrame")
        df = df.drop(columns=cols_to_remove, errors="ignore")

    return df

def _process_data_aem(df: pd.DataFrame) -> pd.DataFrame:
    """Expand the `data` column (a list of dicts) into individual columns.

    For each element in the list we create a column named "{id}_{title}" and set
    its value to the element's `value` (unpacking nested dicts if necessary).
    """
    # If data is a column of JSON strings, try to normalize it first
    if df.empty:
        return df

    expanded_rows = []
    for _, row in df.iterrows():
        data_list = row.get('data')
        # Ensure we have a list to iterate
        if data_list is None:
            expanded_rows.append({})
            continue

        # If the cell is a JSON string, attempt to parse it
        if isinstance(data_list, str):
            try:
                import json
                data_list = json.loads(data_list)
            except Exception:
                data_list = []

        row_expanded = {}
        if isinstance(data_list, list):
            for item in data_list:
                if not isinstance(item, dict):
                    continue
                _id = item.get('id', '')
                _title = item.get('title', '')
                _title = unidecode(_title).lower()
                # Build column name as id+title (use underscore between to be safe)
                col_name = f"{_id}_{_title}" if _id or _title else ''
                if not col_name:
                    continue

                val = item.get('value')
                # Unpack nested {'value': ...} structures
                if isinstance(val, dict) and 'value' in val:
                    val = val.get('value')

                row_expanded[col_name] = val

        expanded_rows.append(row_expanded)

    # Create a DataFrame from the expanded columns and align index with original df
    df_expanded = pd.DataFrame(expanded_rows, index=df.index)

    # Drop original data column and concat expanded columns
    df = pd.concat([df.drop(columns=['data'], errors='ignore'), df_expanded], axis=1)
    return df


@task(checkpoint=False)
def chunk_missions(mission_ids: list, batch_size: int = 100) -> list:
    """Task wrapper around chunk_list so chunking happens at runtime inside the flow."""
    if not mission_ids:
        return []
    return list(chunk_list(mission_ids, batch_size))


@task(checkpoint=False)
def concat_dfs(dfs: list) -> pd.DataFrame:
    """
    Concatenate a list of DataFrames inside the flow runtime.
    """
    # Filter out any None values
    dfs = [d for d in dfs if not d.empty]
    if not dfs:
        raise SKIP("Dataframe vide. Fin du flow...")
    return pd.concat(dfs, ignore_index=True)


@task(checkpoint=False)
def extract_missions_ids() -> list:
    logger = prefect.context.get("logger")

    mission_ids = extract(
        db_name="monitorenv_remote",
        query_filepath="monitorenv_remote/missions.sql",
        parse_dates=["start_datetime_utc"],
    )

    logger.info((f"Found {len(mission_ids)} missions. "))
    return list(mission_ids.id)


@task(checkpoint=False)
def fetch_rapportnav_api(
    report_type: str,
    missions_ids: list
):
    """Fetch results from a RapportNav API and returns it as a DataFrame.

    Args:
        report_type (str): Endpoint aem or patrol
    Returns:
        int: number of rows loaded
    """
    logger = prefect.context.get("logger")

    path = f'analytics/v1/{report_type}'
    url = RAPPORTNAV_API_ENDPOINT.rstrip("/") + ("/" + path.lstrip("/") if path else "")

    logger.info(f"Fetching data from {url}")
    resp = requests.post(
        url,
        headers={"x-api-key": RAPPORTNAV_API_KEY, "Accept": "application/json"},
        json={"missionIds": missions_ids},
    )

    try:
        resp.raise_for_status()
    except Exception as e:
        logger.error(f"Error fetching RapportNav API: {e}")
        raise

    json_payload = resp.json()

    # Convert payload to DataFrame
    df = pd.json_normalize(json_payload["results"])

    if not isinstance(df, pd.DataFrame):
        raise ValueError("`_default_to_df` must return a pandas.DataFrame")

    n_rows = len(df)
    logger.info(f"Fetched {n_rows} rows")

    if n_rows:
        df = _process_data(df, report_type)
    return df


with Flow("RapportNavAnalytics") as flow:
    logger = prefect.context.get("logger")

    flow_not_running = check_flow_not_running()
    with case(flow_not_running, True):
        mission_ids = extract_missions_ids()

        # Chunk mission ids at runtime using a Prefect task so we can map over batches
        mission_ids_batches = chunk_missions(mission_ids, 100)

        for report_type in ["patrol"]:
            # Map fetch_rapportnav_api over the batches produced by chunk_missions
            df_batch = fetch_rapportnav_api.map(
                report_type=unmapped(report_type),
                missions_ids=mission_ids_batches
            )

            # Concatenate mapped DataFrames at runtime
            # If dataframe is empty, stopping the flow here
            df = concat_dfs(df_batch)

            destination_database = "rapportnav"
            create_database = create_database_if_not_exists("rapportnav")

            drop_table = drop_table_if_exists(
                destination_database,
                report_type,
                upstream_tasks=[create_database],
            )
            created_table = run_ddl_scripts(
                f"rapportnav/create_{report_type}_if_not_exists.sql",
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
