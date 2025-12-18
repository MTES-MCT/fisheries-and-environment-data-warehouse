import datetime
import re
from pathlib import Path

import pandas as pd
import prefect
import requests
from prefect import Flow, case, task, unmapped
from prefect.engine.signals import SKIP
from prefect.engine.state import Failed
from prefect.triggers import all_finished
from unidecode import unidecode

from forklift.config import RAPPORTNAV_API_ENDPOINT, RAPPORTNAV_API_KEY
from forklift.pipeline.helpers.generic import extract
from forklift.pipeline.shared_tasks.control_flow import check_flow_not_running
from forklift.pipeline.shared_tasks.generic import (
    create_database_if_not_exists,
    drop_table_if_exists,
    load_df_to_data_warehouse,
    run_ddl_scripts,
)

col_patrol = [
    "id",
    "idUUID",
    "serviceId",
    "missionTypes",
    "controlUnitsIds",
    "facade",
    "startDateTimeUtc",
    "endDateTimeUtc",
    "isDeleted",
    "missionSource",
    "activity_atSea_nbOfDaysAtSea",
    "activity_atSea_navigationDurationInHours",
    "activity_atSea_anchoredDurationInHours",
    "activity_atSea_totalDurationInHours",
    "activity_atSea_nbControls",
    "activity_docked_maintenanceDurationInHours",
    "activity_docked_meteoDurationInHours",
    "activity_docked_representationDurationInHours",
    "activity_docked_adminFormationDurationInHours",
    "activity_docked_mcoDurationInHours",
    "activity_docked_otherDurationInHours",
    "activity_docked_contrPolDurationInHours",
    "activity_docked_totalDurationInHours",
    "activity_docked_nbControls",
    "activity_unavailable_technicalDurationInHours",
    "activity_unavailable_personnelDurationInHours",
    "activity_unavailable_totalDurationInHours",
    "activity_unavailable_nbControls",
    "controlPolicies_proFishing_nbControls",
    "controlPolicies_proFishing_nbControlsSea",
    "controlPolicies_proFishing_nbControlsLand",
    "controlPolicies_proFishing_nbInfractionsWithRecord",
    "controlPolicies_proFishing_nbInfractionsWithoutRecord",
    "controlPolicies_security_nbControls",
    "controlPolicies_security_nbControlsSea",
    "controlPolicies_security_nbControlsLand",
    "controlPolicies_security_nbInfractionsWithRecord",
    "controlPolicies_security_nbInfractionsWithoutRecord",
    "controlPolicies_navigation_nbControls",
    "controlPolicies_navigation_nbControlsSea",
    "controlPolicies_navigation_nbControlsLand",
    "controlPolicies_navigation_nbInfractionsWithRecord",
    "controlPolicies_navigation_nbInfractionsWithoutRecord",
    "controlPolicies_gensDeMer_nbControls",
    "controlPolicies_gensDeMer_nbControlsSea",
    "controlPolicies_gensDeMer_nbControlsLand",
    "controlPolicies_gensDeMer_nbInfractionsWithRecord",
    "controlPolicies_gensDeMer_nbInfractionsWithoutRecord",
    "controlPolicies_administrative_nbControls",
    "controlPolicies_administrative_nbControlsSea",
    "controlPolicies_administrative_nbControlsLand",
    "controlPolicies_administrative_nbInfractionsWithRecord",
    "controlPolicies_administrative_nbInfractionsWithoutRecord",
    "controlPolicies_envPollution_nbControls",
    "controlPolicies_envPollution_nbControlsSea",
    "controlPolicies_envPollution_nbControlsLand",
    "controlPolicies_envPollution_nbInfractionsWithRecord",
    "controlPolicies_envPollution_nbInfractionsWithoutRecord",
    "controlPolicies_other_nbControls",
    "controlPolicies_other_nbControlsSea",
    "controlPolicies_other_nbControlsLand",
    "controlPolicies_other_nbInfractionsWithRecord",
    "controlPolicies_other_nbInfractionsWithoutRecord",
]


def chunk_list(items, batch_size):
    """Split list into batches"""
    for i in range(0, len(items), batch_size):
        yield items[i : i + batch_size]


def _clean_str(s: str, *, lower: bool = True) -> str:
    """Normalize a string for use as a column or identifier.

    - applies `unidecode` to remove accents
    - optionally lowercases
    - replaces punctuation and spaces with underscores
    - removes parentheses and slashes
    - collapses multiple underscores and strips leading/trailing underscores
    """
    if s is None:
        return ""
    s = str(s)
    s = unidecode(s)
    if lower:
        s = s.lower()

    # replace dots, whitespace and single quotes with underscore
    s = re.sub(r"[\.\s']+", "_", s)

    # replace comma and dash by underscore, remove parentheses and slashes
    s = (
        s.replace(",", "_")
        .replace("-", "_")
        .replace("(", "")
        .replace(")", "")
        .replace("/", "")
    )

    # collapse multiple underscores and trim
    s = re.sub(r"_+", "_", s).strip("_")

    return s


def _process_data(df: pd.DataFrame, report_type: str) -> pd.DataFrame:
    if not df.empty:
        # Normalize column names using the shared cleaning function
        df.columns = [_clean_str(c, lower=False) for c in df.columns]

        df["controlUnitsIds"] = df["controlUnits"].apply(
            lambda x: [y["id"] for y in x], 1
        )
        del df["controlUnits"]

        df["startDateTimeUtc"] = pd.to_datetime(df["startDateTimeUtc"], errors="coerce")
        df["endDateTimeUtc"] = pd.to_datetime(df["endDateTimeUtc"], errors="coerce")

        # Deal with potential null values
        df["facade"] = df["facade"].fillna("NON_RESEIGNE")

        if report_type == "patrol":
            df = _process_data_patrol(df)
        elif report_type == "aem":
            df = _process_data_aem(df)
        return df
    else:
        logger.error("Invalid report type")
        return pd.DataFrame()


def _process_data_patrol(df: pd.DataFrame) -> pd.DataFrame:
    # Process null values for control policies
    cols = [col for col in df.columns if "controlPolicies" in col]
    df[cols] = df[cols].fillna(0)

    # Filter columns
    df = df.loc[:, df.columns.isin(col_patrol)]

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
        data_list = row.get("data")
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
                _id = _clean_str(item.get("id", ""), lower=False)
                _title = _clean_str(item.get("title", ""), lower=True)

                # Build column name as id+title (use underscore between to be safe)
                col_name = f"{_id}_{_title}" if _id or _title else ""
                if not col_name:
                    continue

                val = item.get("value")
                # Unpack nested {'value': ...} structures
                if isinstance(val, dict) and "value" in val:
                    val = val.get("value")

                row_expanded[col_name] = val

        expanded_rows.append(row_expanded)

    # Create a DataFrame from the expanded columns and align index with original df
    df_expanded = pd.DataFrame(expanded_rows, index=df.index)

    # Extract year from datetime
    df["annee"] = df["startDateTimeUtc"].dt.year

    # Drop original data column and concat expanded columns
    df = pd.concat([df.drop(columns=["data"], errors="ignore"), df_expanded], axis=1)
    if not df.empty:
        columns_to_del = [
            "endDateTimeUtc",
            "startDateTimeUtc",
            "isDeleted",
            "missionSource",
        ]
        for col in columns_to_del:
            del df[col]

    # Fill empty values with -1 or '' for strings
    for str_col in ["idUUID", "facade", "missionTypes"]:
        df[str_col] = df[str_col].fillna("")
    df = df.fillna(-1)
    return df


@task(checkpoint=False)
def chunk_missions(mission_ids: list, batch_size: int = 100) -> list:
    """Task wrapper around chunk_list so chunking happens at runtime inside the flow."""
    if not mission_ids:
        return []
    return list(chunk_list(mission_ids, batch_size))


@task(checkpoint=False, trigger=all_finished)
def concat_dfs(dfs: list) -> pd.DataFrame:
    """
    Concatenate a list of DataFrames inside the flow runtime.
    """
    if isinstance(dfs, Failed):
        logger.error(
            "Aucune tâche fetch_rapportnav_api n a fonctionné. Aucune donnéee disponible"
        )
        return None
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


@task(checkpoint=False, max_retries=4, retry_delay=datetime.timedelta(seconds=10))
def fetch_rapportnav_api(report_type: str, missions_ids: list):
    """Fetch results from a RapportNav API and returns it as a DataFrame.

    Args:
        report_type (str): Endpoint aem or patrol
    Returns:
        int: number of rows loaded
    """
    logger = prefect.context.get("logger")

    path = f"analytics/v1/{report_type}"
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
    report_types = ["aem"]

    flow_not_running = check_flow_not_running()
    with case(flow_not_running, True):
        mission_ids = extract_missions_ids()

        # Chunk mission ids at runtime using a Prefect task so we can map over batches
        mission_ids_batches = chunk_missions(mission_ids, 100)

        for report_type in report_types:
            # Map fetch_rapportnav_api over the batches produced by chunk_missions
            df_batch = fetch_rapportnav_api.map(
                report_type=unmapped(report_type), missions_ids=mission_ids_batches
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
