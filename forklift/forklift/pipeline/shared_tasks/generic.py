from pathlib import Path
from typing import List

import pandas as pd
import prefect
from prefect import task

from forklift.pipeline.entities.databases import Database
from forklift.pipeline.helpers.generic import load_to_data_warehouse, run_sql_script


@task(checkpoint=False)
def create_database_if_not_exists(database: str):
    """
    Creates a database in Clickhouse with the default database engine (Atomic) if it
    does not exist.

    If it already exists, does nothing.

    Args:
        database (str): Database to create in the data warehouse. Possible value are
          `monitorfish`, `monitorenv`
    """
    db = Database(database)
    sql = "CREATE DATABASE IF NOT EXISTS {database:Identifier}"
    run_sql_script(sql=sql, parameters={"database": db.value})


@task(checkpoint=False)
def run_ddl_scripts(
    ddl_script_paths: str | Path | list,
    python_bind_parameters: dict = None,
    **parameters,
):
    """
    Runs DDL script(s) at designated location(s), passing kwargs to `run_sql_script`.

    Args:
        ddl_script_paths (str | Path | list): DDL script location, or list of DDL
          scripts locations, relative to ddl directory
        python_bind_parameters (dict): a dictionary of parameters that will be binded
          in python in the query string before passing the query to the data warehouse.
        parameters (dict, optionnal): pamaters to pass to `run_sql_script`
    """
    logger = prefect.context.get("logger")

    if isinstance(ddl_script_paths, (str, Path)):
        ddl_script_paths = [ddl_script_paths]
    else:
        assert isinstance(ddl_script_paths, list)

    n_scripts = len(ddl_script_paths)

    for i, ddl_script_path in enumerate(ddl_script_paths):
        logger.info(f"Running script {ddl_script_path} ({i + 1}/{n_scripts})")
        run_sql_script(
            sql_script_filepath=Path("ddl") / ddl_script_path,
            python_bind_parameters=python_bind_parameters,
            parameters=parameters,
        )


@task(checkpoint=False)
def run_data_flow_script(data_flow_script_path: str, **parameters):
    """
    Runs data flow script at designated location, passing kwargs to `run_sql_script`.

    Args:
        data_flow_script_path (str): data flow script location, relative to the
          `data_flows` directory
        parameters (dict, optionnal): pamaters to pass to `run_sql_script`
    """
    run_sql_script(
        sql_script_filepath=Path("data_flows") / data_flow_script_path,
        parameters=parameters,
    )


@task(checkpoint=False)
def drop_table_if_exists(database: str, table: str):
    """
    Drops designated table from data_warehouse if it exists.
    If the table does not exist, does nothing.

    Args:
        database (str): Database name in data_warehouse.
        table (str): Name of the table to drop.
    """
    sql = (
        "DROP TABLE IF EXISTS  {database:Identifier}.{table:Identifier} "
        "SETTINGS check_table_dependencies=0"
    )
    run_sql_script(sql=sql, parameters={"database": database, "table": table})


@task(checkpoint=False)
def drop_dictionary_if_exists(database: str, dictionary: str):
    """
    Drops designated dictionary from data_warehouse if it exists.
    If the dictionary does not exist, does nothing.

    Args:
        database (str): Database name in data_warehouse.
        dictionary (str): Name of the dictionary to drop.
    """
    sql = "DROP DICTIONARY IF EXISTS  {database:Identifier}.{dictionary:Identifier}"
    run_sql_script(sql=sql, parameters={"database": database, "dictionary": dictionary})


@task(checkpoint=False)
def drop_partition(database: str, table: str, partition: int | str):
    logger = prefect.context.get("logger")
    logger.info((f"Dropping partition {partition} " f"from table {database}.{table}"))
    run_sql_script(
        sql=(
            "ALTER TABLE {database:Identifier}.{table:Identifier} "
            "DROP PARTITION {partition:String}"
        ),
        parameters={
            "partition": partition,
            "database": database,
            "table": table,
        },
    )


@task(checkpoint=False)
def load_df_to_data_warehouse(
    df: pd.DataFrame,
    destination_database: str,
    destination_table: str,
    datetime_cols_to_clip: List = None,
):
    load_to_data_warehouse(
        df,
        table_name=destination_table,
        database=destination_database,
        logger=prefect.context.get("logger"),
        datetime_cols_to_clip=datetime_cols_to_clip,
    )
