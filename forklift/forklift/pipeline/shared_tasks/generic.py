from pathlib import Path

import pandas as pd
import prefect
from prefect import task

from forklift.pipeline.entities.databases import Database
from forklift.pipeline.helpers.generic import load_to_data_warehouse, run_sql_script


@task(checkpoint=False)
def create_database_if_not_exists(database: Database):
    """
    Creates a database in Clickhouse with the default database engine (Atomic) if it
    does not exist.

    If it already exists, does nothing.

    Args:
        database (Database): Database to create in the data warehouse.
    """
    assert isinstance(database, Database)
    sql = "CREATE DATABASE IF NOT EXISTS {database:Identifier}"
    run_sql_script(sql=sql, parameters={"database": database.value})


@task(checkpoint=False)
def run_ddl_script(ddl_script_path: str, database: str, table: str):
    """
    Runs DDL script at designated location with `database`  and `table` parameters.

    Args:
        ddl_script_path (str): DDL script location, relative to ddl directory
        database (str): database name, passed as `database` parameter to the client
        table (str): table name, passed as `table` parameter to the client
    """
    run_sql_script(
        sql_script_filepath=Path("ddl") / ddl_script_path,
        parameters={
            "database": database,
            "table": table,
        },
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
    sql = "DROP TABLE IF EXISTS  {database:Identifier}.{table:Identifier}"
    run_sql_script(sql=sql, parameters={"database": database, "table": table})


@task(checkpoint=False)
def load_df_to_data_warehouse(
    df: pd.DataFrame,
    destination_database: str,
    destination_table: str,
):
    load_to_data_warehouse(
        df,
        table_name=destination_table,
        database=destination_database,
        logger=prefect.context.get("logger"),
    )
