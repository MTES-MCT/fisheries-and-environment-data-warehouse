from pathlib import Path

import pandas as pd
import prefect
from prefect import Flow, Parameter, case, task

from forklift.pipeline.helpers.generic import (
    extract,
    load_to_data_warehouse,
    run_sql_script,
)
from forklift.pipeline.shared_tasks.control_flow import check_flow_not_running


@task(checkpoint=False)
def create_database_if_not_exists(database: str):
    """
    Creates a database in Clickhouse with the default database engine (Atomic) if it
    does not exist.

    If it already exists, does nothing.

    Args:
        database (str): Name of the database to create in Clickhouse.
    """
    sql = "CREATE DATABASE IF NOT EXISTS {database:Identifier}"
    run_sql_script(sql=sql, parameters={"database": database})


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
def create_table_from_ddl_script(ddl_script_path: str, database: str, table: str):
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
def extract_df(source_database: str, query_filepath: str) -> pd.DataFrame:
    return extract(db_name=source_database, query_filepath=query_filepath)


@task(checkpoint=False)
def load_df(
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


with Flow("Sync table with pandas") as flow:
    flow_not_running = check_flow_not_running()
    with case(flow_not_running, True):
        source_database = Parameter("source_database")
        query_filepath = Parameter("query_filepath", default=None)
        destination_database = Parameter("destination_database")
        destination_table = Parameter("destination_table")
        ddl_script_path = Parameter("ddl_script_path", default=None)

        df = extract_df(
            source_database=source_database,
            query_filepath=query_filepath,
        )

        create_database = create_database_if_not_exists(destination_database)

        drop_table = drop_table_if_exists(
            destination_database,
            destination_table,
            upstream_tasks=[create_database],
        )
        created_table = create_table_from_ddl_script(
            ddl_script_path,
            database=destination_database,
            table=destination_table,
            upstream_tasks=[drop_table],
        )
        load_df(
            df,
            destination_database=destination_database,
            destination_table=destination_table,
            upstream_tasks=[created_table],
        )


flow.file_name = Path(__file__).name
