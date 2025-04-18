from pathlib import Path
from typing import Optional

import prefect
from prefect import Flow, Parameter, case, task

from forklift.config import QUERIES_LOCATION
from forklift.db_engines import create_datawarehouse_client
from forklift.pipeline.helpers.generic import run_sql_script
from forklift.pipeline.shared_tasks.control_flow import (
    check_flow_not_running,
    parameter_is_given,
)
from forklift.pipeline.shared_tasks.generic import (
    create_database_if_not_exists,
    drop_table_if_exists,
    run_ddl_scripts,
)


@task(checkpoint=False)
def insert_data_from_source_to_destination(
    source_database: str,
    destination_database: str,
    destination_table: str,
    create_table: bool,
    order_by: str,
    source_table: str = None,
    query_filepath: Optional[Path] = None,
):
    logger = prefect.context.get("logger")
    client = create_datawarehouse_client()

    # Check for the existence of source and destination databases
    databases = client.query_df("SHOW DATABASES")
    try:
        assert source_database in set(databases.name)
    except AssertionError:
        raise ValueError(
            (
                f"Source database {source_database} not found. "
                f"Available databases : {set(databases.name)}"
            )
        )

    try:
        assert destination_database in set(databases.name)
    except AssertionError:
        raise ValueError(
            (
                f"Destination database {destination_database} not found. "
                f"Available databases : {set(databases.name)}"
            )
        )

    if not create_table:
        # Check for the existence of destination tables. Source table cannot be check in
        # this way, because it can be a view, and `SHOW TABLES` does not include views.
        destination_tables = client.query_df(
            "SHOW TABLES FROM {destination_database:Identifier}",
            parameters={"destination_database": destination_database},
        )
        try:
            assert destination_table in set(destination_tables.name)
        except AssertionError:
            raise ValueError(
                (
                    f"Destination table {destination_table} not found in destination "
                    f"database {destination_database}. Available tables : {set(destination_tables.name)}"
                )
            )

    if create_table:
        logger.info(
            (
                f"Creating table {destination_database}.{destination_table} "
                "from SELECT query."
            )
        )

        assert destination_database is not None
        assert destination_table is not None
        assert order_by is not None

        sql = """
        CREATE TABLE {destination_database:Identifier}.{destination_table:Identifier}
        ENGINE MergeTree
        ORDER BY {order_by:Identifier}
        AS """
    else:
        logger.info(f"Inserting data into {destination_database}.{destination_table}.")
        sql = (
            "INSERT INTO "
            "{destination_database:Identifier}.{destination_table:Identifier} "
        )

    if query_filepath:
        with open(QUERIES_LOCATION / query_filepath, "r") as f:
            query = f.read()
        sql += query
    else:
        sql += "SELECT * FROM {source_database:Identifier}.{source_table:Identifier}"

    run_sql_script(
        sql=sql,
        parameters={
            "source_database": source_database,
            "source_table": source_table,
            "destination_database": destination_database,
            "destination_table": destination_table,
            "order_by": order_by,
        },
    )


with Flow("Sync table from database connection") as flow:
    flow_not_running = check_flow_not_running()
    with case(flow_not_running, True):
        source_database = Parameter("source_database")
        source_table = Parameter("source_table", default=None)
        query_filepath = Parameter("query_filepath", default=None)
        destination_database = Parameter("destination_database")
        destination_table = Parameter("destination_table")
        ddl_script_path = Parameter("ddl_script_path", default=None)
        order_by = Parameter("order_by", default=None)

        ddl_script_given = parameter_is_given(ddl_script_path, str)

        create_database = create_database_if_not_exists(destination_database)

        with case(ddl_script_given, False):
            drop_table = drop_table_if_exists(
                destination_database,
                destination_table,
                upstream_tasks=[create_database],
            )
            insert_data_from_source_to_destination(
                source_database,
                destination_database,
                destination_table,
                create_table=True,
                order_by=order_by,
                source_table=source_table,
                query_filepath=query_filepath,
                upstream_tasks=[drop_table],
            )

        with case(ddl_script_given, True):
            drop_table = drop_table_if_exists(
                destination_database,
                destination_table,
                upstream_tasks=[create_database],
            )
            created_table = run_ddl_scripts(
                ddl_script_path,
                database=destination_database,
                table=destination_table,
                upstream_tasks=[drop_table],
            )
            insert_data_from_source_to_destination(
                source_database,
                destination_database,
                destination_table,
                create_table=False,
                order_by=order_by,
                source_table=source_table,
                query_filepath=query_filepath,
                upstream_tasks=[created_table],
            )


flow.file_name = Path(__file__).name
