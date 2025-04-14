from pathlib import Path
from typing import Optional

import pandas as pd
import prefect
from prefect import Flow, Parameter, case, task

from forklift.pipeline.helpers.generic import extract, read_table
from forklift.pipeline.shared_tasks.control_flow import (
    check_flow_not_running,
    parameter_is_given,
)
from forklift.pipeline.shared_tasks.generic import (
    create_database_if_not_exists,
    drop_table_if_exists,
    load_df_to_data_warehouse,
    run_data_flow_script,
    run_ddl_scripts,
)


@task(checkpoint=False)
def extract_df(
    source_database: str,
    query_filepath: str = None,
    schema: str = None,
    table_name: str = None,
    backend: str = "pandas",
    geom_col: str = "geom",
    crs: Optional[int] = None,
) -> pd.DataFrame:
    logger = prefect.context.get("logger")

    if query_filepath:
        assert isinstance(query_filepath, str)
        try:
            assert table_name is None and schema is None
        except AssertionError:
            logger.error(
                (
                    "A `table_name` or a `schema` cannot be supplied "
                    "at the same time as a `query_filepath`."
                )
            )
            raise

        return extract(
            db_name=source_database,
            query_filepath=query_filepath,
            backend=backend,
            geom_col=geom_col,
            crs=crs,
        )

    else:
        try:
            assert isinstance(table_name, str) and isinstance(schema, str)
        except AssertionError:
            logger.error(
                (
                    "A `table_name` or a `schema` must be supplied "
                    "if no `query_filepath` if given."
                )
            )
            raise

        res = read_table(
            db=source_database,
            schema=schema,
            table_name=table_name,
            backend=backend,
            geom_col=geom_col,
            crs=crs,
        )
        return res


with Flow("Sync table with pandas") as flow:
    flow_not_running = check_flow_not_running()
    with case(flow_not_running, True):
        source_database = Parameter("source_database")
        query_filepath = Parameter("query_filepath", default=None)
        schema = Parameter("schema", default=None)
        table_name = Parameter("table_name", default=None)
        backend = Parameter("backend", default="pandas")
        geom_col = Parameter("geom_col", default="geom")
        crs = Parameter("crs", default=4326)
        destination_database = Parameter("destination_database")
        destination_table = Parameter("destination_table")
        ddl_script_path = Parameter("ddl_script_path", default=None)
        post_processing_script_path = Parameter(
            "post_processing_script_path", default=None
        )
        final_table = Parameter("final_table", default=None)

        df = extract_df(
            source_database=source_database,
            query_filepath=query_filepath,
            schema=schema,
            table_name=table_name,
            backend=backend,
            geom_col=geom_col,
            crs=crs,
        )

        create_database = create_database_if_not_exists(destination_database)

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
        loaded_df = load_df_to_data_warehouse(
            df,
            destination_database=destination_database,
            destination_table=destination_table,
            upstream_tasks=[created_table],
        )

        post_processing_needed = parameter_is_given(post_processing_script_path, str)

        with case(post_processing_needed, True):
            drop_final_table = drop_table_if_exists(
                database=destination_database,
                table=final_table,
                upstream_tasks=[loaded_df],
            )
            post_processing = run_data_flow_script(
                post_processing_script_path, upstream_tasks=[drop_final_table]
            )
            final_drop_table = drop_table_if_exists(
                database=destination_database,
                table=destination_table,
                upstream_tasks=[post_processing],
            )

flow.set_reference_tasks([loaded_df, final_drop_table])
flow.file_name = Path(__file__).name
