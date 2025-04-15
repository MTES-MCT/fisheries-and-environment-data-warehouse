from pathlib import Path
from typing import List

import geopandas as gpd
import h3.api.numpy_int as h3
import pandas as pd
import prefect
from prefect import Flow, Parameter, case, task, unmapped
from prefect.executors import LocalDaskExecutor

from forklift.pipeline.helpers.generic import extract, load_to_data_warehouse
from forklift.pipeline.shared_tasks.control_flow import check_flow_not_running
from forklift.pipeline.shared_tasks.generic import (
    create_database_if_not_exists,
    drop_table_if_exists,
    run_ddl_scripts,
)


@task(checkpoint=False)
def extract_geo_table(
    source_database: str,
    batch_size: int,
    query_filepath: str,
    geometry_column: str = "geometry",
    crs: int = 4326,
) -> List[gpd.GeoDataFrame]:
    assert isinstance(source_database, str)
    assert isinstance(geometry_column, str)
    assert isinstance(batch_size, int)
    assert batch_size > 0
    assert isinstance(crs, int)

    geo_table = extract(
        db_name=source_database,
        query_filepath=query_filepath,
        backend="geopandas",
        geom_col=geometry_column,
        crs=crs,
    )

    # Splitting into multiple DataFrames enables processing them in parallel in
    # downstream tasks, levering all CPU cores
    geo_tables = [
        geo_table[i : i + batch_size] for i in range(0, geo_table.shape[0], batch_size)
    ]
    return geo_tables


@task(checkpoint=False)
def h3ify_and_load(
    gdf: gpd.GeoDataFrame,
    resolution: int,
    destination_database: str,
    destination_table: str,
) -> pd.DataFrame:
    logger = prefect.context.get("logger")
    h3_df = gdf.copy(deep=True)
    geometry_column = h3_df.active_geometry_name
    logger.info(f"Geometry column found: {geometry_column}")
    logger.info("Converting geometries to h3")
    h3_df["h3"] = h3_df.apply(
        lambda row: h3.h3shape_to_cells_experimental(
            h3.geo_to_h3shape(row[geometry_column].__geo_interface__),
            res=resolution,
            contain="overlap",
        ),
        axis=1,
    )

    logger.info("Exploding h3 arrays")
    h3_df = (
        h3_df.drop(columns=[geometry_column])
        .explode("h3")
        .drop_duplicates()
        .reset_index(drop=True)
    )

    load_to_data_warehouse(
        h3_df,
        table_name=destination_table,
        database=destination_database,
        logger=prefect.context.get("logger"),
    )


with Flow("Geo table to H3", executor=LocalDaskExecutor()) as flow:
    flow_not_running = check_flow_not_running()
    with case(flow_not_running, True):
        source_database = Parameter("source_database")
        query_filepath = Parameter("query_filepath")
        geometry_column = Parameter("geometry_column")
        crs = Parameter("crs", default=4326)
        resolution = Parameter("resolution")
        ddl_script_paths = Parameter("ddl_script_paths")
        destination_database = Parameter("destination_database")
        destination_table = Parameter("destination_table")
        batch_size = Parameter("batch_size", default=50)

        created_database = create_database_if_not_exists(destination_database)
        dropped_table = drop_table_if_exists(
            destination_database, destination_table, upstream_tasks=[created_database]
        )
        created_table = run_ddl_scripts(
            ddl_script_paths, upstream_tasks=[dropped_table]
        )

        gdfs = extract_geo_table(
            source_database=source_database,
            query_filepath=query_filepath,
            batch_size=batch_size,
            geometry_column=geometry_column,
            crs=crs,
        )

        h3_dfs = h3ify_and_load.map(
            gdfs,
            resolution=unmapped(resolution),
            destination_database=unmapped(destination_database),
            destination_table=unmapped(destination_table),
            upstream_tasks=[unmapped(created_table)],
        )


flow.file_name = Path(__file__).name
