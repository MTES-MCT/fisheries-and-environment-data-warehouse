import pytest

from forklift.db_engines import create_datawarehouse_client
from forklift.pipeline.flows.sync_geo_table_to_h3_table import flow
from tests.mocks import replace_check_flow_not_running

replace_check_flow_not_running(flow)


@pytest.mark.parametrize(
    (
        "source_database,query_filepath,geometry_column,resolution,ddl_script_paths,"
        "destination_database,destination_table,batch_size"
    ),
    [
        (
            "monitorfish_remote",
            "monitorfish_remote/regulations.sql",
            "geometry_simplified",
            5,
            "monitorfish/create_regulations_h3.sql",
            "monitorfish",
            "regulations_h3",
            5,
        ),
        (
            "monitorenv_remote",
            "monitorenv_remote/amp_cacem.sql",
            "geom",
            5,
            "monitorenv/create_amp_cacem_h3.sql",
            "monitorenv",
            "amp_cacem_h3",
            50,
        ),
        (
            "monitorenv_remote",
            "monitorenv_remote/regulations_cacem_h3.sql",
            "geom",
            5,
            "monitorenv/create_regulations_cacem_h3.sql",
            "monitorenv",
            "regulations_cacem_h3",
            50,
        ),
    ],
)
def test_sync_geo_table_to_h3_table(
    source_database,
    query_filepath,
    geometry_column,
    resolution,
    ddl_script_paths,
    destination_database,
    destination_table,
    batch_size,
):
    client = create_datawarehouse_client()

    state = flow.run(
        source_database=source_database,
        query_filepath=query_filepath,
        geometry_column=geometry_column,
        resolution=resolution,
        ddl_script_paths=ddl_script_paths,
        destination_database=destination_database,
        destination_table=destination_table,
        batch_size=batch_size,
    )

    assert state.is_successful()

    df = client.query_df(
        (
            "SELECT * FROM "
            "{destination_database:Identifier}.{destination_table:Identifier}"
        ),
        parameters={
            "destination_database": destination_database,
            "destination_table": destination_table,
        },
    )

    assert len(df) > 0

    client.command(
        "DROP DATABASE IF EXISTS {database:Identifier}",
        parameters={"database": destination_database},
    )
