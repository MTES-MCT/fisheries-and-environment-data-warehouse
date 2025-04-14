import pytest

from forklift.db_engines import create_datawarehouse_client
from forklift.pipeline.flows.sync_geo_table_to_h3_table import flow
from forklift.pipeline.flows.sync_table_with_pandas import flow as flow_sync_pandas
from tests.mocks import mock_check_flow_not_running

flow_sync_pandas.replace(
    flow_sync_pandas.get_tasks("check_flow_not_running")[0], mock_check_flow_not_running
)
flow.replace(flow.get_tasks("check_flow_not_running")[0], mock_check_flow_not_running)


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
            [
                "monitorfish/create_regulations_h3.sql",
                "monitorfish/add_regulations_h3_proiection.sql",
            ],
            "monitorfish",
            "regulations_h3",
            5,
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
