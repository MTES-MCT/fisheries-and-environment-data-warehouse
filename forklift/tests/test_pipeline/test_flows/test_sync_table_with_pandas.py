import pytest

from forklift.db_engines import create_datawarehouse_client
from forklift.pipeline.flows.sync_table_with_pandas import flow
from tests.mocks import mock_check_flow_not_running

flow.replace(flow.get_tasks("check_flow_not_running")[0], mock_check_flow_not_running)


@pytest.mark.parametrize(
    (
        "source_database,"
        "query_filepath,"
        "destination_database,"
        "destination_table,"
        "ddl_script_path,"
    ),
    [
        (
            "monitorenv_remote",
            "monitorenv_remote/analytics_actions.sql",
            "monitorenv",
            "analytics_actions",
            "monitorenv/create_analytics_actions.sql",
        ),
    ],
)
def test_sync_table(
    source_database,
    query_filepath,
    destination_database,
    destination_table,
    ddl_script_path,
):
    print(
        f"Testing syncing of {destination_database}.{destination_table} from {source_database}."
    )
    client = create_datawarehouse_client()

    state = flow.run(
        source_database=source_database,
        query_filepath=query_filepath,
        destination_database=destination_database,
        destination_table=destination_table,
        ddl_script_path=ddl_script_path,
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
        (
            "DROP TABLE "
            "{destination_database:Identifier}.{destination_table:Identifier}"
        ),
        parameters={
            "destination_database": destination_database,
            "destination_table": destination_table,
        },
    )
