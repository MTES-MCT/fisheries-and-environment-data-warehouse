import pytest

from forklift.db_engines import create_datawarehouse_client
from forklift.pipeline.flows.reset_proxy_pg_database import flow
from tests.mocks import replace_check_flow_not_running

replace_check_flow_not_running(flow)


@pytest.mark.parametrize(
    "source_database,proxy_db_name,test_table",
    [
        ("monitorfish_remote", "monitorfish_proxy", "vessels"),
        ("monitorenv_remote", "monitorenv_proxy", "missions"),
    ],
)
def test_reset_proxy_pg_database(source_database, proxy_db_name, test_table):
    print(f"Testing the proxying of {source_database} as {proxy_db_name}")
    client = create_datawarehouse_client()
    initial_databases = client.query_df("SHOW DATABASES")
    state = flow.run(
        database=source_database,
        schema="public",
        database_name_in_dw=proxy_db_name,
    )

    assert state.is_successful()
    final_databases = client.query_df("SHOW DATABASES")
    assert set(final_databases.name.values) - set(initial_databases.name.values) == {
        proxy_db_name
    }
    df = client.query_df(
        "SELECT * FROM {proxy_db_name:Identifier}.{test_table:Identifier}",
        parameters={
            "proxy_db_name": proxy_db_name,
            "test_table": test_table,
        },
    )
    assert len(df) > 0
    client.command(
        "DROP DATABASE {proxy_db_name:Identifier}",
        parameters={"proxy_db_name": proxy_db_name},
    )
