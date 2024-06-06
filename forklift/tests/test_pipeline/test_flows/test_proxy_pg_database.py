import pytest

from forklift.db_engines import create_datawarehouse_client
from forklift.pipeline.flows.proxy_pg_database import flow
from tests.mocks import mock_check_flow_not_running

flow.replace(flow.get_tasks("check_flow_not_running")[0], mock_check_flow_not_running)


@pytest.mark.parametrize(
    "source_database,proxy_db_name,test_table,expected_len",
    [
        ("monitorfish_remote", "monitorfish_proxy", "analytics_controls_full_data", 26),
        ("monitorenv_remote", "monitorenv_proxy", "analytics_actions", 7),
    ],
)
def test_proxy_pg_database(source_database, proxy_db_name, test_table, expected_len):
    print(f"Testing the proxying of {source_database} as {proxy_db_name}")
    client = create_datawarehouse_client()
    initial_databases = client.query_df("SHOW DATABASES")
    flow.run(
        database=source_database,
        schema="public",
        database_name_in_dw=proxy_db_name,
    )
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
    assert len(df) == expected_len
    client.command(
        "DROP DATABASE {proxy_db_name:Identifier}",
        parameters={"proxy_db_name": proxy_db_name},
    )
