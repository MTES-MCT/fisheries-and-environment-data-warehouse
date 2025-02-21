import pandas as pd
import pytest

from forklift.config import LIBRARY_LOCATION
from forklift.db_engines import create_datawarehouse_client
from forklift.pipeline.flows.sync_table_from_db_connection import flow
from tests.mocks import mock_check_flow_not_running

if flow.get_tasks("check_flow_not_running"):
    flow.replace(
        flow.get_tasks("check_flow_not_running")[0], mock_check_flow_not_running
    )


scheduled_runs = pd.read_csv(
    LIBRARY_LOCATION / "pipeline/flow_schedules/sync_table_from_db_connection.csv"
).drop(columns=["cron_string"])
parameters = ",".join(scheduled_runs.columns)
try:
    assert parameters == (
        "source_database,source_table,query_filepath,destination_database,"
        "destination_table,ddl_script_path,order_by"
    )
except AssertionError:
    raise ValueError("Test fixtures non coherent with CSV columns")

parameter_values = [
    tuple(r[1].where(r[1].notnull(), None)) for r in scheduled_runs.iterrows()
]


@pytest.mark.parametrize(parameters, parameter_values)
def test_sync_table_from_db_connection(
    add_monitorfish_proxy_database,
    add_monitorenv_proxy_database,
    source_database,
    source_table,
    query_filepath,
    destination_database,
    destination_table,
    ddl_script_path,
    order_by,
):
    print(
        f"Testing syncing of {destination_database}.{destination_table} from {source_database}.{source_table}"
    )
    client = create_datawarehouse_client()

    state = flow.run(
        source_database=source_database,
        source_table=source_table,
        query_filepath=query_filepath,
        destination_database=destination_database,
        destination_table=destination_table,
        ddl_script_path=ddl_script_path,
        order_by=order_by,
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
