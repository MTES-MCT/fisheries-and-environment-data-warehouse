import pandas as pd
import pytest
from clickhouse_connect.driver.exceptions import DatabaseError
from pytest import fixture

from forklift.db_engines import create_datawarehouse_client
from forklift.pipeline.flows.vms import flow
from tests.mocks import mock_check_flow_not_running

flow.replace(flow.get_tasks("check_flow_not_running")[0], mock_check_flow_not_running)


@fixture
def drop_vms():
    client = create_datawarehouse_client()
    print("Drop vms init")
    yield
    print("Drop vms cleaning")
    client.command("DROP TABLE IF EXISTS monitorfish.vms")


@fixture
def expected_vms() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "report_id": ["4", "6", "7", "7"],
            "cfr": ["ABC000542519", "ABC000542519", "ABC000542519", "ABC000542519"],
            "flag_state": ["FRA", "FRA", "FRA", "FRA"],
            "trip_number": ["20210001", "20210002", "20210002", "20210002"],
            "latitude": [47.084, 47.084, 47.084, 47.084],
            "longitude": [-3.872, -3.872, -3.872, -3.872],
            "operation_datetime_utc": [
                pd.Timestamp("2025-01-01 18:57:03.012521"),
                pd.Timestamp("2025-01-25 18:57:03.012521"),
                pd.Timestamp("2025-01-26 18:57:03.012521"),
                pd.Timestamp("2025-01-26 18:57:03.012521"),
            ],
            "far_datetime_utc": [
                pd.Timestamp("2025-01-01 18:57:03.012000"),
                pd.Timestamp("2025-01-25 18:57:03.012000"),
                pd.Timestamp("2025-01-26 18:57:03.012000"),
                pd.Timestamp("2025-01-26 18:57:03.012000"),
            ],
            "fao_area": ["27.8.c", "27.8.c", "27.8.c", "27.8.c"],
            "statistical_rectangle": ["16E4", "16E4", "16E4", "16E4"],
            "economic_zone": ["FRA", "FRA", "FRA", "FRA"],
            "gear": ["OTB", "OTB", "OTB", "OTB"],
            "mesh": [80.0, 80.0, 80.0, 80.0],
            "species": ["HKE", "HKE", "HKE", "SOL"],
            "weight": [713.0, 713.0, 1713.0, 157.0],
        }
    )


def test_vms(drop_vms):
    client = create_datawarehouse_client()

    query = "SELECT * FROM monitorfish.catches ORDER BY report_id, species"

    # Initially the catches table does not exist
    with pytest.raises(
        DatabaseError,
    ):
        client.query_df(query)

    state = flow.run(
        start_months_ago=0,
        end_months_ago=0,
    )
    assert state.is_successful()
    vms_after_one_run = client.query_df(query)

    state = flow.run(
        start_months_ago=12,
        end_months_ago=0,
    )
    assert state.is_successful()
    vms_after_two_runs = client.query_df(query)

    assert len(vms_after_two_runs) == len(vms_after_one_run) == 6

    expected_ids = [
        202501000000000,
        202501000000001,
        202501000000002,
        202501000000003,
        202501000000004,
        202502000000000,
    ]
    assert vms_after_one_run.id.sort_values().tolist() == expected_ids
    assert vms_after_two_runs.id.sort_values().tolist() == expected_ids
