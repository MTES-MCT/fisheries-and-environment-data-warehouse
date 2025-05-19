from datetime import datetime

import pandas as pd
import pytest
from clickhouse_connect.driver.exceptions import DatabaseError
from pytest import fixture

from forklift.db_engines import create_datawarehouse_client
from forklift.pipeline.flows.catches import extract_bft_catches, extract_catches, flow
from tests.mocks import get_utcnow_mock_factory, replace_check_flow_not_running

replace_check_flow_not_running(flow)


@fixture
def drop_catches():
    client = create_datawarehouse_client()
    print("Drop catches init")
    yield
    print("Drop catches cleaning")
    client.command("DROP TABLE IF EXISTS monitorfish.catches")


@fixture
def expected_catches() -> pd.DataFrame:
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


@fixture
def expected_bft_catches() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "report_id": ["31"],
            "cfr": ["CFR000999999"],
            "flag_state": ["FRA"],
            "trip_number": ["20230008"],
            "latitude": [38.65],
            "longitude": [0.916],
            "operation_datetime_utc": [pd.Timestamp("2025-01-05 18:14:00")],
            "far_datetime_utc": [datetime(2025, 1, 4, 8, 17)],
            "fao_area": ["37.1.1"],
            "statistical_rectangle": [None],
            "economic_zone": ["ESP"],
            "gear": ["PS"],
            "mesh": [140.0],
            "species": ["BFT"],
            "weight": [101593.44],
        }
    )


def test_extract_catches(expected_catches):
    catches = extract_catches.run(month_start=datetime(2025, 1, 1))
    pd.testing.assert_frame_equal(
        catches.sort_values(["report_id", "species"]).reset_index(drop=True),
        expected_catches,
    )

    catches = extract_catches.run(month_start=datetime(2015, 2, 1))
    pd.testing.assert_frame_equal(catches, expected_catches.head(0), check_dtype=False)


def test_extract_bft_catches(expected_bft_catches):
    catches = extract_bft_catches.run(month_start=datetime(2025, 1, 1))
    pd.testing.assert_frame_equal(
        catches.sort_values(["report_id", "species"]).reset_index(drop=True),
        expected_bft_catches,
    )

    catches = extract_catches.run(month_start=datetime(2015, 2, 1))
    pd.testing.assert_frame_equal(
        catches, expected_bft_catches.head(0), check_dtype=False
    )


def test_catches(drop_catches):
    client = create_datawarehouse_client()

    flow.replace(
        flow.get_tasks("get_utcnow")[0], get_utcnow_mock_factory(datetime(2025, 2, 7))
    )

    query = "SELECT * FROM monitorfish.catches ORDER BY report_id, species"

    # Initially the catches table does not exist
    with pytest.raises(
        DatabaseError,
    ):
        client.query_df(query)

    state = flow.run(
        start_months_ago=12,
        end_months_ago=0,
    )
    assert state.is_successful()
    catches_after_one_run = client.query_df(query)

    state = flow.run(
        start_months_ago=12,
        end_months_ago=0,
    )
    assert state.is_successful()
    catches_after_two_runs = client.query_df(query)

    assert len(catches_after_two_runs) == len(catches_after_one_run) == 6

    expected_ids = [
        202501000000000,
        202501000000001,
        202501000000002,
        202501000000003,
        202501000000004,
        202502000000000,
    ]
    assert catches_after_one_run.id.sort_values().tolist() == expected_ids
    assert catches_after_two_runs.id.sort_values().tolist() == expected_ids
