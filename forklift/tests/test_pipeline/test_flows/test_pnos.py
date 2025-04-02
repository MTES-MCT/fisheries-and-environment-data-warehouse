from datetime import datetime

import pandas as pd
import pytest
from clickhouse_connect.driver.exceptions import DatabaseError
from pytest import fixture

from forklift.db_engines import create_datawarehouse_client
from forklift.pipeline.flows.pnos import extract_pnos, flow
from tests.mocks import get_utcnow_mock_factory, mock_check_flow_not_running

flow.replace(flow.get_tasks("check_flow_not_running")[0], mock_check_flow_not_running)


@fixture
def drop_pnos():
    client = create_datawarehouse_client()
    print("Drop pnos init")
    yield
    print("Drop pnos cleaning")
    client.command("DROP TABLE IF EXISTS monitorfish.pnos")


@fixture
def expected_pnos() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "report_id": ["7ee30c6c-adf9-4f60-a4f1-f7f15ab92803"],
            "cfr": ["SOCR4T3"],
            "flag_state": ["CYP"],
            "trip_number": ["SRC-TRP-TTT20200506194051795"],
            "port_locode": ["GBPHD"],
            "port_name": ["Port with facade"],
            "port_latitude": [None],
            "port_longitude": [None],
            "operation_datetime_utc": [pd.Timestamp("2020-05-06 18:41:03")],
            "report_datetime_utc": [pd.Timestamp("2020-05-06 15:41:03")],
            "predicted_arrival_datetime_utc": [
                pd.Timestamp("2020-05-06 20:41:03.340000")
            ],
            "trip_start_date": [pd.Timestamp("2020-05-04 19:41:03.340000")],
            "fao_area": ["27.7.a"],
            "statistical_rectangle": [None],
            "economic_zone": [None],
            "species": ["GHL"],
            "nb_fish": [None],
            "freshness": [None],
            "packaging": [None],
            "effort_zone": [None],
            "presentation": [None],
            "conversion_factor": [None],
            "preservation_state": [None],
            "weight": [1500.0],
        }
    )


def test_extract_pnos(expected_pnos):
    pnos = extract_pnos.run(month_start=datetime(2020, 5, 1))
    pd.testing.assert_frame_equal(
        pnos,
        expected_pnos,
    )

    pnos = extract_pnos.run(month_start=datetime(2023, 4, 1))
    pd.testing.assert_frame_equal(pnos, expected_pnos.head(0), check_dtype=False)


def test_pnos(drop_pnos):
    client = create_datawarehouse_client()

    flow.replace(
        flow.get_tasks("get_utcnow")[0], get_utcnow_mock_factory(datetime(2024, 6, 7))
    )

    query = "SELECT * FROM monitorfish.pnos ORDER BY report_id, species"

    # Initially the landings table does not exist
    with pytest.raises(
        DatabaseError,
    ):
        client.query_df(query)

    state = flow.run(
        start_months_ago=60,
        end_months_ago=0,
    )
    assert state.is_successful()
    catches_after_one_run = client.query_df(query)

    state = flow.run(
        start_months_ago=60,
        end_months_ago=0,
    )
    assert state.is_successful()
    catches_after_two_runs = client.query_df(query)

    assert len(catches_after_two_runs) == len(catches_after_one_run) == 1
