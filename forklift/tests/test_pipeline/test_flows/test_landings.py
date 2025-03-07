from datetime import datetime

import pandas as pd
import pytest
from clickhouse_connect.driver.exceptions import DatabaseError
from pytest import fixture

from forklift.db_engines import create_datawarehouse_client
from forklift.pipeline.flows.landings import extract_landings, flow
from tests.mocks import get_utcnow_mock_factory, mock_check_flow_not_running

flow.replace(flow.get_tasks("check_flow_not_running")[0], mock_check_flow_not_running)


@fixture
def drop_landings():
    client = create_datawarehouse_client()
    print("Drop landings init")
    yield
    print("Drop landings cleaning")
    client.command("DROP TABLE IF EXISTS monitorfish.landings")


@fixture
def expected_landings() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "report_id": ["83952732-ef89-4168-b2a1-df49d0aa1aff"],
            "cfr": ["SOCR4T3"],
            "flag_state": ["CYP"],
            "trip_number": ["SRC-TRP-TTT20200506194051795"],
            "operation_datetime_utc": [pd.Timestamp("2020-05-06 18:41:26")],
            "landing_datetime_utc": [pd.Timestamp("2020-05-05 19:41:26.516000")],
            "port_locode": ["ESCAR"],
            "port_name": [None],
            "port_latitude": [None],
            "port_longitude": [None],
            "fao_area": ["27.9.b.2"],
            "statistical_rectangle": [None],
            "economic_zone": ["ESP"],
            "species": ["HAD"],
            "nb_fish": [None],
            "freshness": [None],
            "packaging": ["BOX"],
            "effort_zone": [None],
            "presentation": ["GUT"],
            "conversion_factor": ["1.2"],
            "preservation_state": ["FRO"],
            "weight": [100.0],
        }
    )


def test_extract_landings(expected_landings):
    landings = extract_landings.run(month_start=datetime(2020, 5, 1))
    pd.testing.assert_frame_equal(
        landings.sort_values(["report_id", "species"]).reset_index(drop=True),
        expected_landings,
    )

    landings = extract_landings.run(month_start=datetime(2023, 4, 1))
    pd.testing.assert_frame_equal(
        landings, expected_landings.head(0), check_dtype=False
    )


def test_landings(drop_landings):
    client = create_datawarehouse_client()

    flow.replace(
        flow.get_tasks("get_utcnow")[0], get_utcnow_mock_factory(datetime(2024, 6, 7))
    )

    query = "SELECT * FROM monitorfish.landings ORDER BY report_id, species"

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

    assert len(catches_after_two_runs) == len(catches_after_one_run) == 2
