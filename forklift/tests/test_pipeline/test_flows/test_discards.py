from datetime import datetime

import pandas as pd
import pytest
from clickhouse_connect.driver.exceptions import DatabaseError
from pytest import fixture

from forklift.db_engines import create_datawarehouse_client
from forklift.pipeline.flows.discards import extract_discards, flow
from tests.mocks import get_utcnow_mock_factory, replace_check_flow_not_running

replace_check_flow_not_running(flow)


@fixture
def drop_discards():
    client = create_datawarehouse_client()
    print("Drop discards init")
    yield
    print("Drop discards cleaning")
    client.command("DROP TABLE IF EXISTS monitorfish.discards")


@fixture
def expected_discards() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "report_id": ["a913a52e-5e66-4f40-8c64-148f90fa8cd9"],
            "cfr": ["SOCR4T3"],
            "flag_state": ["CYP"],
            "trip_number": ["SRC-TRP-TTT20200506194051795"],
            "operation_datetime_utc": [pd.Timestamp("2020-05-06 18:40:34")],
            "dis_datetime_utc": [pd.Timestamp("2020-05-06 11:40:34.449000")],
            "fao_area": ["27.8.a"],
            "statistical_rectangle": [None],
            "economic_zone": [None],
            "species": ["COD"],
            "weight": [100.0],
            "presentation": [None],
            "packaging": [None],
            "preservation_state": [None],
            "conversion_factor": [None],
            "id": [202005000000000],
        }
    )


def test_extract_catches(expected_discards):
    discards = extract_discards.run(month_start=datetime(2020, 5, 1))
    pd.testing.assert_frame_equal(
        discards,
        expected_discards,
    )

    discards = extract_discards.run(month_start=datetime(2015, 2, 1))
    pd.testing.assert_frame_equal(
        discards, expected_discards.head(0), check_dtype=False
    )


def test_discards(drop_discards):
    client = create_datawarehouse_client()

    flow.replace(
        flow.get_tasks("get_utcnow")[0], get_utcnow_mock_factory(datetime(2020, 12, 7))
    )

    query = "SELECT * FROM monitorfish.discards ORDER BY report_id, species"

    # Initially the discards table does not exist
    with pytest.raises(
        DatabaseError,
    ):
        client.query_df(query)

    state = flow.run(
        start_months_ago=12,
        end_months_ago=0,
    )
    assert state.is_successful()
    discards_after_one_run = client.query_df(query)

    state = flow.run(
        start_months_ago=12,
        end_months_ago=0,
    )
    assert state.is_successful()
    discards_after_two_runs = client.query_df(query)

    assert len(discards_after_two_runs) == len(discards_after_one_run) == 1

    expected_ids = [202005000000000]
    assert discards_after_one_run.id.tolist() == expected_ids
    assert discards_after_two_runs.id.tolist() == expected_ids
