from datetime import datetime

import pandas as pd
import pytest
from clickhouse_connect.driver.exceptions import DatabaseError
from pytest import fixture

from forklift.db_engines import create_datawarehouse_client
from forklift.pipeline.flows.cps import extract_cps, flow
from tests.mocks import get_utcnow_mock_factory, replace_check_flow_not_running

replace_check_flow_not_running(flow)


@fixture
def drop_cps():
    client = create_datawarehouse_client()
    print("Drop cps init")
    yield
    print("Drop cps cleaning")
    client.command("DROP TABLE IF EXISTS monitorfish.cps")


@fixture
def expected_cps() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "report_id": ["31"],
            "cfr": ["CFR000999999"],
            "flag_state": ["FRA"],
            "trip_number": ["20230008"],
            "operation_datetime_utc": [pd.Timestamp("2025-01-06 20:14:00")],
            "cps_datetime_utc": [pd.Timestamp("2025-01-06 16:37:00")],
            "fao_area": ["27.7.d"],
            "statistical_rectangle": ["25E2"],
            "economic_zone": ["FRA"],
            "species": ["BSS"],
            "weight": [85.0],
            "nb_fish": [None],
            "latitude": [39.65],
            "longitude": [6.83],
            "sex": ["M"],
            "health_state": ["DEA"],
            "care_minutes": [50.0],
            "ring": [None],
            "fate": ["DIS"],
            "comment": ["Remis à l'eau"],
        }
    )


def test_extract_cps(expected_cps):
    cps = extract_cps.run(month_start=datetime(2025, 1, 1))
    pd.testing.assert_frame_equal(
        cps,
        expected_cps,
    )

    cps = extract_cps.run(month_start=datetime(2015, 2, 1))
    pd.testing.assert_frame_equal(cps, expected_cps.head(0), check_dtype=False)


def test_cps(drop_cps):
    client = create_datawarehouse_client()

    flow.replace(
        flow.get_tasks("get_utcnow")[0], get_utcnow_mock_factory(datetime(2025, 2, 1))
    )

    query = "SELECT * FROM monitorfish.cps ORDER BY report_id, species"

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

    expected_report_ids = ["31"]
    assert discards_after_one_run.report_id.tolist() == expected_report_ids
    assert discards_after_two_runs.report_id.tolist() == expected_report_ids
