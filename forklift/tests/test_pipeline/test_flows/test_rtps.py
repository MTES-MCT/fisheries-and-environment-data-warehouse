from datetime import datetime

import pandas as pd
import pytest
from clickhouse_connect.driver.exceptions import DatabaseError
from pytest import fixture

from forklift.db_engines import create_datawarehouse_client
from forklift.pipeline.flows.rtps import extract_rtps, flow
from tests.mocks import replace_check_flow_not_running

replace_check_flow_not_running(flow)


@fixture
def drop_rtps():
    client = create_datawarehouse_client()
    print("Drop rtps init")
    yield
    print("Drop rtps cleaning")
    client.command("DROP TABLE IF EXISTS monitorfish.rtps")


@fixture
def expected_rtps() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "report_id": [
                "cc45063f-2d3c-4cda-ac0c-8381e279e150",
                "dde5df56-24c2-4a2e-8afb-561f32113256",
            ],
            "cfr": ["SOCR4T3", "SOCR4T3"],
            "external_immatriculation": [None, "XR006"],
            "ircs": [None, "IRCS6"],
            "vessel_name": ["GOLF", None],
            "vessel_id": [None, None],
            "flag_state": ["CYP", "CYP"],
            "trip_number": [
                "SRC-TRP-TTT20200506194051795",
                "SRC-TRP-TTT20200506194051795",
            ],
            "port_locode": ["ESCAR", "ESCAR"],
            "port_name": [None, None],
            "port_latitude": [None, None],
            "port_longitude": [None, None],
            "operation_datetime_utc": [
                pd.Timestamp("2020-05-06 18:41:15"),
                pd.Timestamp("2020-05-06 18:41:20"),
            ],
            "report_datetime_utc": [
                pd.Timestamp("2020-05-06 15:41:15"),
                pd.Timestamp("2020-05-06 15:41:20"),
            ],
            "return_datetime_utc": [
                pd.Timestamp("2020-05-06 11:41:15.013000"),
                pd.Timestamp("2020-05-06 11:41:20.712000"),
            ],
            "reason_of_return": ["REF", "LAN"],
        }
    )


def test_extract_rtps(expected_rtps):
    rtps = extract_rtps.run(month_start=datetime(2020, 5, 1))
    pd.testing.assert_frame_equal(
        rtps,
        expected_rtps,
    )

    rtps = extract_rtps.run(month_start=datetime(2023, 4, 1))
    pd.testing.assert_frame_equal(rtps, expected_rtps.head(0), check_dtype=False)


def test_rtps(drop_rtps):
    client = create_datawarehouse_client()

    query = "SELECT * FROM monitorfish.rtps ORDER BY report_id"

    # Initially the rtps table does not exist
    with pytest.raises(
        DatabaseError,
    ):
        client.query_df(query)

    logbook_rtp_date = datetime(2020, 5, 6)
    months_ago = (
        int((datetime.utcnow() - logbook_rtp_date).total_seconds() / 3600 / 24 / 30) + 1
    )

    # Import logbook RTP from 2020-05-06
    state = flow.run(
        start_months_ago=months_ago + 3,
        end_months_ago=months_ago - 3,
    )
    assert state.is_successful()
    rtps_after_one_run = client.query_df(query)

    # Import manual RTP from now - 3 months
    state = flow.run(
        start_months_ago=3,
        end_months_ago=3,
    )
    assert state.is_successful()
    rtps_after_two_runs = client.query_df(query)

    assert len(rtps_after_one_run) == 2
    pd.testing.assert_frame_equal(rtps_after_one_run, rtps_after_two_runs)
