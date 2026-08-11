from datetime import datetime

import pandas as pd
import pytest
from clickhouse_connect.driver.exceptions import DatabaseError
from pytest import fixture

from forklift.db_engines import create_datawarehouse_client
from forklift.pipeline.flows.eofs import extract_eofs, flow
from tests.mocks import replace_check_flow_not_running

replace_check_flow_not_running(flow)


@fixture
def drop_eofs():
    client = create_datawarehouse_client()
    print("Drop eofs init")
    yield
    print("Drop eofs cleaning")
    client.command("DROP TABLE IF EXISTS monitorfish.eofs")


@fixture
def expected_eofs() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "report_id": ["8b70cee7-2dd4-4b05-9b83-ad902758d9ae"],
            "cfr": ["SOCR4T3"],
            "external_immatriculation": [None],
            "ircs": [None],
            "vessel_name": ["GOLF"],
            "vessel_id": [None],
            "flag_state": ["CYP"],
            "trip_number": ["SRC-TRP-TTT20200506194051795"],
            "operation_datetime_utc": [pd.Timestamp("2020-05-06 18:41:15")],
            "report_datetime_utc": [pd.Timestamp("2020-05-06 15:41:15")],
            "end_of_fishing_datetime_utc": [pd.Timestamp("2020-05-06 11:41:15.013000")],
        }
    )


def test_extract_eofs(expected_eofs):
    eofs = extract_eofs.run(month_start=datetime(2020, 5, 1))
    pd.testing.assert_frame_equal(
        eofs,
        expected_eofs,
    )

    eofs = extract_eofs.run(month_start=datetime(2023, 4, 1))
    pd.testing.assert_frame_equal(eofs, expected_eofs.head(0), check_dtype=False)


def test_eofs(drop_eofs):
    client = create_datawarehouse_client()

    query = "SELECT * FROM monitorfish.eofs ORDER BY report_id"

    # Initially the eofs table does not exist
    with pytest.raises(
        DatabaseError,
    ):
        client.query_df(query)

    logbook_eof_date = datetime(2020, 5, 6)
    months_ago = (
        int((datetime.utcnow() - logbook_eof_date).total_seconds() / 3600 / 24 / 30) + 1
    )

    # Import EOF from 2020-05-06
    state = flow.run(
        start_months_ago=months_ago + 3,
        end_months_ago=months_ago - 3,
    )
    assert state.is_successful()
    eofs_after_one_run = client.query_df(query)

    # Import again
    state = flow.run(
        start_months_ago=months_ago + 3,
        end_months_ago=months_ago - 3,
    )
    assert state.is_successful()
    eofs_after_two_runs = client.query_df(query)

    assert len(eofs_after_one_run) == 1
    pd.testing.assert_frame_equal(eofs_after_one_run, eofs_after_two_runs)
