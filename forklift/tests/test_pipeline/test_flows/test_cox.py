from datetime import datetime

import pandas as pd
import pytest
from clickhouse_connect.driver.exceptions import DatabaseError
from pytest import fixture

from forklift.db_engines import create_datawarehouse_client
from forklift.pipeline.flows.cox import extract_cox, flow
from tests.mocks import get_utcnow_mock_factory, replace_check_flow_not_running

replace_check_flow_not_running(flow)


@fixture
def drop_cox():
    client = create_datawarehouse_client()
    print("Drop cox init")
    yield
    print("Drop cox cleaning")
    client.command("DROP TABLE IF EXISTS monitorfish.cox")


@fixture
def expected_cox() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "report_id": [
                "9d1ddd34-1394-470e-b8a6-469b86150e1e",
                "f006a2e5-0fdd-48a0-9a9a-ccae00d052d8",
            ],
            "cfr": ["SOCR4T3", "SOCR4T3"],
            "flag_state": ["CYP", "CYP"],
            "trip_number": [
                "SRC-TRP-TTT20200506194051795",
                "SRC-TRP-TTT20200506194051795",
            ],
            "operation_datetime_utc": [
                pd.Timestamp("2020-05-06 18:40:57"),
                pd.Timestamp("2020-05-06 18:40:51"),
            ],
            "report_document_type": ["DECLARATION", "NOTIFICATION"],
            "exit_datetime_utc": [
                pd.Timestamp("2020-05-06 11:40:57.580000"),
                pd.Timestamp("2020-05-06 11:40:51.790000"),
            ],
            "latitude_exited": [46.678, 57.7258],
            "longitude_exited": [-14.616, 0.5983],
            "economic_zone_exited": [None, None],
            "effort_zone_exited": ["A", None],
            "fao_area_exited": ["27.7.a", "27.7.a"],
            "statistical_rectangle_exited": [None, None],
            "target_species_on_exit": [None, None],
            "species": [None, None],
            "weight": [None, None],
            "nb_fish": [None, None],
            "catch_fao_area": [None, None],
            "catch_statistical_rectangle": [None, None],
            "catch_economic_zone": [None, None],
            "catch_effort_zone": [None, None],
            "presentation": [None, None],
            "packaging": [None, None],
            "freshness": [None, None],
            "preservation_state": [None, None],
            "conversion_factor": [None, None],
        }
    )


def test_extract_cox(expected_cox):
    cox = extract_cox.run(month_start=datetime(2020, 5, 1))
    pd.testing.assert_frame_equal(
        cox.sort_values("report_id").reset_index(drop=True),
        expected_cox,
    )

    cox = extract_cox.run(month_start=datetime(2015, 2, 1))
    pd.testing.assert_frame_equal(cox, expected_cox.head(0), check_dtype=False)


def test_cox(drop_cox):
    client = create_datawarehouse_client()

    flow.replace(
        flow.get_tasks("get_utcnow")[0], get_utcnow_mock_factory(datetime(2021, 2, 1))
    )

    query = "SELECT * FROM monitorfish.cox ORDER BY report_id, species"

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
    after_one_run = client.query_df(query)

    state = flow.run(
        start_months_ago=12,
        end_months_ago=0,
    )
    assert state.is_successful()
    after_two_runs = client.query_df(query)

    assert len(after_two_runs) == len(after_one_run) == 2

    expected_report_ids = [
        "9d1ddd34-1394-470e-b8a6-469b86150e1e",
        "f006a2e5-0fdd-48a0-9a9a-ccae00d052d8",
    ]
    assert after_one_run.report_id.tolist() == expected_report_ids
    assert after_two_runs.report_id.tolist() == expected_report_ids
