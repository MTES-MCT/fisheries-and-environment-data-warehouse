from datetime import datetime

import pandas as pd
import pytest
from clickhouse_connect.driver.exceptions import DatabaseError
from pytest import fixture

from forklift.db_engines import create_datawarehouse_client
from forklift.pipeline.flows.coe import extract_coe, flow
from tests.mocks import get_utcnow_mock_factory, replace_check_flow_not_running

replace_check_flow_not_running(flow)


@fixture
def drop_coe():
    client = create_datawarehouse_client()
    print("Drop coe init")
    yield
    print("Drop coe cleaning")
    client.command("DROP TABLE IF EXISTS monitorfish.coe")


@fixture
def expected_coe() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "report_id": ["2843bd5b-e4e7-4816-8372-76805201301e"],
            "cfr": ["SOCR4T3"],
            "flag_state": ["CYP"],
            "trip_number": ["SRC-TRP-TTT20200506194051795"],
            "operation_datetime_utc": [pd.Timestamp("2020-05-06 18:39:46")],
            "report_document_type": ["NOTIFICATION"],
            "entry_datetime_utc": [pd.Timestamp("2020-05-06 11:39:46.583")],
            "latitude_entered": [42.794],
            "longitude_entered": [-13.809],
            "economic_zone_entered": [None],
            "effort_zone_entered": [None],
            "fao_area_entered": ["29.b"],
            "statistical_rectangle_entered": [None],
            "target_species_on_entry": [None],
            "species": [None],
            "weight": [None],
            "nb_fish": [None],
            "catch_fao_area": [None],
            "catch_statistical_rectangle": [None],
            "catch_economic_zone": [None],
            "catch_effort_zone": [None],
            "presentation": [None],
            "packaging": [None],
            "freshness": [None],
            "preservation_state": [None],
            "conversion_factor": [None],
        }
    )


def test_extract_coe(expected_coe):
    coe = extract_coe.run(month_start=datetime(2020, 5, 1))
    pd.testing.assert_frame_equal(
        coe,
        expected_coe,
    )

    coe = extract_coe.run(month_start=datetime(2015, 2, 1))
    pd.testing.assert_frame_equal(coe, expected_coe.head(0), check_dtype=False)


def test_coe(drop_coe):
    client = create_datawarehouse_client()

    flow.replace(
        flow.get_tasks("get_utcnow")[0], get_utcnow_mock_factory(datetime(2021, 2, 1))
    )

    query = "SELECT * FROM monitorfish.coe ORDER BY report_id, species"

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
        "2843bd5b-e4e7-4816-8372-76805201301e",
        "8eec0190-c353-4147-8a65-fcc697fbadbc",
    ]
    assert after_one_run.report_id.tolist() == expected_report_ids
    assert after_two_runs.report_id.tolist() == expected_report_ids
