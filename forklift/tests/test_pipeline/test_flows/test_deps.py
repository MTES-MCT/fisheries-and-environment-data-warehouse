from datetime import datetime

import pandas as pd
import pytest
from clickhouse_connect.driver.exceptions import DatabaseError
from pytest import fixture

from forklift.db_engines import create_datawarehouse_client
from forklift.pipeline.flows.deps import extract_deps, flow
from tests.mocks import get_utcnow_mock_factory, replace_check_flow_not_running

replace_check_flow_not_running(flow)


@fixture
def drop_deps():
    client = create_datawarehouse_client()
    print("Drop deps init")
    yield
    print("Drop deps cleaning")
    client.command("DROP TABLE IF EXISTS monitorfish.deps")


@fixture
def expected_deps() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "report_id": [
                "1e1bff95-dfff-4cc3-82d3-d72b46fda745",
                "7712fe73-cef2-4646-97bb-d634fde00b07",
            ],
            "cfr": ["SOCR4T3", "SOCR4T3"],
            "flag_state": ["CYP", "CYP"],
            "trip_number": [
                "SRC-TRP-TTT20200506194051795",
                "SRC-TRP-TTT20200506194051795",
            ],
            "operation_datetime_utc": [
                pd.Timestamp("2020-05-06 18:39:33"),
                pd.Timestamp("2020-05-06 18:39:40"),
            ],
            "departure_datetime_utc": [
                pd.Timestamp("2020-05-06 11:39:33.176000"),
                pd.Timestamp("2020-05-06 11:39:40.722000"),
            ],
            "port_locode": ["ESCAR", "ESCAR"],
            "port_name": [None, None],
            "port_latitude": [None, None],
            "port_longitude": [None, None],
            "country_code_iso2": [None, None],
            "facade": ["Hors façade", "Hors façade"],
            "region": [None, None],
            "fao_area": ["27.9.b.2", None],
            "statistical_rectangle": [None, None],
            "economic_zone": ["ESP", None],
            "species": ["COD", None],
            "nb_fish": [None, None],
            "freshness": [None, None],
            "packaging": ["BOX", None],
            "effort_zone": [None, None],
            "presentation": ["GUT", None],
            "conversion_factor": ["1.1", None],
            "preservation_state": ["FRO", None],
            "weight": [50.0, None],
        }
    )


def test_extract_deps(expected_deps):
    deps = extract_deps.run(month_start=datetime(2020, 5, 1))
    pd.testing.assert_frame_equal(
        deps.sort_values(["report_id"]).reset_index(drop=True),
        expected_deps,
    )

    deps = extract_deps.run(month_start=datetime(2023, 4, 1))
    pd.testing.assert_frame_equal(deps, expected_deps.head(0), check_dtype=False)


def test_deps(drop_deps):
    client = create_datawarehouse_client()

    flow.replace(
        flow.get_tasks("get_utcnow")[0], get_utcnow_mock_factory(datetime(2024, 6, 7))
    )

    query = "SELECT * FROM monitorfish.deps ORDER BY report_id, species"

    # Initially the deps table does not exist
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
