from datetime import datetime

import pandas as pd
import pytest
from clickhouse_connect.driver.exceptions import DatabaseError
from dateutil.relativedelta import relativedelta
from pytest import fixture

from forklift.db_engines import create_datawarehouse_client
from forklift.pipeline.flows.pnos import extract_manual_pnos, extract_pnos, flow
from tests.mocks import mock_check_flow_not_running

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
            "external_immatriculation": [None],
            "ircs": [None],
            "vessel_name": [None],
            "vessel_id": [None],
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
            "prior_notification_source": ["LOGBOOK"],
        }
    )


@fixture
def expected_manual_pnos() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "report_id": ["00000000-0000-4000-0000-000000000003"],
            "cfr": [None],
            "flag_state": ["FRA"],
            "trip_number": [None],
            "port_locode": ["FRDPE"],
            "port_name": ["Somewhere over the clouds"],
            "port_latitude": [None],
            "port_longitude": [None],
            "operation_datetime_utc": [pd.Timestamp("2025-02-06 08:16:51.865634")],
            "report_datetime_utc": [pd.Timestamp("2025-02-06 08:16:51.865634")],
            "predicted_arrival_datetime_utc": [
                pd.Timestamp("2021-05-06 07:41:03.340000")
            ],
            "trip_start_date": [pd.Timestamp("2021-05-04 11:41:03.340000")],
            "fao_area": ["37.1"],
            "statistical_rectangle": [None],
            "economic_zone": [None],
            "species": ["BFT"],
            "nb_fish": ["3"],
            "freshness": [None],
            "packaging": [None],
            "effort_zone": [None],
            "presentation": [None],
            "conversion_factor": [None],
            "preservation_state": [None],
            "weight": [172.0],
            "prior_notification_source": ["MANUAL"],
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


def test_extract_manual_pnos():
    now = datetime.utcnow()
    start_date = now.date().replace(day=1) - relativedelta(months=3)

    pnos = extract_manual_pnos.run(month_start=start_date)
    assert len(pnos) == 1

    pnos = extract_pnos.run(month_start=datetime(2023, 4, 1))
    assert len(pnos) == 0


def test_pnos(drop_pnos):
    client = create_datawarehouse_client()

    query = "SELECT * FROM monitorfish.pnos ORDER BY report_id, species"

    # Initially the landings table does not exist
    with pytest.raises(
        DatabaseError,
    ):
        client.query_df(query)

    logbook_pno_date = datetime(2020, 5, 6)
    months_ago = (
        int((datetime.utcnow() - logbook_pno_date).total_seconds() / 3600 / 24 / 30) + 1
    )

    # Import logbook PNO from 2020-05-06
    state = flow.run(
        start_months_ago=months_ago + 3,
        end_months_ago=months_ago - 3,
    )
    assert state.is_successful()
    catches_after_one_run = client.query_df(query)

    # Import manual PNO from now - 3 months
    state = flow.run(
        start_months_ago=3,
        end_months_ago=3,
    )
    assert state.is_successful()
    catches_after_two_runs = client.query_df(query)

    assert len(catches_after_one_run) == 1
    assert len(catches_after_two_runs) == 2
    assert catches_after_two_runs.groupby("prior_notification_source")[
        "report_id"
    ].count().to_dict() == {"LOGBOOK": 1, "MANUAL": 1}
