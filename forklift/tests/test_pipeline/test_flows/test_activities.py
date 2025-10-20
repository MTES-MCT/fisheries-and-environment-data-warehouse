from datetime import datetime

import pandas as pd
import pytest
from clickhouse_connect.driver.exceptions import DatabaseError
from pytest import fixture
from sqlalchemy import text

from forklift.db_engines import create_datawarehouse_client, create_engine
from forklift.pipeline.flows.activities import flow
from forklift.pipeline.helpers.generic import read_query
from tests.mocks import get_utcnow_mock_factory, replace_check_flow_not_running

replace_check_flow_not_running(flow)


@fixture
def drop_activities():
    client = create_datawarehouse_client()
    print("Drop activities init")
    yield
    print("Drop activities cleaning")
    client.command("DROP TABLE IF EXISTS monitorfish.activities")


@fixture
def mess_up_activity_datetime_utc():
    engine = create_engine("monitorfish_remote")
    with engine.begin() as con:
        initial_activity_datetime_utc = read_query(
            "SELECT activity_datetime_utc FROM logbook_reports WHERE report_id = '12'",
            con=con,
        ).iloc[0, 0]
        con.execute(
            text(
                "UPDATE logbook_reports "
                "SET activity_datetime_utc = '1910-02-25 12:35:41' "
                "WHERE report_id = '12'"
            )
        )

    yield

    with engine.begin() as con:
        con.execute(
            text(
                "UPDATE logbook_reports "
                "SET activity_datetime_utc = :initial_activity_datetime_utc "
                "WHERE report_id = '12'"
            ),
            parameters={"initial_activity_datetime_utc": initial_activity_datetime_utc},
        )


@fixture
def expected_activities() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "operation_datetime_utc": [
                pd.Timestamp("2025-01-05 18:32:03"),
                pd.Timestamp("2025-01-05 16:57:03"),
                pd.Timestamp("2025-01-05 18:05:03"),
                pd.Timestamp("2025-02-03 18:57:03"),
                pd.Timestamp("2025-02-04 12:57:03"),
                pd.Timestamp("2024-02-02 18:57:03"),
                pd.Timestamp("2024-12-31 18:57:03"),
                pd.Timestamp("2025-01-01 18:57:03"),
                pd.Timestamp("2025-01-05 17:57:03"),
                pd.Timestamp("2025-01-24 18:57:03"),
                pd.Timestamp("2025-01-25 18:57:03"),
                pd.Timestamp("2025-01-26 18:57:03"),
                pd.Timestamp("2025-01-05 18:14:00"),
                pd.Timestamp("2025-01-06 20:14:00"),
                pd.Timestamp("2025-01-05 18:45:03"),
                pd.Timestamp("2025-01-05 18:25:03"),
            ],
            "cfr": [
                "ABC000000000",
                "ABC000306959",
                "ABC000306959",
                "ABC000306959",
                "ABC000306959",
                "ABC000542519",
                "ABC000542519",
                "ABC000542519",
                "ABC000542519",
                "ABC000542519",
                "ABC000542519",
                "ABC000542519",
                "CFR000999999",
                "CFR000999999",
                "INVA_PNO_VES",
                "___TARGET___",
            ],
            "activity_datetime_utc": [
                pd.Timestamp("1970-01-01 00:00:00"),
                pd.Timestamp("2025-01-05 20:57:03"),
                pd.Timestamp("2025-01-05 22:05:03"),
                pd.Timestamp("2025-02-03 18:57:03"),
                pd.Timestamp("2025-02-04 12:57:03"),
                pd.Timestamp("2024-02-02 22:57:03"),
                pd.Timestamp("2024-12-31 18:57:03"),
                pd.Timestamp("2025-01-01 18:57:03"),
                pd.Timestamp("2025-01-05 21:57:03"),
                pd.Timestamp("2025-01-24 18:57:03"),
                pd.Timestamp("2025-01-25 18:57:03"),
                pd.Timestamp("2025-01-26 18:57:03"),
                pd.Timestamp("2025-01-04 08:17:00"),
                pd.Timestamp("2025-01-06 16:37:00"),
                pd.Timestamp("2025-01-05 22:45:03"),
                pd.Timestamp("2025-01-05 22:25:03"),
            ],
            "log_type": [
                "PNO",
                "PNO",
                "PNO",
                "DEP",
                "FAR",
                "PNO",
                "DEP",
                "FAR",
                "PNO",
                "DEP",
                "FAR",
                "FAR",
                "FAR",
                "CPS",
                "PNO",
                "PNO",
            ],
            "trip_number": [
                "20510002",
                "20510001",
                "20510003",
                "20210001",
                "20210001",
                "20210000",
                "20210001",
                "20210001",
                "20510000",
                "20210002",
                "20210002",
                "20210002",
                "20230008",
                "20230008",
                "20510003",
                "20510003",
            ],
            "trip_number_was_computed": [
                False,
                False,
                False,
                False,
                False,
                False,
                False,
                False,
                False,
                False,
                False,
                False,
                False,
                False,
                False,
                False,
            ],
            "report_id": [
                "12",
                "13",
                "14",
                "1",
                "2",
                "8",
                "3",
                "4",
                "11",
                "5",
                "6",
                "7",
                "31",
                "31",
                "21",
                "15",
            ],
        }
    ).astype(
        {
            "operation_datetime_utc": "datetime64[s]",
            "activity_datetime_utc": "datetime64[ns]",
            "cfr": "string[python]",
            "trip_number": "string[python]",
            "log_type": "string[python]",
            "report_id": "string[python]",
        }
    )


def test_activities(
    drop_activities, mess_up_activity_datetime_utc, expected_activities
):
    client = create_datawarehouse_client()

    flow.replace(
        flow.get_tasks("get_utcnow")[0], get_utcnow_mock_factory(datetime(2025, 2, 7))
    )

    query = "SELECT * FROM monitorfish.activities ORDER BY cfr, activity_datetime_utc"

    # Initially the catches table does not exist
    with pytest.raises(
        DatabaseError,
    ):
        client.query_df(query)

    state = flow.run(
        start_months_ago=12,
        end_months_ago=0,
    )
    assert state.is_successful()
    activities_after_one_run = client.query_df(query)

    state = flow.run(
        start_months_ago=12,
        end_months_ago=0,
    )
    assert state.is_successful()
    activities_after_two_runs = client.query_df(query)

    pd.testing.assert_frame_equal(activities_after_one_run, expected_activities)
    pd.testing.assert_frame_equal(activities_after_one_run, activities_after_two_runs)
