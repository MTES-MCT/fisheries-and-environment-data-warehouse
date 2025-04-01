import pandas as pd
import pytest
from clickhouse_connect.driver.exceptions import DatabaseError
from pytest import fixture

from forklift.db_engines import create_datawarehouse_client
from forklift.pipeline.flows.vms import flow
from tests.mocks import mock_check_flow_not_running

flow.replace(flow.get_tasks("check_flow_not_running")[0], mock_check_flow_not_running)


@fixture
def expected_first_position() -> dict:
    return {
        "id": 13632385,
        "cfr": "ABC000055481",
        "external_reference_number": "AS761555",
        "ircs": "IL2468",
        "vessel_name": "PLACE SPECTACLE SUBIR",
        "flag_state": "NL",
        "latitude": 53.428001403808594,
        "longitude": 5.543000221252441,
        "speed": 3.0,
        "course": 241.0,
        "is_manual": False,
        "is_at_port": False,
        "meters_from_previous_position": 3500.0,
        "time_since_previous_position": 0.5,
        "average_speed": 2.690000057220459,
        "is_fishing": True,
        "time_emitting_at_sea": 24.0,
        "network_type": None,
        "geometry": (5.543, 53.428),
        "h3_6": 603929590649323519,
        "h3_8": 612936789849538559,
    }


@fixture
def drop_vms():
    client = create_datawarehouse_client()
    print("Drop vms init")
    yield
    print("Drop vms cleaning")
    client.command("DROP TABLE IF EXISTS monitorfish.vms")


def test_vms(drop_vms, add_monitorfish_proxy_database, expected_first_position):
    client = create_datawarehouse_client()

    query = "SELECT * FROM monitorfish.vms ORDER BY id"

    # Initially the catches table does not exist
    with pytest.raises(
        DatabaseError,
    ):
        client.query_df(query)

    state = flow.run(
        start_months_ago=1,
        end_months_ago=0,
    )
    assert state.is_successful()
    vms_after_one_run = client.query_df(query)

    state = flow.run(
        start_months_ago=12,
        end_months_ago=0,
    )
    assert state.is_successful()
    vms_after_two_runs = client.query_df(query)

    pd.testing.assert_frame_equal(vms_after_one_run, vms_after_two_runs)
    assert len(vms_after_one_run) == 27

    pos = vms_after_one_run.loc[0].to_dict()
    pos.pop("date_time")
    assert pos == expected_first_position
