from datetime import date

import pytest
from pytest import fixture

from forklift.db_engines import create_datawarehouse_client
from forklift.pipeline.entities.generic import IdRange
from forklift.pipeline.entities.sacrois import SacroisPartition
from forklift.pipeline.flows.compute_sacrois_segments import flow, get_partition
from forklift.pipeline.shared_tasks.generic import (
    create_database_if_not_exists,
    run_ddl_scripts,
)
from tests.mocks import replace_check_flow_not_running

replace_check_flow_not_running(flow)


@fixture
def init_sacrois():
    client = create_datawarehouse_client()
    print("Creating sacrois database")
    create_database_if_not_exists.run("sacrois")
    yield
    print("Dropping sacrois databse")
    client.command("DROP DATABASE IF EXISTS sacrois")


@fixture
def init_fishing_activity(init_sacrois):
    print("Creating fishing_activity table")
    run_ddl_scripts.run("sacrois/create_fishing_activity_if_not_exists.sql")
    client = create_datawarehouse_client()
    print("Inserting test data into fishing_activity table")
    client.command(
        """
        INSERT INTO TABLE sacrois.fishing_activity
        SELECT * FROM file('sacrois/fishing_activity_for_compute_segments_test.csv')
    """
    )
    yield
    print("Dropping fishing_activity table")
    client.command("DROP TABLE sacrois.fishing_activity")


@fixture
def expected_segmented_fishing_activity() -> dict:
    return {
        1: "NO_SEGMENT",
        2: "T8-9",
        3: "L",
        4: "L",
        5: "NO_SEGMENT",
        6: "T8-9",
        7: "L",
        8: "L",
        9: "NO_SEGMENT",
        10: "T8-9",
        11: "L",
        12: "L",
        13: "NO_SEGMENT",
        14: "T8-9",
        15: "L",
        16: "L",
        17: "NO_SEGMENT",
        18: "T8-9",
        19: "L",
        20: "L",
        21: "T8-PEL",
        22: "NO_SEGMENT",
        25: "FT",
        26: "FT",
        27: "NO_SEGMENT",
        28: "NO_SEGMENT",
        29: "NO_SEGMENT",
        30: "T8-9",
        31: "L HKE",
        32: "L HKE",
        33: "L BFT",
        34: "L BFT",
        35: "L BFT",
        36: "L BFT",
    }


def test_get_partition():
    partition = get_partition.run(processing_year=2020, processing_month=5)
    assert partition == SacroisPartition("202005", processing_date=date(2020, 5, 1))


def test_get_partition_raises_if_invalid_input():
    with pytest.raises(ValueError):
        get_partition.run(processing_year=2020, processing_month=13)

    with pytest.raises(ValueError):
        get_partition.run(processing_year=20200, processing_month=3)


def test_compute_fleet_segments(
    init_fishing_activity,
    init_fleet_segments,
    init_vessels,
    init_species,
    init_sacrois,
    expected_segmented_fishing_activity,
):
    client = create_datawarehouse_client()
    state = flow.run(
        processing_year=2051,
        segments_year=2050,
        processing_month=1,
        batch_size=3,
    )
    assert state.is_successful()

    query = "SELECT ID, FLEET FROM sacrois.segmented_fishing_activity ORDER BY ID"

    batches = state.result[flow.get_tasks("get_trip_id_ranges")[0]].result
    assert batches == [
        IdRange(id_min=1, id_max=3),
        IdRange(id_min=4, id_max=6),
        IdRange(id_min=7, id_max=8),
    ]

    segmented_fishing_activity_first_run = (
        client.query_df(query).set_index("ID")["FLEET"].to_dict()
    )
    assert segmented_fishing_activity_first_run == expected_segmented_fishing_activity

    # Second run
    state = flow.run(
        processing_year=2051,
        segments_year=2050,
        processing_month=1,
    )
    assert state.is_successful()

    batches = state.result[flow.get_tasks("get_trip_id_ranges")[0]].result
    assert batches == [IdRange(id_min=1, id_max=8)]

    segmented_fishing_activity_second_run = (
        client.query_df(query).set_index("ID")["FLEET"].to_dict()
    )
    assert segmented_fishing_activity_second_run == expected_segmented_fishing_activity
