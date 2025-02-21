from datetime import date

import pytest
from pytest import fixture

from forklift.db_engines import create_datawarehouse_client
from forklift.pipeline.entities.sacrois import IdRange, SacroisPartition
from forklift.pipeline.flows.compute_sacrois_segments import (
    flow,
    get_id_ranges,
    get_partition,
)
from forklift.pipeline.flows.sync_table_from_db_connection import (
    flow as sync_table_from_db_connection_flow,
)
from forklift.pipeline.shared_tasks.generic import (
    create_database_if_not_exists,
    run_ddl_script,
)
from tests.mocks import mock_check_flow_not_running

flow.replace(flow.get_tasks("check_flow_not_running")[0], mock_check_flow_not_running)
if sync_table_from_db_connection_flow.get_tasks("check_flow_not_running"):
    sync_table_from_db_connection_flow.replace(
        sync_table_from_db_connection_flow.get_tasks("check_flow_not_running")[0],
        mock_check_flow_not_running,
    )


@fixture
def init_species(add_monitorfish_proxy_database):
    print("Creating monitorfish.species table")
    state = sync_table_from_db_connection_flow.run(
        source_database="monitorfish_proxy",
        source_table="species",
        destination_database="monitorfish",
        destination_table="species",
        order_by="species_code",
    )
    assert state.is_successful()
    client = create_datawarehouse_client()
    yield
    print("Dropping monitorfish.species table")
    client.command("DROP TABLE monitorfish.species")


@fixture
def init_fleet_segments(add_monitorfish_proxy_database):
    # Create table in data warehouse by syncing with monitorfish_proxy database
    print("Creating monitorfish.fleet_segments table")
    state = sync_table_from_db_connection_flow.run(
        source_database="monitorfish_proxy",
        source_table="fleet_segments",
        destination_database="monitorfish",
        destination_table="fleet_segments",
        order_by="year",
    )
    assert state.is_successful()

    # Then replace the fleet_segments data with the fleet segments that we want for the
    # tests in the parquet file
    client = create_datawarehouse_client()
    client.command("TRUNCATE TABLE monitorfish.fleet_segments")
    client.command(
        """
        INSERT INTO TABLE monitorfish.fleet_segments
        SELECT * FROM file('monitorfish/fleet_segments.parquet')
    """
    )

    yield
    print("Dropping monitorfish.fleet_segments table")
    client.command("DROP TABLE monitorfish.fleet_segments")


@fixture
def init_vessels(add_monitorfish_proxy_database):
    # Create table in data warehouse by syncing with vessels database
    print("Creating monitorfish.vessels table")
    state = sync_table_from_db_connection_flow.run(
        source_database="monitorfish_proxy",
        query_filepath="monitorfish_proxy/vessels.sql",
        destination_database="monitorfish",
        destination_table="vessels",
        order_by="id",
    )
    assert state.is_successful()
    client = create_datawarehouse_client()
    yield
    print("Dropping monitorfish.vessels table")
    client.command("DROP TABLE IF EXISTS monitorfish.vessels")


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
    run_ddl_script.run("sacrois/create_fishing_activity_if_not_exists.sql")
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


def test_get_id_ranges_returns_id_ranges():
    ids = [1, 2, 9, 4, 8, 3, 10, 5, 7, 6]
    assert get_id_ranges(ids=ids, batch_size=5) == [
        IdRange(id_min=1, id_max=5),
        IdRange(id_min=6, id_max=10),
    ]
    assert get_id_ranges(ids=ids, batch_size=3) == [
        IdRange(id_min=1, id_max=3),
        IdRange(id_min=4, id_max=6),
        IdRange(id_min=7, id_max=9),
        IdRange(id_min=10, id_max=10),
    ]

    assert get_id_ranges(ids=ids, batch_size=12) == [
        IdRange(id_min=1, id_max=10),
    ]

    assert get_id_ranges(ids=[], batch_size=5) == []


def test_get_id_ranges_raises_if_invalid_input():
    with pytest.raises(AssertionError):
        get_id_ranges([1, 2, 3], batch_size=0)

    with pytest.raises(AssertionError):
        get_id_ranges([1, 2, 3], batch_size="2")

    with pytest.raises(AssertionError):
        get_id_ranges("This is not a list", batch_size=5)

    with pytest.raises(AssertionError):
        get_id_ranges(["This", "is", "not", "an", "integer", "list"], batch_size=5)


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

    query = "SELECT ID, SEGMENT FROM sacrois.segmented_fishing_activity ORDER BY ID"

    batches = state.result[flow.get_tasks("get_trip_id_ranges")[0]].result
    assert batches == [
        IdRange(id_min=1, id_max=3),
        IdRange(id_min=4, id_max=6),
        IdRange(id_min=7, id_max=8),
    ]

    segmented_fishing_activity_first_run = (
        client.query_df(query).set_index("ID")["SEGMENT"].to_dict()
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
        client.query_df(query).set_index("ID")["SEGMENT"].to_dict()
    )
    assert segmented_fishing_activity_second_run == expected_segmented_fishing_activity
