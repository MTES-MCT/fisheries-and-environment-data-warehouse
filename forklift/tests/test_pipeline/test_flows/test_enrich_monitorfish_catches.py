from pytest import fixture

from forklift.db_engines import create_datawarehouse_client
from forklift.pipeline.entities.generic import IdRange
from forklift.pipeline.flows.enrich_monitorfish_catches import extract_cfr_ranges, flow
from forklift.pipeline.shared_tasks.generic import (
    create_database_if_not_exists,
    run_ddl_script,
)
from tests.mocks import mock_check_flow_not_running

flow.replace(flow.get_tasks("check_flow_not_running")[0], mock_check_flow_not_running)


@fixture
def init_monitorfish():
    client = create_datawarehouse_client()
    print("Creating monitorfish database")
    create_database_if_not_exists.run("monitorfish")
    yield
    print("Dropping monitorfish databse")
    client.command("DROP DATABASE IF EXISTS monitorfish")


@fixture
def init_landings(init_monitorfish):
    print("Creating landings table")
    run_ddl_script.run("monitorfish/create_landings_if_not_exists.sql")
    client = create_datawarehouse_client()
    print("Inserting test data into landings table")
    client.command(
        """
        INSERT INTO TABLE monitorfish.landings
        SELECT * FROM file('monitorfish/landings_for_enrich_monitorfish_catches_test.csv')
    """
    )
    yield
    print("Dropping landings table")
    client.command("DROP TABLE monitorfish.landings")


@fixture
def init_catches(init_monitorfish):
    print("Creating catches table")
    run_ddl_script.run("monitorfish/create_catches_if_not_exists.sql")
    client = create_datawarehouse_client()
    print("Inserting test data into catches table")
    client.command(
        """
        INSERT INTO TABLE monitorfish.catches
        SELECT * FROM file('monitorfish/catches_for_enrich_monitorfish_catches_test.csv')
    """
    )
    yield
    print("Dropping catches table")
    client.command("DROP TABLE monitorfish.catches")


@fixture
def expected_segmented_catches() -> dict:
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
        23: "FT",
        24: "FT",
        25: "NO_SEGMENT",
        26: "NO_SEGMENT",
        27: "NO_SEGMENT",
        28: "T8-9",
        29: "L HKE",
        30: "L HKE",
        31: "L BFT",
        32: "L BFT",
        33: "L BFT",
        34: "L BFT",
    }


def test_extract_extract_cfr_ranges(init_catches):
    id_ranges = extract_cfr_ranges.run(far_datetime_year=2050, batch_size=5)
    assert id_ranges == [
        IdRange(id_min="ABC000306959", id_max="CFR000888888"),
    ]

    id_ranges = extract_cfr_ranges.run(far_datetime_year=2050, batch_size=1)
    assert id_ranges == [
        IdRange(id_min="ABC000306959", id_max="ABC000306959"),
        IdRange(id_min="CFR000888888", id_max="CFR000888888"),
    ]


def test_enrich_catches(
    init_landings,
    init_catches,
    init_fleet_segments,
    init_vessels,
    init_species,
    expected_segmented_catches,
):
    query = (
        "SELECT "
        "ROW_NUMBER() OVER (ORDER BY report_id, species, gear DESC, fao_area) AS id, "
        "* "
        "FROM monitorfish.enriched_catches "
        "ORDER BY report_id, species, gear DESC, fao_area"
    )
    client = create_datawarehouse_client()

    # First run
    state = flow.run(
        far_datetime_year=2050,
        batch_size=3,
    )
    assert state.is_successful()

    df = client.query_df(query)
    enriched_catches_first_run = df.set_index("id")["segment"].to_dict()
    assert enriched_catches_first_run == expected_segmented_catches

    # Second run should arrive to the same result
    state = flow.run(
        far_datetime_year=2050,
        batch_size=3,
    )
    assert state.is_successful()

    df = client.query_df(query)
    enriched_catches_second_run = df.set_index("id")["segment"].to_dict()
    assert enriched_catches_second_run == expected_segmented_catches
    landing_ports = (
        df[["trip_number", "landing_port_locode"]]
        .drop_duplicates()
        .set_index("trip_number")
        .to_dict()["landing_port_locode"]
    )
    assert landing_ports == {
        "1": "",
        "Trip_2": "FRCQF",
        "3": "FRLEH",
        "Trip_4": "",
        "5": "",
        "Trip_6": "FRZJZ",
        "7": "FRBES",
        "8": "FRCQF",
    }
