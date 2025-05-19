import pandas as pd
from prefect import task
from pytest import fixture

from forklift.db_engines import create_datawarehouse_client
from forklift.pipeline.entities.generic import IdRange
from forklift.pipeline.flows.enrich_monitorfish_catches import extract_cfr_ranges, flow
from forklift.pipeline.flows.reset_dictionary import flow as reset_dictionary_flow
from forklift.pipeline.flows.sync_table_with_pandas import (
    flow as sync_table_with_pandas_flow,
)
from forklift.pipeline.helpers.generic import run_sql_script
from forklift.pipeline.shared_tasks.generic import (
    create_database_if_not_exists,
    run_ddl_scripts,
)
from tests.mocks import replace_check_flow_not_running

replace_check_flow_not_running(flow)
replace_check_flow_not_running(sync_table_with_pandas_flow)
replace_check_flow_not_running(reset_dictionary_flow)


def mock_get_current_year_factory(year: int):
    @task(checkpoint=False)
    def mock_get_current_year() -> int:
        """Returns current year"""
        return year

    return mock_get_current_year


mock_get_current_year = mock_get_current_year_factory(2051)

flow.replace(flow.get_tasks("get_current_year")[0], mock_get_current_year)


@fixture
def init_monitorfish():
    print("Creating monitorfish database")
    create_database_if_not_exists.run("monitorfish")
    yield
    print("Dropping monitorfish databse")
    run_sql_script(sql="DROP DATABASE IF EXISTS monitorfish")


@fixture
def init_landings(init_monitorfish):
    print("Creating landings table")
    run_ddl_scripts.run("monitorfish/create_landings_if_not_exists.sql")
    print("Inserting test data into landings table")
    run_sql_script(
        sql="""
        INSERT INTO TABLE monitorfish.landings
        SELECT * FROM file('monitorfish/landings_for_enrich_monitorfish_catches_test.csv')
    """
    )
    yield
    print("Dropping landings table")
    run_sql_script(sql="DROP TABLE monitorfish.landings")


@fixture
def init_catches(init_monitorfish):
    print("Creating catches table")
    run_ddl_scripts.run("monitorfish/create_catches_if_not_exists.sql")
    print("Inserting test data into catches table")
    run_sql_script(
        sql="""
        INSERT INTO TABLE monitorfish.catches
        SELECT * FROM file('monitorfish/catches_for_enrich_monitorfish_catches_test.csv')
    """
    )
    yield
    print("Dropping catches table")
    run_sql_script(sql="DROP TABLE monitorfish.catches")


@fixture
def init_rectangles_stat_areas(init_monitorfish):
    print("Creating rectangles_stat_areas table")
    print("Inserting test data into vms table")
    sync_table_with_pandas_flow.run(
        source_database="monitorfish_remote",
        query_filepath="monitorfish_remote/rectangles_stat_areas.sql",
        backend="geopandas",
        geom_col="wkb_geometry",
        destination_database="monitorfish",
        destination_table="rectangles_stat_areas_tmp",
        ddl_script_path="monitorfish/create_rectangles_stat_areas_tmp.sql",
        post_processing_script_path="monitorfish/post_process_rectangles_stat_areas.sql",
        final_table="rectangles_stat_areas",
    )
    yield
    print("Dropping rectangles_stat_areas table")
    run_sql_script(sql="DROP TABLE monitorfish.rectangles_stat_areas")


@fixture
def init_vms(init_monitorfish):
    print("Creating vms table")
    run_ddl_scripts.run("monitorfish/create_vms_if_not_exists.sql")
    print("Inserting test data into vms table")
    run_sql_script(
        sql="""
        INSERT INTO TABLE monitorfish.vms
        SELECT
            *,
            (longitude, latitude)::Point AS geometry,
            geoToH3(latitude::Float64, longitude::Float64, 8) AS h3_8
        FROM file('monitorfish/vms_for_enrich_monitorfish_catches_test.csv')
    """
    )
    yield
    print("Dropping vms table")
    run_sql_script(sql="DROP TABLE monitorfish.vms")


@fixture
def init_facade_areas_subdivided(init_monitorfish):
    print("Creating facade_areas_subdivided table")
    print("Inserting test data into facade_areas_subdivided table")
    sync_table_with_pandas_flow.run(
        source_database="monitorfish_remote",
        query_filepath="monitorfish_remote/facade_areas_subdivided.sql",
        backend="geopandas",
        geom_col="geometry",
        destination_database="monitorfish",
        destination_table="facade_areas_subdivided_tmp",
        ddl_script_path="monitorfish/create_facade_areas_subdivided_tmp.sql",
        post_processing_script_path="monitorfish/post_process_facade_areas_subdivided.sql",
        final_table="facade_areas_subdivided",
    )

    print("Resetting facade areas dictionary")
    reset_dictionary_flow.run(
        database="monitorfish",
        dictionary="facade_areas_dict",
        ddl_script_path="monitorfish/create_facade_areas_dict.sql",
    )

    yield

    print("Dropping facade areas dictionary")
    run_sql_script(sql="DROP DICTIONARY monitorfish.facade_areas_dict")

    print("Dropping facade_areas_subdivided table")
    run_sql_script(sql="DROP TABLE monitorfish.facade_areas_subdivided")


@fixture
def init_ports(init_monitorfish):
    print("Creating monitorfish.ports table")
    print("Inserting test data into ports table")
    sync_table_with_pandas_flow.run(
        source_database="monitorfish_remote",
        query_filepath="monitorfish_remote/ports.sql",
        backend="pandas",
        destination_database="monitorfish",
        destination_table="ports",
        ddl_script_path="monitorfish/create_ports.sql",
    )

    yield

    print("Dropping monitorfish.ports table")
    run_sql_script(sql="DROP TABLE monitorfish.ports")


@fixture
def expected_catches_segment() -> dict:
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


@fixture
def expected_catches_segment_current_year(expected_catches_segment) -> dict:
    return {
        k: ("NO_SEGMENT" if v == "NO_SEGMENT" else f"{v}_current_year")
        for (k, v) in expected_catches_segment.items()
    }


@fixture
def expected_catches_positions() -> pd.DataFrame:
    return pd.DataFrame(
        {
            ""
            "catch_year": [
                2050,
                2050,
                2050,
                2050,
                2050,
                2050,
                2050,
                2050,
                2050,
                2050,
                2050,
                2050,
            ],
            "report_id": [
                "report_1",
                "report_2",
                "report_3a",
                "report_3b",
                "report_3c",
                "report_4a",
                "report_4b",
                "report_4c",
                "report_5",
                "report_6",
                "report_7",
                "report_8",
            ],
            "far_latitude": [
                45.5236,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            ],
            "far_longitude": [
                -1.5979999999999999,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            ],
            "vms_latitude": [
                45.119998931884766,
                None,
                39.5,
                39.5,
                39.5,
                None,
                None,
                47.2599983215332,
                54.400001525878906,
                -5.050000190734863,
                -5.050000190734863,
                None,
            ],
            "vms_longitude": [
                -0.9800000190734863,
                None,
                3.5,
                3.5,
                3.5,
                None,
                None,
                7.424999952316284,
                8.5,
                8.800000190734863,
                8.800000190734863,
                None,
            ],
            "facade": [
                "SA",
                None,
                "NAMO",
                "NAMO",
                "NAMO",
                None,
                None,
                "SA",
                None,
                None,
                None,
                None,
            ],
        },
    ).astype(
        {
            "catch_year": "int32",
            "report_id": "string[python]",
            "facade": "string[python]",
        }
    )


@fixture
def expected_catches_positions_and_facades() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "report_id": [
                "report_1",
                "report_2",
                "report_3a",
                "report_3b",
                "report_3c",
                "report_4a",
                "report_4b",
                "report_4c",
                "report_5",
                "report_6",
                "report_7",
                "report_8",
            ],
            "longitude": [
                -1.5979999999999999,
                -43.5,
                3.5,
                3.5,
                3.5,
                None,
                68.5,
                7.424999952316284,
                8.5,
                8.800000190734863,
                8.800000190734863,
                -1.1153902467699264,
            ],
            "latitude": [
                45.5236,
                39.75,
                39.5,
                39.5,
                39.5,
                None,
                48.75,
                47.2599983215332,
                54.400001525878906,
                -5.050000190734863,
                -5.050000190734863,
                45.5,
            ],
            "position_source": [
                "LOGBOOK",
                "ICES_SR",
                "VMS",
                "VMS",
                "VMS",
                "NONE",
                "ICES_SR",
                "VMS",
                "VMS",
                "VMS",
                "VMS",
                "PORT",
            ],
            "facade": [
                "SA",
                "NAMO",
                "NAMO",
                "NAMO",
                "NAMO",
                "Hors façade",
                "SA",
                "SA",
                "Hors façade",
                "Hors façade",
                "Hors façade",
                "SA",
            ],
            "landing_port_locode": [
                None,
                "FRCQF",
                "FRLEH",
                "FRLEH",
                "FRLEH",
                None,
                None,
                None,
                None,
                "FRZJZ",
                "FRBES",
                "FRBES",
            ],
            "landing_facade": [
                "Hors façade",
                "NAMO",
                "SA",
                "SA",
                "SA",
                "Hors façade",
                "Hors façade",
                "Hors façade",
                "Hors façade",
                "SA",
                "SA",
                "SA",
            ],
            "statistical_rectangle": [
                "E25A",
                "08A0",
                None,
                None,
                None,
                None,
                "26M8",
                "27M8",
                None,
                None,
                None,
                None,
            ],
        }
    ).astype(
        {
            "report_id": "string[python]",
            "facade": "string[python]",
            "landing_port_locode": "string[python]",
            "landing_facade": "string[python]",
            "statistical_rectangle": "string[python]",
        }
    )


def test_extract_extract_cfr_ranges(init_catches):
    id_ranges = extract_cfr_ranges.run(catch_year=2050, batch_size=5)
    assert id_ranges == [
        IdRange(id_min="ABC000306959", id_max="CFR000888888"),
    ]

    id_ranges = extract_cfr_ranges.run(catch_year=2050, batch_size=1)
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
    init_rectangles_stat_areas,
    init_vms,
    init_facade_areas_subdivided,
    init_ports,
    expected_catches_segment,
    expected_catches_segment_current_year,
    expected_catches_positions,
    expected_catches_positions_and_facades,
):
    positions_query = (
        "SELECT * " "FROM monitorfish.catches_positions " "ORDER BY report_id"
    )

    catches_query = (
        "SELECT "
        "ROW_NUMBER() OVER (ORDER BY report_id, species, gear DESC, fao_area) AS id, "
        "* "
        "FROM monitorfish.enriched_catches "
        "ORDER BY report_id, species, gear DESC, fao_area"
    )
    client = create_datawarehouse_client()

    # First run
    state = flow.run(
        years_ago=1,
        batch_size=3,
    )
    assert state.is_successful()

    enriched_catches_first_run = client.query_df(catches_query)
    positions_first_run = client.query_df(positions_query)
    catches_segment = enriched_catches_first_run.set_index("id")["segment"].to_dict()
    catches_segment_current_year = enriched_catches_first_run.set_index("id")[
        "segment_current_year"
    ].to_dict()
    assert catches_segment == expected_catches_segment
    assert catches_segment_current_year == expected_catches_segment_current_year

    landing_ports = (
        enriched_catches_first_run[["trip_number", "landing_port_locode"]]
        .drop_duplicates()
        .set_index("trip_number")
        .to_dict()["landing_port_locode"]
    )
    assert landing_ports == {
        "1": None,
        "Trip_2": "FRCQF",
        "3": "FRLEH",
        "Trip_4": None,
        "5": None,
        "Trip_6": "FRZJZ",
        "7": "FRBES",
        "8": "FRBES",
    }

    # Second run should arrive to the same result
    state = flow.run(
        years_ago=1,
        batch_size=3,
    )
    assert state.is_successful()

    enriched_catches_second_run = client.query_df(catches_query)
    positions_second_run = client.query_df(positions_query)
    pd.testing.assert_frame_equal(
        enriched_catches_second_run, enriched_catches_first_run
    )
    pd.testing.assert_frame_equal(
        enriched_catches_second_run[
            [
                "report_id",
                "longitude",
                "latitude",
                "position_source",
                "facade",
                "landing_port_locode",
                "landing_facade",
                "statistical_rectangle",
            ]
        ]
        .drop_duplicates()
        .reset_index(drop=True),
        expected_catches_positions_and_facades,
    )

    pd.testing.assert_frame_equal(positions_second_run, expected_catches_positions)
    expected_catches_positions
    pd.testing.assert_frame_equal(positions_first_run, expected_catches_positions)
