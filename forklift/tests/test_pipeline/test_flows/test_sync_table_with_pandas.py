import pandas as pd
import pytest

from forklift.config import LIBRARY_LOCATION
from forklift.db_engines import create_datawarehouse_client
from forklift.pipeline.flows.reset_dictionary import flow as reset_dict_flow
from forklift.pipeline.flows.sync_table_with_pandas import flow
from tests.mocks import replace_check_flow_not_running

replace_check_flow_not_running(flow)
replace_check_flow_not_running(reset_dict_flow)


scheduled_runs = pd.read_csv(
    LIBRARY_LOCATION / "pipeline/flow_schedules/sync_table_with_pandas.csv"
).drop(columns=["cron_string"])
parameters = ",".join(scheduled_runs.columns)
try:
    assert parameters == (
        "source_database,query_filepath,schema,table_name,backend,geom_col,"
        "destination_database,destination_table,ddl_script_path,"
        "post_processing_script_path,final_table"
    )
except AssertionError:
    raise ValueError("Test fixtures non coherent with CSV columns")

parameter_values = [
    tuple(r[1].where(r[1].notnull(), None)) for r in scheduled_runs.iterrows()
]


@pytest.fixture
def drop_db(destination_database):
    client = create_datawarehouse_client()
    yield
    print("Dropping databases")
    client.command("DROP DATABASE IF EXISTS monitorfish")
    client.command("DROP DATABASE IF EXISTS monitorenv")


@pytest.mark.parametrize(parameters, parameter_values)
def test_sync_table_with_pandas(
    add_monitorenv_proxy_database,
    drop_db,
    source_database,
    query_filepath,
    schema,
    table_name,
    backend,
    geom_col,
    destination_database,
    destination_table,
    ddl_script_path,
    post_processing_script_path,
    final_table,
):
    client = create_datawarehouse_client()

    state = flow.run(
        source_database=source_database,
        query_filepath=query_filepath,
        schema=schema,
        table_name=table_name,
        backend=backend,
        geom_col=geom_col,
        destination_database=destination_database,
        destination_table=destination_table,
        ddl_script_path=ddl_script_path,
        post_processing_script_path=post_processing_script_path,
        final_table=final_table,
    )

    assert state.is_successful()

    # Check data is loaded to the desired table and no other table stays lingering
    expected_table = final_table or destination_table
    tables_in_db = client.query_df(
        "SHOW TABLES FROM {database:Identifier}",
        parameters={
            "database": destination_database,
        },
    )

    pd.testing.assert_frame_equal(
        tables_in_db, pd.DataFrame({"name": [expected_table]}), check_dtype=False
    )

    df = client.query_df(
        ("SELECT * FROM " "{database:Identifier}.{table:Identifier}"),
        parameters={
            "database": destination_database,
            "table": expected_table,
        },
    )

    assert len(df) > 0

    if final_table == "non_overlapping_fao_areas":
        state = reset_dict_flow.run(
            database="monitorfish",
            dictionary="fao_areas_dict",
            ddl_script_path="monitorfish/create_fao_areas_dict.sql",
        )
        assert state.is_successful()

        q = "SELECT dictGet(monitorfish.fao_areas_dict, 'f_code', (0, 45)) AS fao_area"

        area_from_dict_1 = client.query_df(q)

        # Re-running should yield the same result
        state = reset_dict_flow.run(
            database="monitorfish",
            dictionary="fao_areas_dict",
            ddl_script_path="monitorfish/create_fao_areas_dict.sql",
        )
        assert state.is_successful()

        area_from_dict_2 = client.query_df(q)

        expected_fao_areas = pd.DataFrame({"fao_area": ["27.8"]})
        assert state.is_successful()

        pd.testing.assert_frame_equal(
            area_from_dict_1, expected_fao_areas, check_dtype=False
        )
        pd.testing.assert_frame_equal(
            area_from_dict_2, expected_fao_areas, check_dtype=False
        )

    if final_table == "eez_areas":
        state = reset_dict_flow.run(
            database="monitorfish",
            dictionary="eez_areas_dict",
            ddl_script_path="monitorfish/create_eez_areas_dict.sql",
        )
        assert state.is_successful()

        q = "SELECT dictGet(monitorfish.eez_areas_dict, 'iso_sov1', (5, 47)) AS eez_area"

        area_from_dict_1 = client.query_df(q)

        # Re-running should yield the same result
        state = reset_dict_flow.run(
            database="monitorfish",
            dictionary="eez_areas_dict",
            ddl_script_path="monitorfish/create_eez_areas_dict.sql",
        )
        assert state.is_successful()

        area_from_dict_2 = client.query_df(q)

        expected_eez_areas = pd.DataFrame({"eez_area": ["EST"]})
        assert state.is_successful()

        pd.testing.assert_frame_equal(
            area_from_dict_1, expected_eez_areas, check_dtype=False
        )
        pd.testing.assert_frame_equal(
            area_from_dict_2, expected_eez_areas, check_dtype=False
        )

    if final_table == "rectangles_stat_areas":
        state = reset_dict_flow.run(
            database="monitorfish",
            dictionary="rectangles_stat_areas_dict",
            ddl_script_path="monitorfish/create_rectangles_stat_areas_dict.sql",
        )
        assert state.is_successful()

        q = "SELECT dictGet(monitorfish.rectangles_stat_areas_dict, 'icesname', (65.5, -53.25)) AS stat_rectangle"

        area_from_dict_1 = client.query_df(q)

        # Re-running should yield the same result
        state = reset_dict_flow.run(
            database="monitorfish",
            dictionary="rectangles_stat_areas_dict",
            ddl_script_path="monitorfish/create_rectangles_stat_areas_dict.sql",
        )
        assert state.is_successful()

        area_from_dict_2 = client.query_df(q)

        expected_stat_rectangle = pd.DataFrame({"stat_rectangle": ["484"]})
        assert state.is_successful()

        pd.testing.assert_frame_equal(
            area_from_dict_1, expected_stat_rectangle, check_dtype=False
        )
        pd.testing.assert_frame_equal(
            area_from_dict_2, expected_stat_rectangle, check_dtype=False
        )

    if final_table == "facade_areas_subdivided":
        state = reset_dict_flow.run(
            database="monitorfish",
            dictionary="facade_areas_dict",
            ddl_script_path="monitorfish/create_facade_areas_dict.sql",
        )
        assert state.is_successful()

        q = "SELECT dictGet(monitorfish.facade_areas_dict, 'facade', (0, 25)) AS facade"

        area_from_dict_1 = client.query_df(q)

        # Re-running should yield the same result
        state = reset_dict_flow.run(
            database="monitorfish",
            dictionary="rectangles_stat_areas_dict",
            ddl_script_path="monitorfish/create_rectangles_stat_areas_dict.sql",
        )
        assert state.is_successful()

        area_from_dict_2 = client.query_df(q)

        expected_facade = pd.DataFrame({"facade": ["NAMO"]})
        assert state.is_successful()

        pd.testing.assert_frame_equal(
            area_from_dict_1, expected_facade, check_dtype=False
        )
        pd.testing.assert_frame_equal(
            area_from_dict_2, expected_facade, check_dtype=False
        )
