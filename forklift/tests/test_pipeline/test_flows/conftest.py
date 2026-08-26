import uuid
from pathlib import Path

from pytest import fixture

from forklift.db_engines import create_datawarehouse_client
from forklift.pipeline.flows.sync_table_from_db_connection import (
    flow as sync_table_from_db_connection_flow,
)
from forklift.pipeline.helpers.generic import run_sql_script
from tests.mocks import replace_check_flow_not_running

replace_check_flow_not_running(sync_table_from_db_connection_flow)


@fixture
def init_analytics_controls_full_data():
    # fact_action_pam_ulam (rapport_pam_ulam_action.sql) unions nav with
    # monitorfish.analytics_controls_full_data / monitorenv.analytics_actions
    # + actions_infractions -- these 3 tables are normally built by the
    # sync_table_with_pandas flow (not sync_table_from_db_connection, which
    # is all this test suite runs), so they don't exist otherwise in the
    # test ClickHouse instance. Built here directly from the real DDL
    # scripts (same ones sync_table_with_pandas uses) + a couple of
    # PAM/ULAM test rows, on the same test mission (999100) as the other
    # ULAM fixtures, so fact_action_pam_ulam has real cross-source data to
    # union in tests.
    client = create_datawarehouse_client()
    client.command("CREATE DATABASE IF NOT EXISTS monitorfish")
    client.command("CREATE DATABASE IF NOT EXISTS monitorenv")

    run_sql_script(
        sql_script_filepath=Path("ddl/monitorfish/create_analytics_controls_full_data.sql"),
        parameters={"database": "monitorfish", "table": "analytics_controls_full_data"},
    )
    run_sql_script(
        sql_script_filepath=Path("ddl/monitorenv/create_analytics_actions.sql"),
        parameters={"database": "monitorenv", "table": "analytics_actions"},
    )
    run_sql_script(
        sql_script_filepath=Path("ddl/monitorenv/create_actions_infractions.sql"),
        parameters={"database": "monitorenv", "table": "actions_infractions"},
    )

    # FISH : 1 contrôle avec infraction WITH_RECORD (avec PV), sur la
    # mission de test 999100.
    client.command(
        """
        INSERT INTO monitorfish.analytics_controls_full_data
            (id, vessel_id, mission_id, control_unit_id, control_type,
             is_under_jdp, control_datetime_utc, control_year, control_unit,
             administration, vessel_name, facade, longitude, latitude,
             infraction, fishing_infraction, infraction_report,
             infraction_natinfs, infraction_count_with_record,
             infraction_count_without_record, infraction_count_pending,
             infraction_count_total)
        VALUES
            (999100, 999100, 999100, 999100, 'SEA_CONTROL',
             false, '2025-06-02 09:00:00', 2025, 'ULAM TEST 999100',
             'DDTM 33', 'Navire Test FISH', 'SA', -1.15, 44.85,
             1, 1, 1,
             ['12345'], 1,
             0, 0,
             1)
        """
    )

    # ENV : 1 action de contrôle avec infraction WITH_REPORT (avec PV),
    # même mission de test. UUID générés côté Python (pas generateUUIDv4()
    # côté ClickHouse : pattern non utilisé ailleurs dans ce repo, évité
    # plutôt que supposé -- cf. discussion en chat) pour rester
    # déterministe et éviter un aller-retour SELECT pour récupérer l'id
    # généré.
    env_action_id = str(uuid.uuid4())
    env_infraction_id = str(uuid.uuid4())
    client.command(
        f"""
        INSERT INTO monitorenv.analytics_actions
            (id, mission_id, action_start_datetime_utc, year,
             mission_start_datetime_utc, mission_end_datetime_utc,
             mission_type, action_type, mission_facade, control_unit_id,
             control_unit, administration, is_aff_mar, is_aem,
             administration_aem, action_facade, action_department,
             theme_level_1, longitude, latitude, infraction,
             number_of_controls)
        VALUES
            ('{env_action_id}', 999100, '2025-06-02 09:00:00', 2025,
             '2025-06-02 08:00:00', '2025-06-02 18:00:00',
             'CONTROL', 'CONTROL', 'SA', 999100,
             'ULAM TEST 999100', 'DDTM 33', 1, 1,
             'Affaires Maritimes', 'SA', 'Gironde',
             'Environnement marin', -1.15, 44.85, 1,
             1)
        """
    )
    client.command(
        f"""
        INSERT INTO monitorenv.actions_infractions
            (env_action_id, infraction_id, natinf, infraction_type, vessel_name)
        VALUES
            ('{env_action_id}', '{env_infraction_id}', [67890], 'WITH_REPORT', 'Navire Test ENV')
        """
    )

    yield

    print("Dropping monitorfish.analytics_controls_full_data / monitorenv.analytics_actions / monitorenv.actions_infractions")
    client.command("DROP TABLE IF EXISTS monitorfish.analytics_controls_full_data")
    client.command("DROP TABLE IF EXISTS monitorenv.analytics_actions")
    client.command("DROP TABLE IF EXISTS monitorenv.actions_infractions")


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
        query_filepath="data_warehouse/vessels.sql",
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
def init_mission_action(add_rapportnav_proxy_database):
    # Create table in data warehouse by syncing with mission_action in rapportnav database
    print("Creating rapportnav.mission_action table")
    state = sync_table_from_db_connection_flow.run(
        source_database="rapportnav_proxy",
        query_filepath=None,
        destination_database="rapportnav",
        destination_table="mission_action",
        order_by="id",
    )
    assert state.is_successful()
    client = create_datawarehouse_client()
    yield
    print("Dropping rapportnav.mission_action table")
    client.command("DROP TABLE IF EXISTS rapportnav.mission_action")
