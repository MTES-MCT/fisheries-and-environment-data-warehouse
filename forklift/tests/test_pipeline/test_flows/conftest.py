from pytest import fixture

from forklift.db_engines import create_datawarehouse_client
from forklift.pipeline.flows.sync_table_from_db_connection import (
    flow as sync_table_from_db_connection_flow,
)
from tests.mocks import mock_check_flow_not_running

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
