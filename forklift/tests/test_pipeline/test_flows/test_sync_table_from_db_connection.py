import pandas as pd
import pytest

from forklift.config import LIBRARY_LOCATION
from forklift.db_engines import create_datawarehouse_client
from forklift.pipeline.flows.sync_table_from_db_connection import flow
from tests.mocks import replace_check_flow_not_running

replace_check_flow_not_running(flow)

scheduled_runs = pd.read_csv(
    LIBRARY_LOCATION / "pipeline/flow_schedules/sync_table_from_db_connection.csv"
).drop(columns=["cron_string"])
parameters = ",".join(scheduled_runs.columns)
try:
    assert parameters == (
        "source_database,source_table,query_filepath,destination_database,"
        "destination_table,ddl_script_path,order_by"
    )
except AssertionError:
    raise ValueError("Test fixtures non coherent with CSV columns")

parameter_values = [
    tuple(r[1].where(r[1].notnull(), None)) for r in scheduled_runs.iterrows()
]


@pytest.mark.parametrize(parameters, parameter_values)
def test_sync_table_from_db_connection(
    request,
    add_monitorfish_proxy_database,
    add_monitorenv_proxy_database,
    add_rapportnav_proxy_database,
    source_database,
    source_table,
    query_filepath,
    destination_database,
    destination_table,
    ddl_script_path,
    order_by,
):
    print(
        f"Testing syncing of {destination_database}.{destination_table} from {source_database}.{source_table}"
    )
    client = create_datawarehouse_client()

    if destination_table in ("fact_action_pam_ulam", "fact_cible_pam_ulam"):
        # rapport_pam_ulam_action.sql AND rapport_pam_ulam_cible.sql both
        # union nav with monitorfish.analytics_controls_full_data /
        # monitorenv.analytics_actions + actions_infractions
        # (fact_action_pam_ulam absorbed the former fact_controle_pam_ulam,
        # cf. discussion en chat) -- these 3 tables aren't built by this
        # flow, cf. init_analytics_controls_full_data in conftest.py for why
        # and how they're set up here. rapport_pam_ulam_moyen.sql/
        # rapport_pam_ulam_mission.sql/rapport_pam_ulam_controle_croise.sql
        # don't reference these 3 tables in a real FROM clause (checked),
        # only in comments -- no fixture needed for those.
        request.getfixturevalue("init_analytics_controls_full_data")

    state = flow.run(
        source_database=source_database,
        source_table=source_table,
        query_filepath=query_filepath,
        destination_database=destination_database,
        destination_table=destination_table,
        ddl_script_path=ddl_script_path,
        order_by=order_by,
    )

    assert state.is_successful()

    df = client.query_df(
        (
            "SELECT * FROM "
            "{destination_database:Identifier}.{destination_table:Identifier}"
        ),
        parameters={
            "destination_database": destination_database,
            "destination_table": destination_table,
        },
    )

    assert len(df) > 0

    # dim_unit_reference is a shared dependency: missions_aem and all 5
    # rapport_pam_ulam_*.sql queries INNER JOIN it (via their own
    # pam_ulam_control_units CTE, rapportnav.dim_unit_reference),
    # cf. discussion en chat sur la centralisation du référentiel unité.
    # Contrairement aux autres tables de ce test (qui n'ont aucune
    # dépendance entre elles), la dropper ici casserait tous les tests
    # paramétrés suivants qui la joignent (table introuvable). On la garde
    # donc en vie pour le reste de la session de test -- son ordre dans
    # sync_table_from_db_connection.csv (1re ligne) garantit qu'elle est
    # créée avant d'être consommée, cf. pytest.mark.parametrize qui
    # préserve l'ordre du CSV. Le DROP TABLE IF EXISTS interne au flow
    # (drop_table_if_exists) la recrée proprement à chaque run réel.
    if destination_table != "dim_unit_reference":
        client.command(
            (
                "DROP TABLE "
                "{destination_database:Identifier}.{destination_table:Identifier}"
            ),
            parameters={
                "destination_database": destination_database,
                "destination_table": destination_table,
            },
        )
