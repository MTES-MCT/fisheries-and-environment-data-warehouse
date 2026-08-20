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

    if destination_table == "fact_mission_pam_ulam":
        print("DEBUG monitorenv missions:", client.query_df(
            "SELECT count() AS n, any(start_datetime_utc) AS start FROM monitorenv_proxy.missions WHERE id = 999100"
        ))
        print("DEBUG missions_control_units:", client.query_df(
            "SELECT count() AS n FROM monitorenv_proxy.missions_control_units WHERE mission_id = 999100"
        ))
        print("DEBUG control_units:", client.query_df(
            "SELECT count() AS n FROM monitorenv_proxy.control_units WHERE id = 999100"
        ))
        print("DEBUG rapportnav mission_general_info:", client.query_df(
            "SELECT count() AS n, any(mission_id) AS found_mission_id FROM rapportnav_proxy.mission_general_info WHERE mission_id = 999100"
        ))
        print("DEBUG mission_action (STATUS/CONTROL/...):", client.query_df(
            "SELECT count() AS n FROM rapportnav_proxy.mission_action WHERE mission_id = 999100"
        ))
        print("DEBUG the actual JOIN used by rapport_pam_ulam_mission.sql:", client.query_df(
            "SELECT count() AS n FROM rapportnav_proxy.mission_general_info mgi "
            "INNER JOIN monitorenv_proxy.missions envm ON envm.id = mgi.mission_id "
            "WHERE mgi.mission_id = 999100"
        ))

    if destination_table == "fact_moyen_pam_ulam":
        # V777.05-V777.09 (monitorenv) used to be a single file/transaction
        # where any one failing statement rolled back all 5 tables together,
        # masking which one actually failed (all showed empty). Now split
        # into 5 independent files -- check each table to see exactly which
        # one, if any, still fails.
        print("DEBUG monitorenv missions (999100):", client.query_df(
            "SELECT count() AS n FROM monitorenv_proxy.missions WHERE id = 999100"
        ))
        print("DEBUG monitorenv control_units (999100):", client.query_df(
            "SELECT count() AS n FROM monitorenv_proxy.control_units WHERE id = 999100"
        ))
        print("DEBUG monitorenv missions_control_units (999100):", client.query_df(
            "SELECT count() AS n FROM monitorenv_proxy.missions_control_units WHERE mission_id = 999100"
        ))
        print("DEBUG monitorenv bases (999100):", client.query_df(
            "SELECT count() AS n FROM monitorenv_proxy.bases WHERE id = 999100"
        ))
        print("DEBUG monitorenv control_unit_resources (999100/999101):", client.query_df(
            "SELECT count() AS n FROM monitorenv_proxy.control_unit_resources WHERE id IN (999100, 999101)"
        ))
        print("DEBUG mission_action_resource total (unfiltered):", client.query_df(
            "SELECT count() AS n FROM rapportnav_proxy.mission_action_resource"
        ))
        print("DEBUG mission_action_resource for our test resources:", client.query_df(
            "SELECT count() AS n FROM rapportnav_proxy.mission_action_resource WHERE resource_id IN (999100, 999101)"
        ))
        print("DEBUG mission_action for mission 999100:", client.query_df(
            "SELECT count() AS n FROM rapportnav_proxy.mission_action WHERE mission_id = 999100"
        ))
        print("DEBUG mission_action_resource joined to mission_action (used by rapport_pam_ulam_moyen.sql):", client.query_df(
            "SELECT count() AS n FROM rapportnav_proxy.mission_action_resource mar "
            "INNER JOIN rapportnav_proxy.mission_action ma ON ma.id = mar.action_id "
            "INNER JOIN monitorenv_proxy.missions envm ON envm.id = ma.mission_id "
            "WHERE ma.mission_id = 999100"
        ))
        print("DEBUG mission_action.control_type populated (needed since the grain change, V777.08):", client.query_df(
            "SELECT count() AS n FROM rapportnav_proxy.mission_action "
            "WHERE mission_id = 999100 AND control_type IS NOT NULL"
        ))

    if destination_table == "fact_cible_pam_ulam":
        print("DEBUG target_2 for our test action (999100/...0004):", client.query_df(
            "SELECT count() AS n FROM rapportnav_proxy.target_2 "
            "WHERE action_id = '99910000-0000-0000-0000-000000000004'"
        ))
        print("DEBUG control_2 has_been_done=true for our test target:", client.query_df(
            # Bare boolean predicate, not "= true" : rapportnav_proxy pushes
            # this WHERE down to the real Postgres column (genuine boolean
            # type there) -- ClickHouse renders a literal "true" comparison
            # as "= 1" in the generated Postgres SQL, and Postgres rejects
            # "boolean = integer" outright (caught by an actual CI run,
            # cf. discussion en chat -- pre-existing bug in this debug print,
            # never hit before since Docker/a live DB weren't reachable
            # earlier in this work). The production queries avoid this by
            # wrapping in coalesce(...) = true, which apparently takes a
            # different (working) pushdown path.
            "SELECT count() AS n FROM rapportnav_proxy.control_2 "
            "WHERE target_id = '99910100-0000-0000-0000-000000000001' AND has_been_done"
        ))
        print("DEBUG infraction_2 for our test controls:", client.query_df(
            "SELECT count() AS n, groupArray(infraction_type) AS types FROM rapportnav_proxy.infraction_2 "
            "WHERE control_id = '99910200-0000-0000-0000-000000000001'"
        ))

    if destination_table == "fact_controle_croise_pam_ulam":
        print("DEBUG service_control_unit (999001/999002):", client.query_df(
            "SELECT count() AS n FROM rapportnav_proxy.service_control_unit "
            "WHERE service_id IN (999001, 999002)"
        ))
        print("DEBUG inquiry rows (service 999001/999002):", client.query_df(
            "SELECT count() AS n FROM rapportnav_proxy.inquiry WHERE service_id IN (999001, 999002)"
        ))
        print("DEBUG dim_unit_reference PAM/ULAM test units (999100/999102):", client.query_df(
            "SELECT control_unit_id, unit_type FROM rapportnav.dim_unit_reference "
            "WHERE control_unit_id IN (999100, 999102)"
        ))
        print("DEBUG the actual JOIN used by rapport_pam_ulam_controle_croise.sql:", client.query_df(
            "SELECT count() AS n FROM rapportnav_proxy.inquiry i "
            "INNER JOIN rapportnav_proxy.service_control_unit scu ON scu.service_id = i.service_id "
            "INNER JOIN monitorenv_proxy.control_units cu ON cu.id = scu.control_unit_id "
            "INNER JOIN rapportnav.dim_unit_reference uu ON uu.control_unit_id = cu.id AND uu.unit_type IN ('PAM', 'ULAM') "
            "WHERE i.service_id IN (999001, 999002)"
        ))

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

    # dim_unit_reference is a shared dependency: missions_aem and the 3
    # fact_*_pam_ulam queries LEFT JOIN it directly (rapportnav.dim_unit_reference),
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
