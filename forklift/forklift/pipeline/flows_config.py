from copy import deepcopy

import pandas as pd
from dotenv import dotenv_values
from prefect.run_configs.docker import DockerRun
from prefect.schedules import CronSchedule, Schedule, clocks
from prefect.storage.local import Local

from forklift.config import (
    FLOWS_LABEL,
    FLOWS_LOCATION,
    FORKLIFT_DOCKER_IMAGE,
    FORKLIFT_VERSION,
    LIBRARY_LOCATION,
    ROOT_DIRECTORY,
)
from forklift.pipeline.flows import (
    catches,
    clean_flow_runs,
    compute_sacrois_segments,
    cps,
    discards,
    drop_table,
    enrich_monitorfish_catches,
    import_sacrois_data,
    landings,
    pnos,
    reset_dictionary,
    reset_proxy_pg_database,
    sync_geo_table_to_h3_table,
    sync_table_from_db_connection,
    sync_table_with_pandas,
    vms,
)


def make_cron_clock_from_run_param_series(s: pd.Series) -> clocks.CronClock:
    d = s.dropna().to_dict()
    cron_string = d.pop("cron_string")
    return clocks.CronClock(cron=cron_string, parameter_defaults=d)


################################ Define flow schedules ################################
def get_flows_to_register():
    catches_flow = deepcopy(catches.flow)
    clean_flow_runs_flow = deepcopy(clean_flow_runs.flow)
    compute_sacrois_segments_flow = deepcopy(compute_sacrois_segments.flow)
    cps_flow = deepcopy(cps.flow)
    discards_flow = deepcopy(discards.flow)
    drop_table_flow = deepcopy(drop_table.flow)
    enrich_monitorfish_catches_flow = deepcopy(enrich_monitorfish_catches.flow)
    reset_proxy_pg_database_flow = deepcopy(reset_proxy_pg_database.flow)
    import_sacrois_data_flow = deepcopy(import_sacrois_data.flow)
    landings_flow = deepcopy(landings.flow)
    pnos_flow = deepcopy(pnos.flow)
    reset_dictionary_flow = deepcopy(reset_dictionary.flow)
    sync_geo_table_to_h3_table_flow = deepcopy(sync_geo_table_to_h3_table.flow)
    sync_table_from_db_connection_flow = deepcopy(sync_table_from_db_connection.flow)
    sync_table_with_pandas_flow = deepcopy(sync_table_with_pandas.flow)
    vms_flow = deepcopy(vms.flow)

    catches_flow.schedule = CronSchedule("44 4 * * *")
    cps_flow.schedule = CronSchedule("41 4 * * *")
    discards_flow.schedule = CronSchedule("35 4 * * *")
    enrich_monitorfish_catches_flow.schedule = CronSchedule("04 5 * * *")
    clean_flow_runs_flow.schedule = CronSchedule("8,18,28,38,48,58 * * * *")
    landings_flow.schedule = CronSchedule("54 4 * * *")
    pnos_flow.schedule = CronSchedule("55 4 * * *")
    reset_dictionary_flow.schedule = Schedule(
        clocks=[
            clocks.CronClock(
                "48 1 1 * *",
                parameter_defaults={
                    "database": "monitorfish",
                    "dictionary": "fao_areas_dict",
                    "ddl_script_path": "monitorfish/create_fao_areas_dict.sql",
                },
            )
        ]
    )
    sync_geo_table_to_h3_table_flow.schedule = Schedule(
        clocks=[
            clocks.CronClock(
                "8 5 3,10,17,24 * *",
                parameter_defaults={
                    "source_database": "monitorfish_remote",
                    "query_filepath": "monitorfish_remote/regulations.sql",
                    "geometry_column": "geometry_simplified",
                    "crs": 4326,
                    "resolution": 8,
                    "ddl_script_paths": "monitorfish/create_regulations_h3.sql",
                    "destination_database": "monitorfish",
                    "destination_table": "regulations_h3",
                    "batch_size": 10,
                },
            ),
            clocks.CronClock(
                "8 5 4,11,18,25 * *",
                parameter_defaults={
                    "source_database": "monitorenv_remote",
                    "query_filepath": "monitorenv_remote/amp_cacem.sql",
                    "geometry_column": "geom",
                    "crs": 4326,
                    "resolution": 8,
                    "ddl_script_paths": "monitorenv/create_amp_cacem_h3.sql",
                    "destination_database": "monitorenv",
                    "destination_table": "amp_cacem_h3",
                    "batch_size": 50,
                },
            ),
            clocks.CronClock(
                "8 5 5,12,19,26 * *",
                parameter_defaults={
                    "source_database": "monitorenv_remote",
                    "query_filepath": "monitorenv_remote/regulations_cacem_h3.sql",
                    "geometry_column": "geom",
                    "crs": 4326,
                    "resolution": 8,
                    "ddl_script_paths": "monitorenv/create_regulations_cacem_h3.sql",
                    "destination_database": "monitorenv",
                    "destination_table": "regulations_cacem_h3",
                    "batch_size": 10,
                },
            ),
        ]
    )

    scheduled_runs = pd.read_csv(
        LIBRARY_LOCATION / "pipeline/flow_schedules/sync_table_from_db_connection.csv"
    )
    sync_table_from_db_connection_flow.schedule = Schedule(
        clocks=[
            make_cron_clock_from_run_param_series(s=run[1])
            for run in scheduled_runs.iterrows()
        ]
    )

    scheduled_runs = pd.read_csv(
        LIBRARY_LOCATION / "pipeline/flow_schedules/sync_table_with_pandas.csv"
    )
    sync_table_with_pandas_flow.schedule = Schedule(
        clocks=[
            make_cron_clock_from_run_param_series(s=run[1])
            for run in scheduled_runs.iterrows()
        ]
    )

    vms_flow.schedule = Schedule(
        clocks=[
            # Sync positions of only the current month every day...
            clocks.CronClock("4 5 2-7,9-31 * *"),
            # ...except once a week for the first two weeks of the month, positions of
            # the previous month are synced too, in order to import "late" positions.
            clocks.CronClock(
                "4 5 1,8 * *",
                parameter_defaults={"start_months_ago": 1, "end_months_ago": 0},
            ),
        ]
    )

    #################### List flows to register with prefect server ###################
    flows_to_register = [
        catches_flow,
        clean_flow_runs_flow,
        compute_sacrois_segments_flow,
        catches_flow,
        drop_table_flow,
        discards_flow,
        enrich_monitorfish_catches_flow,
        reset_proxy_pg_database_flow,
        import_sacrois_data_flow,
        landings_flow,
        pnos_flow,
        reset_dictionary_flow,
        sync_geo_table_to_h3_table_flow,
        sync_table_from_db_connection_flow,
        sync_table_with_pandas_flow,
        vms_flow,
    ]

    ############################## Define flows' storage ##############################
    # This defines where the executor can find the flow.py file for each flow
    # **inside** the container.
    for flow in flows_to_register:
        flow.storage = Local(
            add_default_labels=False,
            stored_as_script=True,
            path=(FLOWS_LOCATION / flow.file_name).as_posix(),
        )

    ############################ Define flows' run config #############################
    for flow in flows_to_register:
        host_config = None

        flow.run_config = DockerRun(
            image=f"{FORKLIFT_DOCKER_IMAGE}:{FORKLIFT_VERSION}",
            host_config=host_config,
            env=dotenv_values(ROOT_DIRECTORY / ".env"),
            labels=[FLOWS_LABEL],
        )

    return flows_to_register
