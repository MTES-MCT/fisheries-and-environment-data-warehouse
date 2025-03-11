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
    drop_table,
    enrich_monitorfish_catches,
    import_sacrois_data,
    landings,
    reset_proxy_pg_database,
    sync_table_from_db_connection,
    sync_table_with_pandas,
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
    drop_table_flow = deepcopy(drop_table.flow)
    enrich_monitorfish_catches_flow = deepcopy(enrich_monitorfish_catches.flow)
    reset_proxy_pg_database_flow = deepcopy(reset_proxy_pg_database.flow)
    import_sacrois_data_flow = deepcopy(import_sacrois_data.flow)
    landings_flow = deepcopy(landings.flow)
    sync_table_from_db_connection_flow = deepcopy(sync_table_from_db_connection.flow)
    sync_table_with_pandas_flow = deepcopy(sync_table_with_pandas.flow)

    catches_flow.schedule = CronSchedule("44 4 * * *")
    enrich_monitorfish_catches_flow.schedule = CronSchedule("04 5 * * *")
    clean_flow_runs_flow.schedule = CronSchedule("8,18,28,38,48,58 * * * *")
    landings_flow.schedule = CronSchedule("54 4 * * *")

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

    #################### List flows to register with prefect server ###################
    flows_to_register = [
        catches_flow,
        clean_flow_runs_flow,
        compute_sacrois_segments_flow,
        drop_table_flow,
        enrich_monitorfish_catches_flow,
        reset_proxy_pg_database_flow,
        import_sacrois_data_flow,
        landings_flow,
        sync_table_from_db_connection_flow,
        sync_table_with_pandas_flow,
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
