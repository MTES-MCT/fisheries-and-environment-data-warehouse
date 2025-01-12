from copy import deepcopy

from dotenv import dotenv_values
from prefect.run_configs.docker import DockerRun
from prefect.schedules import CronSchedule, Schedule, clocks
from prefect.storage.local import Local

from forklift.config import (
    FLOWS_LABEL,
    FLOWS_LOCATION,
    FORKLIFT_DOCKER_IMAGE,
    FORKLIFT_VERSION,
    ROOT_DIRECTORY,
)
from forklift.pipeline.flows import clean_flow_runs, reset_proxy_pg_database, sync_table


################################ Define flow schedules ################################
def get_flows_to_register():
    clean_flow_runs_flow = deepcopy(clean_flow_runs.flow)
    reset_proxy_pg_database_flow = deepcopy(reset_proxy_pg_database.flow)
    sync_table_flow = deepcopy(sync_table.flow)

    clean_flow_runs_flow.schedule = CronSchedule("8,18,28,38,48,58 * * * *")
    sync_table_flow.schedule = Schedule(
        clocks=[
            clocks.CronClock(
                "25 4 * * *",
                parameter_defaults={
                    "source_database": "monitorfish_proxy",
                    "source_table": "analytics_controls_full_data",
                    "destination_database": "monitorfish",
                    "destination_table": "analytics_controls_full_data",
                    "ddl_script_path": "monitorfish/create_analytics_controls_full_data.sql",
                },
            ),
            clocks.CronClock(
                "30 4 * * *",
                parameter_defaults={
                    "source_database": "monitorfish_proxy",
                    "source_table": "control_objectives",
                    "destination_database": "monitorfish",
                    "destination_table": "control_objectives",
                    "order_by": "year",
                },
            ),
            clocks.CronClock(
                "35 * * * *",
                parameter_defaults={
                    "source_database": "monitorenv_proxy",
                    "source_table": "analytics_actions",
                    "destination_database": "monitorenv",
                    "destination_table": "analytics_actions",
                    "ddl_script_path": "monitorenv/create_analytics_actions.sql",
                },
            ),
        ]
    )

    #################### List flows to register with prefect server ###################
    flows_to_register = [
        clean_flow_runs_flow,
        reset_proxy_pg_database_flow,
        sync_table_flow,
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
