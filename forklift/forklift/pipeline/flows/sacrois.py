from datetime import date
from pathlib import Path

import prefect
from prefect import Flow, Parameter, case, task

from forklift.db_engines import create_datawarehouse_client
from forklift.pipeline.entities.sacrois import SacroisFileImportSpec, SacroisFileType
from forklift.pipeline.shared_tasks.control_flow import check_flow_not_running
from forklift.pipeline.shared_tasks.generic import (
    create_database_if_not_exists,
    run_ddl_script,
)


@task(checkpoint=False)
def get_file_type(sacrois_file_type: str) -> SacroisFileType:
    return SacroisFileType(sacrois_file_type)


@task(checkpoint=False)
def get_ddl_script_path(file_type: SacroisFileType) -> Path:
    file_type_ddl_map = {
        SacroisFileType.NAVIRES_MOIS_MAREES_JOUR: Path(
            "sacrois/create_navires_mois_marees_jour_if_not_exists.sql"
        ),
        SacroisFileType.FISHING_ACTIVITY: Path(
            "sacrois/create_fishing_activity_if_not_exists.sql"
        ),
    }
    return file_type_ddl_map[file_type]


@task(checkpoint=False)
def get_sacrois_file_import_spec(
    file_type: SacroisFileType, year: int, month: int
) -> SacroisFileImportSpec:
    # Input validation
    d = date(year=year, month=month, day=1)
    year = d.year
    month = d.month
    partition = f"{year}{month:02}"

    filename_templates = {
        SacroisFileType.BMS: "BMS_{partition}.parquet",
        SacroisFileType.REJETS: "REJETS_{partition}.parquet",
        SacroisFileType.NAVIRES_MOIS_MAREES_JOUR: "NAVIRES_MOIS_MAREES_JOUR_{partition}.parquet",
        SacroisFileType.FISHING_ACTIVITY: "FISHING_ACTIVITY_{partition}.parquet",
    }

    filename = filename_templates[file_type].format(partition=partition)
    filepath = Path("sacrois") / partition / filename
    return SacroisFileImportSpec(
        filetype=file_type,
        year=year,
        month=month,
        partition=partition,
        filepath=filepath,
    )


@task(checkpoint=False)
def load_sacrois_data(import_spec: SacroisFileImportSpec):
    logger = prefect.context.get("logger")
    client = create_datawarehouse_client()
    logger.info(
        f"Droppping sacrois partition '{ import_spec.partition }' data warehouse."
    )
    client.command(
        "ALTER TABLE sacrois.{table:Identifier} DROP PARTITION {partition:String}",
        parameters={
            "table": import_spec.filetype.to_table_name(),
            "partition": import_spec.partition,
        },
    )
    logger.info(f"Importing { import_spec.filepath.name }")

    client.command(
        (
            "INSERT INTO sacrois.{table:Identifier} "
            "SELECT * FROM file({filepath:String}, Parquet);"
        ),
        parameters={
            "table": import_spec.filetype.to_table_name(),
            "filepath": import_spec.filepath.as_posix(),
        },
    )


with Flow("Import SACROIS") as flow:
    flow_not_running = check_flow_not_running()
    with case(flow_not_running, True):
        year = Parameter("year")
        month = Parameter("month", default=1)
        sacrois_file_type = Parameter("sacrois_file_type")
        file_type = get_file_type(sacrois_file_type)

        import_spec = get_sacrois_file_import_spec(
            file_type=file_type,
            year=year,
            month=month,
        )

        create_database = create_database_if_not_exists("sacrois")

        ddl_script_path = get_ddl_script_path(file_type)

        created_table = run_ddl_script(
            ddl_script_path,
            upstream_tasks=[create_database],
        )

        load_sacrois_data(import_spec=import_spec, upstream_tasks=[created_table])

flow.file_name = Path(__file__).name
