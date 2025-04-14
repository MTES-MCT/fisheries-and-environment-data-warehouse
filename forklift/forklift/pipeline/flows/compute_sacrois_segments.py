from datetime import date
from pathlib import Path
from typing import List

import prefect
from prefect import Flow, Parameter, case, task, unmapped

from forklift.db_engines import create_datawarehouse_client
from forklift.pipeline.entities.generic import IdRange
from forklift.pipeline.entities.sacrois import SacroisPartition
from forklift.pipeline.helpers.generic import run_sql_script
from forklift.pipeline.helpers.processing import get_id_ranges
from forklift.pipeline.shared_tasks.control_flow import check_flow_not_running
from forklift.pipeline.shared_tasks.generic import run_ddl_scripts


@task(checkpoint=False)
def get_partition(processing_year: int, processing_month: int) -> SacroisPartition:
    # Input validation
    d = date(year=processing_year, month=processing_month, day=1)
    processing_year = d.year
    processing_month = d.month
    name = f"{processing_year}{processing_month:02}"

    return SacroisPartition(name=name, processing_date=d)


@task(checkpoint=False)
def drop_partition(partition: SacroisPartition):
    logger = prefect.context.get("logger")
    client = create_datawarehouse_client()
    logger.info(
        (
            f"Dropping partition {partition.name} "
            "from table sacrois.segmented_fishing_activity"
        )
    )
    client.command(
        (
            "ALTER TABLE sacrois.segmented_fishing_activity "
            "DROP PARTITION {partition:String}"
        ),
        parameters={
            "partition": partition.name,
        },
    )


@task(checkpoint=False)
def get_trip_id_ranges(partition: SacroisPartition, batch_size: int) -> List[IdRange]:
    logger = prefect.context.get("logger")
    client = create_datawarehouse_client()
    trip_ids = client.query_df(
        (
            "SELECT DISTINCT TRIP_ID "
            "FROM sacrois.fishing_activity "
            "WHERE PROCESSING_DATE = {processing_date:Date}"
            "ORDER BY 1"
        ),
        parameters={"processing_date": partition.processing_date},
    )
    trip_ids = trip_ids.TRIP_ID.tolist()
    trip_id_ranges = get_id_ranges(ids=trip_ids, batch_size=batch_size)
    logger.info(
        (
            f"Found {len(trip_ids)} trips to segment. "
            f"Returning {len(trip_id_ranges)} batches of {batch_size} trips."
        )
    )
    return trip_id_ranges


@task(checkpoint=False)
def compute_segments(
    trip_id_range: IdRange, partition: SacroisPartition, segments_year: int
):
    logger = prefect.context.get("logger")
    logger.info(
        f"Computing segments of trips {trip_id_range.id_min} to {trip_id_range.id_max}."
    )

    run_sql_script(
        sql_script_filepath=Path("data_flows")
        / "sacrois/compute_fishing_activity_segments.sql",
        parameters=dict(
            processing_date=partition.processing_date,
            id_min=trip_id_range.id_min,
            id_max=trip_id_range.id_max,
            segments_year=segments_year,
        ),
    )


with Flow("Compute SACROIS segments") as flow:
    flow_not_running = check_flow_not_running()
    with case(flow_not_running, True):
        processing_year = Parameter("processing_year")
        segments_year = Parameter("segments_year")
        processing_month = Parameter("processing_month", default=1)
        batch_size = Parameter("batch_size", default=10000)

        created_table = run_ddl_scripts(
            "sacrois/create_segmented_fishing_activity_if_not_exists.sql"
        )

        partition = get_partition(
            processing_year=processing_year, processing_month=processing_month
        )
        dropped_partition = drop_partition(partition, upstream_tasks=[created_table])

        trip_id_ranges = get_trip_id_ranges(partition=partition, batch_size=batch_size)

        compute_segments.map(
            trip_id_range=trip_id_ranges,
            partition=unmapped(partition),
            segments_year=unmapped(segments_year),
            upstream_tasks=[unmapped(dropped_partition)],
        )

flow.file_name = Path(__file__).name
