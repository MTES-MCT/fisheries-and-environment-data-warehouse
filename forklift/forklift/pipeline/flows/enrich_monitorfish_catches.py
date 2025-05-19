from pathlib import Path
from typing import List

import prefect
from prefect import Flow, Parameter, case, task, unmapped

from forklift.db_engines import create_datawarehouse_client
from forklift.pipeline.entities.generic import IdRange
from forklift.pipeline.helpers.generic import run_sql_script
from forklift.pipeline.helpers.processing import get_id_ranges
from forklift.pipeline.shared_tasks.control_flow import check_flow_not_running
from forklift.pipeline.shared_tasks.dates import get_current_year
from forklift.pipeline.shared_tasks.generic import drop_partition, run_ddl_scripts


@task(checkpoint=False)
def compute_catches_positions(catch_year: int):
    logger = prefect.context.get("logger")

    logger.info(f"Computing catches positions of year {catch_year}.")

    # breakpoint()
    # positions = extract("data_warehouse", query_filepath=Path("../sql_scripts/data_flows") / "monitorfish/compute_catches_positions.sql", params=dict( catch_year=catch_year, ), )
    run_sql_script(
        sql_script_filepath=Path("data_flows")
        / "monitorfish/compute_catches_positions.sql",
        parameters=dict(
            catch_year=catch_year,
        ),
    )


@task(checkpoint=False)
def extract_cfr_ranges(catch_year: int, batch_size: int) -> List[IdRange]:
    logger = prefect.context.get("logger")
    client = create_datawarehouse_client()
    cfrs = client.query_df(
        (
            "SELECT DISTINCT cfr "
            "FROM monitorfish.catches "
            "WHERE toYear(far_datetime_utc) = {catch_year:Integer} "
            "ORDER BY 1"
        ),
        parameters={"catch_year": catch_year},
    )

    if len(cfrs) == 0:
        return []

    cfr_ranges = get_id_ranges(ids=cfrs.cfr.tolist(), batch_size=batch_size)
    logger.info(
        (
            f"Found {len(cfrs)} vessels to enrich. "
            f"Returning {len(cfr_ranges)} batches of {batch_size} vessels."
        )
    )
    return cfr_ranges


@task(checkpoint=False)
def enrich_catches(cfr_range: IdRange, catch_year: int, current_year: int):
    logger = prefect.context.get("logger")
    logger.info(
        f"Enriching catches of vessels {cfr_range.id_min} to {cfr_range.id_max}."
    )

    run_sql_script(
        sql_script_filepath=Path("data_flows") / "monitorfish/enrich_catches.sql",
        parameters=dict(
            catch_year=catch_year,
            current_year=current_year,
            cfr_start=cfr_range.id_min,
            cfr_end=cfr_range.id_max,
        ),
    )


with Flow("Enrich Monitorfish catches") as flow:
    flow_not_running = check_flow_not_running()
    with case(flow_not_running, True):
        years_ago = Parameter("years_ago", default=0)
        batch_size = Parameter("batch_size", default=100)

        current_year = get_current_year()
        catch_year = current_year - years_ago

        catches_positions_created_table = run_ddl_scripts(
            "monitorfish/create_catches_positions_if_not_exists.sql"
        )

        catches_positions_dropped_partition = drop_partition(
            database="monitorfish",
            table="catches_positions",
            partition=catch_year,
            upstream_tasks=[catches_positions_created_table],
        )

        computed_catches_positions = compute_catches_positions(
            catch_year=catch_year,
            upstream_tasks=[catches_positions_dropped_partition],
        )

        enriched_catches_created_table = run_ddl_scripts(
            "monitorfish/create_enriched_catches_if_not_exists.sql",
            upstream_tasks=[computed_catches_positions],
        )

        enriched_catches_dropped_partition = drop_partition(
            database="monitorfish",
            table="enriched_catches",
            partition=catch_year,
            upstream_tasks=[enriched_catches_created_table],
        )

        cfr_ranges = extract_cfr_ranges(catch_year=catch_year, batch_size=batch_size)

        enrich_catches.map(
            cfr_range=cfr_ranges,
            catch_year=unmapped(catch_year),
            current_year=unmapped(current_year),
            upstream_tasks=[unmapped(enriched_catches_dropped_partition)],
        )

flow.file_name = Path(__file__).name
