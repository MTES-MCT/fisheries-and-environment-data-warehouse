from datetime import date, datetime, timedelta
from typing import List

import pandas as pd
import prefect
import pytz
from dateutil.relativedelta import relativedelta
from prefect import task

from forklift.pipeline.helpers import dates


@task(checkpoint=False)
def get_utcnow():
    """Task version of `datetime.utcnow`"""
    return datetime.utcnow()


@task(checkpoint=False)
def get_current_year() -> int:
    """Returns current year"""
    return datetime.utcnow().year


@task(checkpoint=False)
def get_timezone_aware_utcnow():
    return pytz.UTC.localize(datetime.utcnow())


@task(checkpoint=False)
def make_timedelta(**kwargs) -> timedelta:
    """Task version of `datetime.timedelta`"""
    return timedelta(**kwargs)


@task(checkpoint=False)
def make_periods(
    start_hours_ago: int,
    end_hours_ago: int,
    minutes_per_chunk: int,
    chunk_overlap_minutes: int,
) -> List[dates.Period]:
    """
    `prefect.Task` version of the function
    `forklift.pipeline.helpers.dates.make_periods`,with the difference that start and
    end dates are to be given as a number of hours from the current date (instead of
    `datetime` objects), and chunk duration and overlap are to be given as a number of
    minutes (instead of `timedelta` objects).
    This is to accomodate for the fact that Prefect flows' parameters must be
    JSON-serializable, and `datetime` and `timedelta` are not, by default.

    `forklift.pipeline.helpers.dates.make_periods` is recursive, hence the construction
    as a python function first and not directly as Prefect `Task`.

    See `forklift.pipeline.helpers.dates.make_periods` for help.
    """

    now = datetime.utcnow()

    return dates.make_periods(
        start_datetime_utc=now - timedelta(hours=start_hours_ago),
        end_datetime_utc=now - timedelta(hours=end_hours_ago),
        period_duration=timedelta(minutes=minutes_per_chunk),
        overlap=timedelta(minutes=chunk_overlap_minutes),
    )


@task(checkpoint=False)
def get_months_starts(
    now: datetime, start_months_ago: int, end_months_ago: int
) -> List[date]:
    """
    Returns a list of dates corresponding to the first days of the months between
    the month `start_months_ago months` months before `now` and
    the month `end_months_ago months` months before `now`.

    Args:
        now (datetime): Base date from which to compute the date range
        start_months_ago (int): Number of months before `now` to start the date range
        end_months_ago (int): Number of months before `now` to end the date range

    Returns:
        List[date]: List of dates corresponding to the first days of the months in the
        designated date range.
    """
    assert start_months_ago >= end_months_ago
    start_date = now.date().replace(day=1) - relativedelta(months=start_months_ago)
    end_date = now.date() - relativedelta(months=end_months_ago)
    months_starts = (
        pd.date_range(start=start_date, end=end_date, freq="MS")
        .to_pydatetime()
        .tolist()
    )
    logger = prefect.context.get("logger")
    months_list = ", ".join(m.strftime("%Y-%m") for m in months_starts)
    logger.info(f"Catches will be synced for months {months_list}.")
    return months_starts
