from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Optional
from unittest.mock import MagicMock, patch

import pandas as pd
import requests
from prefect import Flow, task

from forklift.pipeline.helpers.generic import extract
from forklift.pipeline.shared_tasks.datagouv import update_resource


def mock_extract_side_effect(
    db_name: str,
    query_filepath: Path | str,
    dtypes: Optional[dict] = None,
    parse_dates: Optional[list | dict] = None,
    params: Optional[dict] = None,
    backend: str = "pandas",
    geom_col: str = "geom",
    crs: Optional[int] = None,
):
    @patch("forklift.read_query.pd")
    @patch("forklift.read_query.create_engine")
    def mock_extract_side_effect_(
        db_name,
        query_filepath,
        dtypes,
        parse_dates,
        params,
        mock_create_engine,
        mock_pd,
    ):
        def read_sql_mock(query, engine, **kwargs):
            return query

        mock_pd.read_sql.side_effect = read_sql_mock

        return extract(
            db_name=db_name,
            query_filepath=query_filepath,
            dtypes=None,
            parse_dates=parse_dates,
            params=params,
        )

    return mock_extract_side_effect_(
        db_name, query_filepath, dtypes, parse_dates, params
    )


def mock_datetime_utcnow(utcnow: datetime):
    mock_datetime = MagicMock()
    mock_datetime.utcnow = MagicMock(return_value=utcnow)
    return mock_datetime


def get_utcnow_mock_factory(utcnow: datetime):
    @task(checkpoint=False)
    def mock_get_utcnow():
        return utcnow

    return mock_get_utcnow


@task(checkpoint=False)
def mock_check_flow_not_running():
    return True


def replace_check_flow_not_running(f: Flow) -> Flow:
    if f.get_tasks("check_flow_not_running"):
        f.replace(f.get_tasks("check_flow_not_running")[0], mock_check_flow_not_running)


@task(checkpoint=False)
def mock_update_resource(
    dataset_id: str,
    resource_id: str,
    resource_title: str,
    resource: BytesIO,
    mock_update: bool,
) -> pd.DataFrame:
    def return_200(url, **kwargs):
        r = requests.Response()
        r.status_code = 200
        r.url = url
        return r

    with patch("forklift.pipeline.shared_tasks.datagouv.requests.post", return_200):
        return update_resource.run(
            dataset_id=dataset_id,
            resource_id=resource_id,
            resource_title=resource_title,
            resource=resource,
            mock_update=mock_update,
        )
