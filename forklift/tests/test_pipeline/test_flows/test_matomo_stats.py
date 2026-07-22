import json
from datetime import date
from unittest.mock import patch

import pandas as pd
import pytest
import requests
from clickhouse_connect.driver.exceptions import DatabaseError
from pytest import fixture

from forklift.config import (
    MATOMO_API_TOKEN,
    MATOMO_URL,
    MONITORFISH_MATOMO_SITE_ID,
    PROXIES,
)
from forklift.db_engines import create_datawarehouse_client
from forklift.pipeline.flows.matomo_stats import fetch_unique_visitors_per_month, flow
from tests.mocks import replace_check_flow_not_running

replace_check_flow_not_running(flow)


MATOMO_UNIQUE_VISITORS_RESPONSE = {
    "2025-01-01": 12,
    "2025-02-01": 34,
    "2025-03-01": 56,
}


def mock_matomo_post(url, params=None, timeout=None, proxies=None):
    response = requests.Response()
    response.status_code = 200
    response._content = json.dumps(MATOMO_UNIQUE_VISITORS_RESPONSE).encode()
    return response


@fixture
def drop_matomo_database():
    client = create_datawarehouse_client()
    yield
    client.command("DROP DATABASE IF EXISTS matomo")


def test_fetch_unique_visitors_per_month():
    with patch(
        "forklift.pipeline.flows.matomo_stats.requests.post",
        side_effect=mock_matomo_post,
    ) as mock_post:
        result = fetch_unique_visitors_per_month.run(
            start_date="2025-01-01", application="monitorfish"
        )

    mock_post.assert_called_once_with(
        f"{MATOMO_URL}/index.php",
        params={
            "module": "API",
            "method": "VisitsSummary.getUniqueVisitors",
            "idSite": MONITORFISH_MATOMO_SITE_ID,
            "period": "month",
            "date": f"2025-01-01,{date.today().isoformat()}",
            "format": "JSON",
            "token_auth": MATOMO_API_TOKEN,
        },
        timeout=30,
        proxies=PROXIES,
    )

    expected = pd.DataFrame(
        {
            "application": ["monitorfish", "monitorfish", "monitorfish"],
            "month": pd.to_datetime(["2025-01-01", "2025-02-01", "2025-03-01"]),
            "unique_visitors": [12, 34, 56],
        }
    )
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)


def test_fetch_unique_visitors_per_month_with_unknown_application_raises():
    with patch(
        "forklift.pipeline.flows.matomo_stats.requests.post",
        side_effect=mock_matomo_post,
    ):
        with pytest.raises(ValueError, match="Unknwon application some_application"):
            fetch_unique_visitors_per_month.run(
                start_date="2025-01-01", application="some_application"
            )


def test_matomo_stats(drop_matomo_database):
    client = create_datawarehouse_client()

    query = "SELECT * FROM matomo.monthly_unique_visitors ORDER BY month"

    # Initially the matomo database does not exist
    with pytest.raises(DatabaseError):
        client.query_df(query)

    with patch(
        "forklift.pipeline.flows.matomo_stats.requests.post",
        side_effect=mock_matomo_post,
    ):
        state = flow.run(start_date="2025-01-01", application="monitorfish")
    assert state.is_successful()

    monthly_unique_visitors_after_one_run = client.query_df(query)
    assert len(monthly_unique_visitors_after_one_run) == 3
    assert set(monthly_unique_visitors_after_one_run.application) == {"monitorfish"}
    assert list(monthly_unique_visitors_after_one_run.unique_visitors) == [12, 34, 56]

    # Running the flow again must replace the `monitorfish` partition rather than
    # duplicating its rows
    with patch(
        "forklift.pipeline.flows.matomo_stats.requests.post",
        side_effect=mock_matomo_post,
    ):
        state = flow.run(start_date="2025-01-01", application="monitorfish")
    assert state.is_successful()

    monthly_unique_visitors_after_two_runs = client.query_df(query)
    assert len(monthly_unique_visitors_after_two_runs) == 3
