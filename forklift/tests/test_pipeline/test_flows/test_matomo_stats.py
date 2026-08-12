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
from forklift.pipeline.flows.matomo_stats import (
    fetch_daily_unique_visitors,
    fetch_monthly_users,
    flow,
)
from tests.mocks import replace_check_flow_not_running

replace_check_flow_not_running(flow)


MATOMO_UNIQUE_VISITORS_RESPONSE = {
    "2025-01-01": 12,
    "2025-01-02": 34,
    "2025-01-03": 56,
}

MATOMO_USERS_RESPONSE = {
    "2025-01-01": 5,
    "2025-02-01": 10,
    "2025-03-01": 15,
}

MATOMO_RESPONSES_BY_METHOD = {
    "VisitsSummary.getUniqueVisitors": MATOMO_UNIQUE_VISITORS_RESPONSE,
    "VisitsSummary.getUsers": MATOMO_USERS_RESPONSE,
}


def mock_matomo_post(url, params=None, timeout=None, proxies=None):
    response = requests.Response()
    response.status_code = 200
    response._content = json.dumps(
        MATOMO_RESPONSES_BY_METHOD[params["method"]]
    ).encode()
    return response


@fixture
def expected_daily_unique_visitors() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "application": ["monitorfish", "monitorfish", "monitorfish"],
            "day": [
                pd.Timestamp("2025-01-01 00:00:00"),
                pd.Timestamp("2025-01-02 00:00:00"),
                pd.Timestamp("2025-01-03 00:00:00"),
            ],
            "unique_visitors": [12, 34, 56],
        }
    )


@fixture
def expected_monthly_users() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "application": ["monitorfish", "monitorfish", "monitorfish"],
            "month": [
                pd.Timestamp("2025-01-01 00:00:00"),
                pd.Timestamp("2025-02-01 00:00:00"),
                pd.Timestamp("2025-03-01 00:00:00"),
            ],
            "users": [5, 10, 15],
        }
    )


@fixture
def drop_matomo_database():
    client = create_datawarehouse_client()
    yield
    client.command("DROP DATABASE IF EXISTS matomo")


def test_fetch_unique_visitors_per_month_with_unknown_application_raises():
    with patch(
        "forklift.pipeline.flows.matomo_stats.requests.post",
        side_effect=mock_matomo_post,
    ):
        with pytest.raises(ValueError, match="Unknwon application some_application"):
            fetch_daily_unique_visitors.run(
                start_date="2025-01-01", application="some_application"
            )


def test_fetch_monthly_users_with_unknown_application_raises():
    with patch(
        "forklift.pipeline.flows.matomo_stats.requests.post",
        side_effect=mock_matomo_post,
    ):
        with pytest.raises(ValueError, match="Unknwon application some_application"):
            fetch_monthly_users.run(
                start_date="2025-01-01", application="some_application"
            )


def test_matomo_stats(
    drop_matomo_database, expected_daily_unique_visitors, expected_monthly_users
):
    client = create_datawarehouse_client()

    unique_visitors_query = "SELECT * FROM matomo.daily_unique_visitors ORDER BY day"
    users_query = "SELECT * FROM matomo.monthly_users ORDER BY month"

    # Initially the matomo database does not exist
    with pytest.raises(DatabaseError):
        client.query_df(unique_visitors_query)
    with pytest.raises(DatabaseError):
        client.query_df(users_query)

    with patch(
        "forklift.pipeline.flows.matomo_stats.requests.post",
        side_effect=mock_matomo_post,
    ) as mock_post:
        state = flow.run(start_date="2025-01-01", application="monitorfish")
    assert state.is_successful()

    expected_date_range = f"2025-01-01,{date.today().isoformat()}"
    mock_post.assert_any_call(
        f"{MATOMO_URL}/index.php",
        params={
            "module": "API",
            "method": "VisitsSummary.getUniqueVisitors",
            "idSite": MONITORFISH_MATOMO_SITE_ID,
            "period": "day",
            "date": expected_date_range,
            "format": "JSON",
            "token_auth": MATOMO_API_TOKEN,
        },
        timeout=30,
        proxies=PROXIES,
    )
    mock_post.assert_any_call(
        f"{MATOMO_URL}/index.php",
        params={
            "module": "API",
            "method": "VisitsSummary.getUsers",
            "idSite": MONITORFISH_MATOMO_SITE_ID,
            "period": "month",
            "date": expected_date_range,
            "format": "JSON",
            "token_auth": MATOMO_API_TOKEN,
        },
        timeout=30,
        proxies=PROXIES,
    )

    daily_unique_visitors_after_one_run = client.query_df(unique_visitors_query)
    monthly_users_after_one_run = client.query_df(users_query)

    # Running the flow again must replace the `monitorfish` partition rather than
    # duplicating its rows
    with patch(
        "forklift.pipeline.flows.matomo_stats.requests.post",
        side_effect=mock_matomo_post,
    ):
        state = flow.run(start_date="2025-01-01", application="monitorfish")
    assert state.is_successful()

    daily_unique_visitors_after_two_runs = client.query_df(unique_visitors_query)
    monthly_users_after_two_runs = client.query_df(users_query)

    pd.testing.assert_frame_equal(
        daily_unique_visitors_after_one_run,
        expected_daily_unique_visitors,
        check_dtype=False,
    )
    pd.testing.assert_frame_equal(
        monthly_users_after_one_run, expected_monthly_users, check_dtype=False
    )
    pd.testing.assert_frame_equal(
        daily_unique_visitors_after_two_runs,
        expected_daily_unique_visitors,
        check_dtype=False,
    )
    pd.testing.assert_frame_equal(
        monthly_users_after_two_runs, expected_monthly_users, check_dtype=False
    )
