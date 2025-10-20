import json
from unittest.mock import patch, MagicMock

import pandas as pd

from forklift.pipeline.shared_tasks.rapportnav import _default_to_df, fetch_rapportnav_api


def test_default_to_df_with_results_key():
    payload = {"results": [{"a": 1}, {"a": 2}]}
    df = _default_to_df(payload)
    assert isinstance(df, pd.DataFrame)
    assert df.shape[0] == 2


@patch("forklift.pipeline.shared_tasks.rapportnav.requests.Session")
@patch("forklift.pipeline.shared_tasks.rapportnav.load_to_data_warehouse")
def test_fetch_rapportnav_api(mock_load, mock_session_cls):
    # Prepare mocked session
    session = MagicMock()
    mock_session_cls.return_value = session

    # Mock response returns 2 rows
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = {"results": [{"a": 1}, {"a": 2}]}

    session.get.return_value = mock_response

    rows = fetch_rapportnav_api.run(
        api_base="https://example.com",
        path="/api/v1/items",
        destination_database="dw",
        destination_table="rapportnav_items",
    )

    # Confirm load_to_data_warehouse was called with a DataFrame of 2 rows
    assert mock_load.called
    called_df = mock_load.call_args[0][0]
    assert isinstance(called_df, pd.DataFrame)
    assert called_df.shape[0] == 2
    assert rows == 2
