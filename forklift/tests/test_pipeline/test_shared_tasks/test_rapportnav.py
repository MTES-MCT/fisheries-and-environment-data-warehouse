
import pandas as pd
from unittest.mock import patch, MagicMock

# Provide HEADERS to avoid NameError in rapportnav.py
HEADERS = {}

from forklift.pipeline.shared_tasks.rapportnav import _default_to_df, fetch_rapportnav_api


def test_default_to_df_with_flat_payload():
    payload = [{"a": 1}, {"a": 2}]
    df = _default_to_df(payload)
    assert isinstance(df, pd.DataFrame)
    assert df.shape[0] == 2


@patch("forklift.pipeline.shared_tasks.rapportnav.load_to_data_warehouse")
@patch("forklift.pipeline.shared_tasks.rapportnav.requests.get")
def test_fetch_rapportnav_api(mock_requests_get, mock_load):
    # Mock response
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = [{"a": 1}, {"a": 2}]
    
    mock_requests_get.return_value = mock_response

    rows = fetch_rapportnav_api.run(
        path="/api/v1/items",
        missions_ids=[1, 2]
    )