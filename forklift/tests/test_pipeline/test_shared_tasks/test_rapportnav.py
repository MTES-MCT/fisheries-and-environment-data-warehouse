
import pandas as pd
from unittest.mock import patch, MagicMock

# Provide HEADERS to avoid NameError in rapportnav.py
HEADERS = {}

from forklift.pipeline.flows.extract_rapportnav_analytics import fetch_rapportnav_api, extract_missions_ids


def test_extract_missions_ids(add_monitorenv_proxy_database):
    mission_ids = extract_missions_ids.run()
    assert len(mission_ids) > 0 
    
@patch("forklift.pipeline.flows.extract_rapportnav_analytics.requests.post")
def test_fetch_rapportnav_api(mock_requests_post):
    # Mock response
    mock_response = MagicMock()
           
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = {
        "results": [
            {
                "id": 1,
                "idUUID": 1,
                "serviceId": 1,
                "startDateTimeUtc": "2024-01-01",
            }
        ]
    }
    
    mock_requests_post.return_value = mock_response

    df = fetch_rapportnav_api.run(
        path="/api/v1/items",
        missions_ids=[1, 2]
    )

    assert len(df) > 0
    assert df.id[0] == 1