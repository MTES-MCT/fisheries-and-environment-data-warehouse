
import pandas as pd
from unittest.mock import patch, MagicMock

# Provide HEADERS to avoid NameError in rapportnav.py
HEADERS = {}

from forklift.pipeline.shared_tasks.rapportnav import fetch_rapportnav_api, extract_missions_ids


def test_extract_missions_ids(add_monitorenv_proxy_database):
    mission_ids = extract_missions_ids.run()
    assert mission_ids
    
@patch("forklift.pipeline.shared_tasks.rapportnav.requests.get")
def test_fetch_rapportnav_api(mock_requests_get):
    # Mock response
    mock_response = MagicMock()
           
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = [
        {
            "id": 1,
            "idUUID": 1,
            "serviceId": 1,
            "startDateTimeUtc": "2024-01-01",
            "endDateTimeUtc": "2024-12-12",
            "missionTypes": "missionTypes",
            "controlUnits": "controlUnits",
            "facade": "facade",
            "isDeleted": False,
            "missionSource": "missionSource",
            "data": {
                "indicateur1": 1,
                "indicateur2": 2
            }
        }
    ]
    
    mock_requests_get.return_value = mock_response

    rows = fetch_rapportnav_api.run(
        path="/api/v1/items",
        missions_ids=[1, 2]
    )