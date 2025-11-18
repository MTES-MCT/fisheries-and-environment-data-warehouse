
from unittest.mock import patch, MagicMock
from forklift.pipeline.flows.extract_rapportnav_analytics import extract_missions_ids, flow
from forklift.db_engines import create_datawarehouse_client
from datetime import datetime
from tests.mocks import replace_check_flow_not_running

replace_check_flow_not_running(flow)

def post_rapportnav_mock_factory():
    return {}


def test_extract_missions_ids(add_monitorenv_proxy_database):
    mission_ids = extract_missions_ids.run()
    assert len(mission_ids) > 0 
    
def test_fetch_rapportnav_api():
    with patch('forklift.pipeline.flows.extract_rapportnav_analytics.requests.post') as mock_post:
        # Mock response
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "results": [
                {
                    "id": 1,
                    "startDateTimeUtc": datetime(2024, 1, 1),
                }
            ]
        }
        mock_post.return_value = mock_response


        state = flow.run()
        assert state.is_successful() 

    client = create_datawarehouse_client()
    df = client.query_df(
        (
            "SELECT * FROM "
            "{destination_database:Identifier}.{destination_table:Identifier}"
        ),
        parameters={
            "destination_database": "rapportnav",
            "destination_table": "patrol",
        },
    )


    assert len(df) > 0