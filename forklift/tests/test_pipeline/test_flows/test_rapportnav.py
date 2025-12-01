
from unittest.mock import patch, MagicMock
from forklift.pipeline.flows.extract_rapportnav_analytics import extract_missions_ids, flow, chunk_list
from forklift.db_engines import create_datawarehouse_client
from datetime import datetime
from tests.mocks import replace_check_flow_not_running

replace_check_flow_not_running(flow)

def post_rapportnav_mock_factory():
    return {}

def test_extract_missions_ids():
    """
    Reads test data from monitorenv (table missions)
    """
    mission_ids = extract_missions_ids.run()
    assert len(mission_ids) > 0 
    assert isinstance(mission_ids, list)
    
def test_fetch_rapportnav_api():
    """
    Reads test data from monitorenv (table missions)
    """
    with patch('forklift.pipeline.flows.extract_rapportnav_analytics.requests.post') as mock_post:
        # Mock response
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "results": [
                {
                    "id":22,
                    "idUUID":None,
                    "serviceId":2,
                    "missionTypes":["SEA"],
                    "controlUnits":[{"id":10121,"administration":"DIRM \\/ DM","isArchived":False,"name":"PAM Jeanne Barret","resources":[],"contact":"00661"}],
                    "facade":"MEMN",
                    "startDateTimeUtc":"2025-01-06T07:00:00Z",
                    "endDateTimeUtc":"2025-01-17T17:00:00Z",
                    "isDeleted":False,
                    "missionSource":"MONITORFISH",
                    "activity.atSea.navigationDurationInHours":101.0,
                    "activity.atSea.anchoredDurationInHours":29.0,
                    "activity.atSea.totalDurationInHours":130.0,
                    "activity.atSea.nbControls":16.0,
                    "activity.docked.maintenanceDurationInHours":73.0,
                    "activity.docked.meteoDurationInHours":0.0,
                    "activity.docked.representationDurationInHours":0.0,
                    "activity.docked.adminFormationDurationInHours":0.0,
                    "activity.docked.mcoDurationInHours":0.0,
                    "activity.docked.otherDurationInHours":0.0,
                    "activity.docked.contrPolDurationInHours":0.0,
                    "activity.docked.totalDurationInHours":73.0,
                    "activity.docked.nbControls":0.0,
                    "activity.unavailable.technicalDurationInHours":48.0,
                    "activity.unavailable.personnelDurationInHours":24.0,
                    "activity.unavailable.totalDurationInHours":72.0,
                    "activity.unavailable.nbControls":0.0,
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


def test_chunk_list():
    """Unit test for chunk_list helper used to batch mission ids."""
    from forklift.pipeline.flows.extract_rapportnav_analytics import chunk_list

    # Regular splitting
    items = list(range(1, 11))
    batches = list(chunk_list(items, 3))
    assert batches == [[1, 2, 3], [4, 5, 6], [7, 8, 9], [10]]

    # batch_size equal to length
    batches = list(chunk_list(items, 10))
    assert batches == [items]

    # batch_size larger than length
    batches = list(chunk_list(items, 20))
    assert batches == [items]

    # batch_size == 1
    batches = list(chunk_list(items, 1))
    assert batches == [[i] for i in items]

    # empty input
    batches = list(chunk_list([], 5))
    assert batches == []

