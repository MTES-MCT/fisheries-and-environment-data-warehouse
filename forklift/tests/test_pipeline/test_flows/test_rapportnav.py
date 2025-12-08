from unittest.mock import MagicMock, patch

import pandas as pd
import pandas.api.types as ptypes

from forklift.db_engines import create_datawarehouse_client
from forklift.pipeline.flows.extract_rapportnav_analytics import (
    _process_data,
    extract_missions_ids,
    flow,
)
from tests.mocks import replace_check_flow_not_running

replace_check_flow_not_running(flow)


def post_rapportnav_mock_factory():
    return {}


def test__process_data():
    data = {
        "controlUnits": [[{"id": 10121, "name": "A"}, {"id": 20222, "name": "B"}]],
        "startDateTimeUtc": ["2025-01-06T07:00:00Z"],
        "endDateTimeUtc": ["2025-01-17T17:00:00Z"],
        "facade": [None],
        # Columns that should be removed (contain operationalSummary. / controlPolicies. and no 'total')
        "operationalSummary.foo": [1],
        "controlPolicies.bar": [2],
        # Column that contains 'total' should NOT be removed
        "operationalSummary.totalDuration": [999],
        # Column with dots that should be converted to underscores
        "activity.atSea.nbControls": [16.0],
    }

    df = pd.DataFrame(data)

    out = _process_data(df, 'patrol')

    # Removed temporary fields should not be present (after replacement dots->underscores)
    assert "operationalSummary_foo" not in out.columns
    assert "controlPolicies_bar" not in out.columns

    # Fields containing 'total' must be kept and dots replaced by underscores
    assert "operationalSummary_totalDuration" in out.columns
    assert out["operationalSummary_totalDuration"].iloc[0] == 999

    # Dots in column names should be replaced by underscores
    assert "activity_atSea_nbControls" in out.columns

    # controlUnits should be removed and controlUnitsIds created from the list of dicts
    assert "controlUnits" not in out.columns
    assert out["controlUnitsIds"].iloc[0] == [10121, 20222]

    # Datetime columns must be converted to pandas datetime dtype (tz-aware or tz-naive)
    assert ptypes.is_datetime64_any_dtype(
        out["startDateTimeUtc"]
    ) or ptypes.is_datetime64tz_dtype(out["startDateTimeUtc"])
    assert ptypes.is_datetime64_any_dtype(
        out["endDateTimeUtc"]
    ) or ptypes.is_datetime64tz_dtype(out["endDateTimeUtc"])

    # Facade nulls should be filled with the placeholder
    assert out["facade"].iloc[0] == "NON_RESEIGNE"

def test__process_data_aem():
    """Test that `_process_data_aem` expands the `data` list into id_title columns."""
    from forklift.pipeline.flows.extract_rapportnav_analytics import _process_data_aem

    # Row 0 has two items, row 1 has none
    data = [
        [
            {"id": "123", "title": "speed", "value": {"value": 12}},
            {"id": "456", "title": "depth", "value": 34},
        ],
        [],
    ]

    df = pd.DataFrame({"data": data})

    out = _process_data_aem(df)

    # Original 'data' column must be dropped
    assert "data" not in out.columns

    # New columns should be present with names id_title (underscore separator)
    assert "123_speed" in out.columns
    assert "456_depth" in out.columns

    # Values should be extracted and unwrapped when nested under {'value': ...}
    assert out.loc[0, "123_speed"] == 12
    assert out.loc[0, "456_depth"] == 34

    # Second row had empty list -> columns should exist but contain NaN
    assert pd.isna(out.loc[1, "123_speed"]) and pd.isna(out.loc[1, "456_depth"]) 

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
    with patch(
        "forklift.pipeline.flows.extract_rapportnav_analytics.requests.post"
    ) as mock_post:
        # Mock response
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "results": [
                {
                    "id": 22,
                    "idUUID": None,
                    "serviceId": 2,
                    "missionTypes": ["SEA"],
                    "controlUnits": [
                        {
                            "id": 10121,
                            "administration": "DIRM \\/ DM",
                            "isArchived": False,
                            "name": "PAM Jeanne Barret",
                            "resources": [],
                            "contact": "00661",
                        }
                    ],
                    "facade": "MEMN",
                    "startDateTimeUtc": "2025-01-06T07:00:00Z",
                    "endDateTimeUtc": "2025-01-17T17:00:00Z",
                    "isDeleted": False,
                    "missionSource": "MONITORFISH",
                    "activity.atSea.navigationDurationInHours": 101.0,
                    "activity.atSea.anchoredDurationInHours": 29.0,
                    "activity.atSea.totalDurationInHours": 130.0,
                    "activity.atSea.nbControls": 16.0,
                    "activity.docked.maintenanceDurationInHours": 73.0,
                    "activity.docked.meteoDurationInHours": 0.0,
                    "activity.docked.representationDurationInHours": 0.0,
                    "activity.docked.adminFormationDurationInHours": 0.0,
                    "activity.docked.mcoDurationInHours": 0.0,
                    "activity.docked.otherDurationInHours": 0.0,
                    "activity.docked.contrPolDurationInHours": 0.0,
                    "activity.docked.totalDurationInHours": 73.0,
                    "activity.docked.nbControls": 0.0,
                    "activity.unavailable.technicalDurationInHours": 48.0,
                    "activity.unavailable.personnelDurationInHours": 24.0,
                    "activity.unavailable.totalDurationInHours": 72.0,
                    "activity.unavailable.nbControls": 0.0,
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
