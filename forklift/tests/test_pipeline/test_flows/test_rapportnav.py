import pandas as pd
import pandas.api.types as ptypes

from forklift.pipeline.flows.extract_rapportnav_analytics import (
    _process_data,
    extract_missions_ids,
    flow,
)
from tests.mocks import replace_check_flow_not_running

replace_check_flow_not_running(flow)


def post_rapportnav_mock_factory():
    return {}


def test__process_data_patrol():
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
        "completenessForStats.status": ["COMPLETE"],
        "isMissionFinished": [True],
    }

    df = pd.DataFrame(data)

    out = _process_data(df, "patrol")

    # Removed temporary fields should not be present (after replacement dots->underscores)
    assert "operationalSummary_foo" not in out.columns

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


def test__process_data_aem():
    data = [
        {
            "id": 1,
            "idUUID": "1211",
            "serviceId": 21,
            "controlUnits": [{"id": 1}],
            "missionTypes": "LAND",
            "startDateTimeUtc": "2025-01-06T07:00:00Z",
            "endDateTimeUtc": "2025-01-17T17:00:00Z",
            "facade": [None],
            "data": [{"id": "1.1.1", "title": "Nombre d'heures de mer", "value": 1211}],
            "completenessForStats.status": "COMPLETE",
            "isMissionFinished": True,
        }
    ]

    df = pd.DataFrame(data)

    out = _process_data(df, "aem")

    # Original 'data' column must be dropped
    assert "data" not in out.columns

    # New columns should be present with names id_title (underscore separator)
    assert "1_1_1_nombre_d_heures_de_mer" in out.columns

    # Values should be extracted and unwrapped when nested under {'value': ...}
    assert out.loc[0, "1_1_1_nombre_d_heures_de_mer"] == 1211

    # Year and month should be extracted from date
    assert out.loc[0, "annee"] == 2025
    assert out.loc[0, "mois"] == 1


def test__process_data_with_complete_and_finished_attributes():
    """Test that complete and finished attributes from RapportNav API are preserved."""
    data = {
        "id": [1, 2],
        "idUUID": ["uuid-1", "uuid-2"],
        "serviceId": [21, 22],
        "controlUnits": [[{"id": 1}], [{"id": 2}]],
        "missionTypes": ["LAND", "SEA"],
        "startDateTimeUtc": ["2025-01-06T07:00:00Z", "2025-01-07T08:00:00Z"],
        "endDateTimeUtc": ["2025-01-17T17:00:00Z", "2025-01-18T18:00:00Z"],
        "facade": ["SA", "NAMO"],
        "completenessForStats.status": ["COMPLETE", "INCOMPLETE"],
        "isMissionFinished": [True, False],
    }

    df = pd.DataFrame(data)

    out = _process_data(df, "patrol")

    # Only mission which are finished and comlpete should be extracted
    assert len(out) == 1


def test_process_control_unit_ids():
    from forklift.pipeline.flows.extract_rapportnav_analytics import (
        _process_control_unit_ids,
        _process_mission_interservices,
    )

    # None or empty -> empty list
    assert _process_control_unit_ids(None) == []
    assert _process_mission_interservices(None) == False
    assert _process_control_unit_ids([]) == []
    assert _process_mission_interservices([]) == False

    # Normal list of dicts with single control unit
    assert _process_control_unit_ids([{"id": 101, "name": "A"}]) == [101]
    assert _process_mission_interservices([{"id": 101, "name": "A"}]) == False

    # Normal list of dicts with multiple control units
    assert _process_control_unit_ids(
        [{"id": 101, "name": "A"}, {"id": 202, "name": "B"}]
    ) == [101, 202]
    assert (
        _process_mission_interservices(
            [{"id": 101, "name": "A"}, {"id": 202, "name": "B"}]
        )
        == True
    )

    # Mixed contents -> only dicts with 'id' are returned
    assert _process_control_unit_ids([{"id": 1}, "str", {"no_id": 3}, {"id": 4}]) == [
        1,
        4,
    ]
    assert (
        _process_mission_interservices([{"id": 1}, "str", {"no_id": 3}, {"id": 4}])
        == True
    )

    # Non-iterable input should be handled and return empty list
    assert _process_control_unit_ids(123) == []
    assert _process_mission_interservices(123) == False


def test_extract_missions_ids():
    """
    Reads test data from monitorenv (table missions)
    """
    mission_ids = extract_missions_ids.run()
    assert len(mission_ids) > 0
    assert isinstance(mission_ids, list)


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
