from pathlib import Path

import pytest
from pytest import fixture

from forklift.db_engines import create_datawarehouse_client
from forklift.pipeline.entities.sacrois import SacroisFileImportSpec, SacroisFileType
from forklift.pipeline.flows.import_sacrois_data import (
    flow,
    get_file_type,
    get_sacrois_file_import_spec,
)
from tests.mocks import replace_check_flow_not_running

replace_check_flow_not_running(flow)


@fixture
def drop_sacrois():
    client = create_datawarehouse_client()
    print("Drop sacrois init")
    yield client
    print("Drop sacrois cleaning")
    client.command("DROP DATABASE IF EXISTS sacrois")


def test_get_file_type_raises_if_input_is_unexpected():
    with pytest.raises(
        ValueError, match="'UNEXPECTED_FILE_TYPE' is not a valid SacroisFileType"
    ):
        get_file_type.run("UNEXPECTED_FILE_TYPE")


def test_get_file_type_returns_sacrois_file_type():
    assert (
        get_file_type.run("NAVIRES_MOIS_MAREES_JOUR")
        == SacroisFileType.NAVIRES_MOIS_MAREES_JOUR
    )


def test_get_sacrois_file_import_spec_returns_spec():
    spec = get_sacrois_file_import_spec.run(
        file_type=SacroisFileType.NAVIRES_MOIS_MAREES_JOUR,
        year=2015,
        month=1,
    )

    assert spec == SacroisFileImportSpec(
        filetype=SacroisFileType.NAVIRES_MOIS_MAREES_JOUR,
        year=2015,
        month=1,
        partition="201501",
        filepath=Path("sacrois/201501/NAVIRES_MOIS_MAREES_JOUR_201501.parquet"),
    )


def test_get_sacrois_file_import_spec_raises_if_year_is_incorrect():
    with pytest.raises(ValueError, match="year 201506 is out of range"):
        get_sacrois_file_import_spec.run(
            file_type=SacroisFileType.NAVIRES_MOIS_MAREES_JOUR,
            year=201506,
            month=1,
        )


def test_get_sacrois_file_import_spec_raises_if_month_is_incorrect():
    with pytest.raises(ValueError, match="month must be in 1..12"):
        get_sacrois_file_import_spec.run(
            file_type=SacroisFileType.NAVIRES_MOIS_MAREES_JOUR,
            year=2015,
            month=18,
        )


@pytest.mark.parametrize(
    "sacrois_file_type_flow_parameter,table_name,lines_run_1,lines_run_2",
    [
        ("NAVIRES_MOIS_MAREES_JOUR", "navires_mois_marees_jour", 10, 15),
        ("FISHING_ACTIVITY", "fishing_activity", 14, 14),
    ],
)
def test_sacrois_navires_mois_marees_jours(
    drop_sacrois, sacrois_file_type_flow_parameter, table_name, lines_run_1, lines_run_2
):
    client = drop_sacrois

    initial_databases = set(client.query_df("SHOW DATABASES").name)
    assert "sacrois" not in initial_databases

    query = f"SELECT * FROM sacrois.{table_name}"

    # 1st run - data should be inserted.
    state = flow.run(
        year=2024,
        month=1,
        sacrois_file_type=sacrois_file_type_flow_parameter,
    )
    assert state.is_successful()

    final_databases = set(client.query_df("SHOW DATABASES").name)
    assert "sacrois" in final_databases

    data_after_run_1 = client.query_df(query)

    # Repeat 1st run - the result should be unchanged as the partition is overwritten.
    state = flow.run(
        year=2024,
        month=1,
        sacrois_file_type=sacrois_file_type_flow_parameter,
    )
    assert state.is_successful()

    data_after_run_1_bis = client.query_df(query)

    # Add a different run - the data should be appended to the data of the first run.
    state = flow.run(
        year=2025,
        month=2,
        sacrois_file_type=sacrois_file_type_flow_parameter,
    )
    assert state.is_successful()

    data_after_run_2 = client.query_df(query)

    assert len(data_after_run_1) == len(data_after_run_1_bis) == lines_run_1
    assert len(data_after_run_2) == lines_run_1 + lines_run_2
