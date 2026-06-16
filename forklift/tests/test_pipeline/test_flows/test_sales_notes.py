from datetime import datetime

import numpy as np
import pandas as pd
import pytest
from clickhouse_connect.driver.exceptions import DatabaseError
from pytest import fixture

from forklift.db_engines import create_datawarehouse_client
from forklift.pipeline.flows.sales_notes import extract_sales_notes, flow
from tests.mocks import get_utcnow_mock_factory, replace_check_flow_not_running

replace_check_flow_not_running(flow)


@fixture
def drop_sales_notes():
    client = create_datawarehouse_client()
    yield
    client.command("DROP TABLE IF EXISTS monitorfish.sales_notes")


@fixture
def expected_sales_notes_apr_2026() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "operation_number": ["FRA20260410509863", "NND20260427157919"],
            "operation_country": ["FRA", "NND"],
            "operation_datetime_utc": [
                pd.Timestamp("2026-04-10 08:47:00"),
                pd.Timestamp("2026-04-27 09:45:00"),
            ],
            "report_id": ["FRA20260410510469", "NND20260427987885"],
            "report_datetime_utc": [
                pd.Timestamp("2026-04-10 07:05:00"),
                pd.Timestamp("2026-04-27 09:45:00"),
            ],
            "cfr": ["RYX346578713", "FRA000999999"],
            "ircs": ["HC5098", "AAAAAA"],
            "external_identification": ["3-SH-01-03", "XR AAAA"],
            "vessel_name": ["CBKCQHIV PGREXSSPH ZPZN", "VESSEL_SALE"],
            "flag_state": ["ESP", "FRA"],
            "imo": [None, None],
            "sales_type": ["SN", "TOD"],
            "sender_id": [None, None],
            "sender_name": [None, None],
            "provider_id": [None, None],
            "provider_name": ["HAM DE LA ROCHELLE", None],
            "buyer_id": ["PC82278248963", None],
            "buyer_name": ["RYQATKPADLZJ HBHSCCX", None],
            "recipient_id": [None, None],
            "recipient_name": [None, None],
            "carrier_id": [None, None],
            "carrier_name": [None, None],
            "sales_datetime_utc": [
                pd.Timestamp("2026-03-23 00:00:00"),
                pd.Timestamp("2026-04-25 00:00:00"),
            ],
            "sales_country": ["FRA", "FRA"],
            "sales_port_code": ["FRLRH", "FRQUY"],
            "sales_contract_reference": [None, None],
            "bcd_number": [None, None],
            "takeover_organization_name": [None, "TAKE-OVER ORGANANIZATION"],
            "storage_facility_name": [None, "STORAGE FACILITY"],
            "storage_facility_address": [None, "STORAGE ADDRESS"],
            "transport_document_reference": [None, None],
            "invoice_datetime_utc": [pd.Timestamp("2026-03-23 00:00:00"), pd.NaT],
            "invoice_number": ["320245", None],
            "takeover_contract_reference": [None, "20260425FRA000000000"],
            "trip_number": [None, None],
            "sales_id": [None, None],
            "landing_port_code": ["ESGAR", "FRQUY"],
            "departure_datetime_utc": [None, None],
            "landing_datetime_utc": [
                pd.Timestamp("2026-03-18 00:00:00"),
                pd.Timestamp("2026-04-25 00:00:00"),
            ],
            "transmission_format": ["ERS", "ERS"],
            "product_usage": [None, None],
            "product_weight": [8.8, 545.0],
            "product_fao_zone": ["37.1.1", "27.7"],
            "product_species": ["LTA", "SCE"],
            "product_currency": ["EUR", None],
            "product_freshness": ["A", "SO"],
            "product_size_class": [None, None],
            "product_total_price": [0.0, np.nan],
            "product_presentation": ["WHL", "WHL"],
            "product_size_category": [None, None],
            "product_preservation_state": ["FRE", "ALI"],
            "product_unit_price": [4.3, np.nan],
            "product_withdrawn": ["N", None],
            "product_destination": ["HCN", None],
            "product_fish_size": [None, None],
            "product_producer_organization": [None, None],
            "product_nb_fish": [None, None],
            "product_economic_zone": [None, None],
            "product_statistical_rectangle": [None, None],
            "product_effort_zone": [None, None],
            "product_packaging": [None, None],
            "product_conversion_factor": [None, None],
        }
    )


def test_extract_sales_notes(expected_sales_notes_apr_2026):
    result = extract_sales_notes.run(month_start=datetime(2026, 4, 1))
    pd.testing.assert_frame_equal(
        result.sort_values("operation_number")
        .reset_index(drop=True)
        .drop(columns=["integration_datetime_utc"]),
        expected_sales_notes_apr_2026,
        check_dtype=False,
    )

    assert len(extract_sales_notes.run(month_start=datetime(2017, 5, 1))) == 12
    assert len(extract_sales_notes.run(month_start=datetime(2022, 1, 1))) == 32

    empty = extract_sales_notes.run(month_start=datetime(2024, 11, 1))
    pd.testing.assert_frame_equal(
        empty.drop(columns=["integration_datetime_utc"]),
        expected_sales_notes_apr_2026.head(0),
        check_dtype=False,
    )


def test_sales_notes(drop_sales_notes):
    client = create_datawarehouse_client()

    flow.replace(
        flow.get_tasks("get_utcnow")[0], get_utcnow_mock_factory(datetime(2026, 6, 8))
    )

    query = "SELECT * FROM monitorfish.sales_notes ORDER BY operation_number, product_species"

    with pytest.raises(DatabaseError):
        client.query_df(query)

    state = flow.run(start_months_ago=2, end_months_ago=0)
    assert state.is_successful()
    sales_notes_after_one_run = client.query_df(query)

    state = flow.run(start_months_ago=2, end_months_ago=0)
    assert state.is_successful()
    sales_notes_after_two_runs = client.query_df(query)

    assert len(sales_notes_after_one_run) == len(sales_notes_after_two_runs) == 2
    assert set(sales_notes_after_one_run.operation_number) == {
        "FRA20260410509863",
        "NND20260427157919",
    }
