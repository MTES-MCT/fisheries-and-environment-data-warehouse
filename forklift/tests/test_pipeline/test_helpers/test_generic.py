from datetime import datetime
from logging import Logger

import pandas as pd
from pytest import fixture

from forklift.db_engines import create_datawarehouse_client
from forklift.pipeline.helpers.generic import load_to_data_warehouse


@fixture
def init_test_db():
    print("Test init")
    client = create_datawarehouse_client()
    client.command("CREATE DATABASE test_db")
    client.command(
        """
        CREATE TABLE test_db.test_table (
            int_id Integer,
            string_field String,
            float_field Float,
            datetime_field DateTime,
            boolean_field boolean,
            nullable_int_field Nullable(Integer),
            nullable_string_field Nullable(String),
            nullable_float_field Nullable(Float),
            nullable_datetime_field Nullable(DateTime),
            nullable_boolean_field Nullable(boolean)
        )
        ENGINE MergeTree()
        ORDER BY int_id;
    """
    )
    yield
    print("Test clean-up")
    client.command("DROP DATABASE IF EXISTS test_db")


@fixture
def df_to_load() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "int_id": [1, 2, 3],
            "string_field": ["a", "b", "c"],
            "float_field": [1.1, 2.2, 3.3],
            "datetime_field": [
                datetime(520, 1, 2, 5, 6),
                datetime(2020, 1, 3, 4, 45),
                datetime(2020, 1, 4, 20, 12, 7),
            ],
            "boolean_field": [True, True, False],
            "nullable_int_field": [4, None, 6],
            "nullable_string_field": ["d", None, "f"],
            "nullable_float_field": [4.4, None, 5.5],
            "nullable_datetime_field": [
                datetime(2020, 1, 5, 15, 36, 41),
                None,
                datetime(3020, 1, 7, 10, 2, 27),
            ],
            "nullable_boolean_field": [False, None, True],
        }
    )


@fixture
def expected_loaded_df(df_to_load):
    df = df_to_load.copy(deep=True)
    df.loc[0, "datetime_field"] = datetime(1970, 1, 1, 0, 0, 0)
    df.loc[2, "nullable_datetime_field"] = datetime(2106, 2, 7, 6, 28, 15)
    return df


def test_load_to_data_warehouse(init_test_db, df_to_load, expected_loaded_df):
    client = create_datawarehouse_client()
    logger = Logger("logger")

    load_to_data_warehouse(
        df=df_to_load,
        table_name="test_table",
        database="test_db",
        logger=logger,
        datetime_cols_to_clip=["datetime_field", "nullable_datetime_field"],
    )
    loaded_df = client.query_df("SELECT * FROM test_db.test_table")
    pd.testing.assert_frame_equal(loaded_df, expected_loaded_df, check_dtype=False)
