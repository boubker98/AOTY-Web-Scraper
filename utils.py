import undetected_chromedriver as uc
import duckdb

import logging


def init_driver(headless=True):
    return uc.Chrome(headless=headless)


def get_connection(database_name):
    return duckdb.connect(database=database_name, read_only=False)


def save_to_table(con, table_name, dataframe):
    con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM {dataframe}")
