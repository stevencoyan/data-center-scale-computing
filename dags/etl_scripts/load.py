import pandas as pd
import argparse
from sqlalchemy import create_engine
import os
from sqlalchemy.dialects.postgresql import insert
import sqlalchemy

def load_data(table_file, table_name, key):
    db_url = os.environ.get('DB_URL')
    conn = create_engine(db_url)

    def insert_on_conflict_nothing(table, conn, keys, data_iter):
        data = [dict(zip(keys, row)) for row in data_iter]
        stmt = insert(table.table).values(data).on_conflict_do_nothing(index_elements = [key])
        result = conn.execute(stmt)
        return result.rowcount

    pd.read_parquet(table_file).to_sql(table_name, conn, if_exists='replace', index=False)
    print(table_name + " loaded")

def load_fact_data(table_file, table_name):
    db_url = os.environ.get('DB_URL')
    conn = create_engine(db_url)

    pd.read_parquet(table_file).to_sql(table_name, conn, if_exists='replace', index=False)
    print(table_name + " loaded")


    