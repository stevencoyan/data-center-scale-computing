import pandas as pd
import argparse
from sqlalchemy import create_engine
import os
from sqlalchemy.dialects.postgresql import insert
import sqlalchemy
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def upload_to_s3(file_path, bucket_name, object_name):
    s3_hook = S3Hook(aws_conn_id='conn_s3')

    if os.path.isfile(file_path):
        # Upload a single file
        s3_hook.load_file(
            filename=file_path,
            bucket_name=bucket_name,
            key=object_name,
            replace=True  # Set to True if you want to overwrite existing files
        )
    elif os.path.isdir(file_path):
        # Upload all files in a directory
        for local_file in os.listdir(file_path):
            local_file_path = os.path.join(file_path, local_file)
            s3_key = f"{object_name}/{local_file}"

            s3_hook.load_file(
                filename=local_file_path,
                bucket_name=bucket_name,
                key=s3_key,
                replace=True  # Set to True if you want to overwrite existing files
            )
    else:
        raise ValueError(f"Invalid file path: {file_path}")


def load_data(table_file, table_name, key):
    db_url = os.environ.get('DB_URL')
    conn = create_engine(db_url)

    def insert_on_conflict_nothing(table, conn, keys, data_iter): # function not working properly
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


    