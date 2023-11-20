from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
from etl_scripts.pipeline import transform_data
from etl_scripts.load import load_data
from etl_scripts.load import load_fact_data
from etl_scripts.load import upload_to_s3

# https://data.austintexas.gov/api/views/9t4d-g238/rows.csv?date=20231118&accessType=DOWNLOAD

SOURCE_URL = 'https://data.austintexas.gov/api/views/9t4d-g238/rows.csv'
AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME', '/opt/airflow')
CSV_TARGET_DIR = AIRFLOW_HOME + '/data/{{ ds }}/downloads'
CSV_TARGET_FILE = CSV_TARGET_DIR + '/outcomes_{{ ds }}.csv'
PQ_TARGET_DIR = AIRFLOW_HOME + '/data/{{ ds }}/processed'
PQ_TARGET_FILE = PQ_TARGET_DIR + '/outcomes_{{ ds }}.parquet'

S3_BUCKET_NAME = "lab3coyansteven"
S3_KEY = "outcomes_{{ ds }}.csv"

with DAG(
    dag_id="outcomes_dag",
    start_date = datetime(2023,11,19),
    schedule_interval="@daily"
) as dag:
    
    extract = BashOperator(
        task_id="extract",
        bash_command=f"curl --create-dirs -o {CSV_TARGET_FILE} {SOURCE_URL}",

    )

    raw_data_to_s3 = PythonOperator(
        task_id="raw_data_to_s3",
        python_callable=upload_to_s3,
        op_kwargs={
            "file_path": CSV_TARGET_FILE,
            "bucket_name": S3_BUCKET_NAME,
            "object_name": "raw_data/outcomes_{{ ds }}.csv"  # Adjust the S3 object key as needed
        },
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable=transform_data,
        op_kwargs={
            "source_csv": CSV_TARGET_FILE,
            "target_dir": PQ_TARGET_DIR
        }
    )

    transform_data_to_s3 = PythonOperator(
        task_id="transformed_data_to_s3",
        python_callable=upload_to_s3,
        op_kwargs={
            "file_path": CSV_TARGET_FILE,
            "bucket_name": S3_BUCKET_NAME,
            "object_name": S3_KEY
        },
    )

    load_animals_dim = PythonOperator(

        task_id="load_animals_dim",
        python_callable=load_data,
        op_kwargs={
            "table_file": PQ_TARGET_DIR + '/dim_animals.parquet',
            "table_name": "dim_animals",
            "key": "Animal_Key"
        }
    )

    load_dates_dim = PythonOperator(

        task_id="load_date_dim",
        python_callable=load_data,
        op_kwargs={
            "table_file": PQ_TARGET_DIR + '/dim_date.parquet',
            "table_name": "dim_date",
            "key": "Date_ID"
        }
    )

    load_outcomes_dim = PythonOperator(

        task_id="load_outcomes_dim",
        python_callable=load_data,
        op_kwargs={
            "table_file": PQ_TARGET_DIR + '/dim_outcomes.parquet',
            "table_name": "dim_outcomes",
            "key": "Outcome_Type_ID"
        }
    )

    load_fact_table = PythonOperator(

        task_id="load_fact_table",
        python_callable=load_fact_data,
        op_kwargs={
            "table_file": PQ_TARGET_DIR + '/fact_table.parquet',
            "table_name": "fact_table",
        }
    )

    data_tables_to_s3 = PythonOperator(
        task_id="data_tables_to_s3",
        python_callable=upload_to_s3,
        op_kwargs={
            "file_path": PQ_TARGET_DIR,
            "bucket_name": S3_BUCKET_NAME,
            "object_name": S3_KEY
        },
    )

    extract >> raw_data_to_s3 >> transform >> transform_data_to_s3 >> [load_animals_dim, load_dates_dim, load_outcomes_dim] >> load_fact_table >> data_tables_to_s3

