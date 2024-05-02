import shutil
import great_expectations as ge
import os
import random
import pandas as pd
import sqlalchemy
import datetime
import requests
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import DateTime
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow import DAG
from great_expectations.dataset.pandas_dataset import PandasDataset
from great_expectations.data_context.data_context import DataContext
from airflow.operators.python import BranchPythonOperator
from airflow.models import Variable
from airflow.hooks.base_hook import BaseHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Connection
from airflow.settings import Session



POSTGRES_CONN_ID ="postgres_default"
RAW_DATA_FOLDER = 'data/raw-data'
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
EXPECTATIONS_FOLDER = os.path.join(CURRENT_DIR, "..", "..", "..", "gx")
GOOD_DATA_FOLDER = os.path.join('..', 'data', 'good-data')
BAD_DATA_FOLDER = os.path.join('..', 'data', 'bad-data')

Variable.set("GOOD_DATA_FOLDER", GOOD_DATA_FOLDER)
Variable.set("BAD_DATA_FOLDER", BAD_DATA_FOLDER)

def read_data(**kwargs):
    files = os.listdir(RAW_DATA_FOLDER)
    if files:
        selected_file = random.choice(files)
        file_path = os.path.join(RAW_DATA_FOLDER, selected_file)
        print("Selected file:", selected_file)  # Add this line for logging
        print("File path in read_data:", file_path)  # Add this line for logging
        kwargs['ti'].xcom_push(key='file_path', value=file_path)

def validate_data(**kwargs):
    file_path = kwargs['ti'].xcom_pull(key='file_path', task_ids='read-data')
    print("File path in validate_data:", file_path)  # Add this line for logging
    if file_path:
        df = pd.read_csv(file_path)
        context = DataContext(context_root_dir=EXPECTATIONS_FOLDER)
        expectation_suite = context.get_expectation_suite('suite')
        ge_df = PandasDataset(df, expectation_suite=expectation_suite)
        validation_results = ge_df.validate(expectation_suite=expectation_suite)
        kwargs['ti'].xcom_push(key='validation_results', value=validation_results)
        good_data_rows = []
        bad_data_rows = []
        for idx, result in enumerate(validation_results['results']):
            if result['success']:
                good_data_rows.append(idx)
            else:
                bad_data_rows.append(idx)
        
        if not bad_data_rows:
            print(file_path, 'is good data')
            kwargs['ti'].xcom_push(key='data_quality', value='good_data')
            good_data_path = os.path.join(Variable.get("GOOD_DATA_FOLDER"), os.path.basename(file_path))
            df.to_csv(good_data_path, index=False)
            print("Entire data saved to good data path:", good_data_path)
        elif not good_data_rows:
            print(file_path, 'is bad data')
            kwargs['ti'].xcom_push(key='data_quality', value='bad_data')
            bad_data_path = os.path.join(Variable.get("BAD_DATA_FOLDER"), os.path.basename(file_path))
            df.to_csv(bad_data_path, index=False)
            print("Entire data saved to bad data path:", bad_data_path)
        else:
            print("Some rows have data problems. Splitting file into good and bad data.")
            good_df = df.iloc[good_data_rows]
            bad_df = df.iloc[bad_data_rows]
            good_data_path = os.path.join(Variable.get("GOOD_DATA_FOLDER"), os.path.basename(file_path))
            bad_data_path = os.path.join(Variable.get("BAD_DATA_FOLDER"), os.path.basename(file_path))
            os.makedirs(os.path.dirname(good_data_path), exist_ok=True)  # Ensure directory exists
            os.makedirs(os.path.dirname(bad_data_path), exist_ok=True)  # Ensure directory exists
            good_df.to_csv(good_data_path, index=False)
            bad_df.to_csv(bad_data_path, index=False)
            print("Good data saved to:", good_data_path)
            print("Bad data saved to:", bad_data_path)
    else:
        print("File path is None. Unable to validate data.")


def decide_which_pathtogo(**kwargs):
    ti = kwargs['ti']
    data_quality = ti.xcom_pull(task_ids='validate-data', key='data_quality')
    if data_quality == 'good_data':
        return 'split-and-save-data'
    else:
        return ['send-alerts', 'save-data-errors', 'split-and-save-data']

def send_alerts(**kwargs):
    validation_results = kwargs['ti'].xcom_pull(key='validation_results', task_ids='validate-data')
    file_path = kwargs['ti'].xcom_pull(key='file_path', task_ids='read-data')
    total_rows = len(validation_results["results"])
    failed_rows = len([result for result in validation_results["results"] if not result["success"]])
    context = DataContext(context_root_dir=EXPECTATIONS_FOLDER)
    context.build_data_docs()
    data_docs_url = context.get_docs_sites_urls()[0]['site_url']
    print(f"Data Docs report available at: {data_docs_url}")

    teams_webhook_url = "https://epitafr.webhook.office.com/webhookb2/2fa20091-4701-4b5d-9ede-c4ccaed1992b@3534b3d7-316c-4bc9-9ede-605c860f49d2/IncomingWebhook/f96d7e6d99ed4719a0197701d450c1a9/c9156a2d-2a28-42a7-88a0-c157f2a03744"
    headers = {'Content-Type': 'application/json'}

    data_docs_url = "http://192.168.1.37:8000/local_site/index.html"

    if failed_rows > 5:
        criticality = 'high'
    elif failed_rows > 3:
        criticality = 'medium'
    else:
        criticality = 'low'

    alert_message = {
        "text": f"Data Problem Alert:\nCriticality: {criticality}\nSummary: {failed_rows} rows failed validation.\nCheck the detailed report here: {data_docs_url}"
    }

    try:
        response = requests.post(teams_webhook_url, headers=headers, json=alert_message)
        if response.status_code == 200:
            print("Alert sent to Teams successfully.")
        else:
            print(f"Failed to send alert to Teams. Status code: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"Error sending alert to Teams: {e}")

def save_data_errors(**kwargs):
    session = Session()
    db_conn = (
        session.query(Connection)
        .filter(Connection.conn_id == "postgres_default")
        .first()
    )
    print(db_conn.conn_type)
    if db_conn and db_conn.conn_type == "postgres":
        try:
            engine = create_engine("postgresql://postgres:password@127.0.0.1:5432/FlightPrediction")
            validation_results = kwargs['ti'].xcom_pull(key='validation_results', task_ids='validate-data')
            file_path = kwargs['ti'].xcom_pull(key='file_path', task_ids='read-data')
            total_rows = len(validation_results["results"])
            failed_rows = len([result for result in validation_results["results"] if not result["success"]])

            with engine.connect() as connection:
                print("connected to the postgres")
                metadata = MetaData(bind=engine)
                data_quality_table = Table(
                    'data_quality_report',
                    metadata,
                    Column('id', Integer, primary_key=True, autoincrement=True),
                    Column('total_rows', Integer),
                    Column('failed_rows', Integer),
                    Column('timestamp', DateTime),
                    Column('file_path', String(500))
                )
                metadata.create_all(engine)
                insert_query = data_quality_table.insert().values(
                    total_rows=total_rows,
                    failed_rows=failed_rows,
                    timestamp=datetime.datetime.now(),
                    file_path=file_path,
                )
                connection.execute(insert_query)
                print("Data quality report saved to the database.")

        except SQLAlchemyError as e:
            print(f"Error saving data quality report to the database: {e}")

def split_and_save_data(**kwargs):
    ti = kwargs['ti']
    file_path = ti.xcom_pull(key='file_path', task_ids='read-data')
    data_quality = ti.xcom_pull(key='data_quality', task_ids='validate-data')
    
    print("Received file path:", file_path)
    print("Received data quality:", data_quality)

    if file_path is None:
        raise ValueError("Received NoneType object as file path.")
    
    output_folder = BAD_DATA_FOLDER if data_quality == 'bad_data' else GOOD_DATA_FOLDER
    os.makedirs(output_folder, exist_ok=True)
    dataframe = pd.read_csv(file_path)
    output_path = os.path.join(output_folder, os.path.basename(file_path))
    dataframe.to_csv(output_path, index=False)  


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'email_on_failure': False,
    'email_on_retry': False,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'data_ingestion',
    default_args=default_args,
    description='DAG to ingest data from raw-data to good-data or bad-data',
    schedule_interval='@daily',
)

read_data_task = PythonOperator(
    task_id='read-data',
    python_callable=read_data,
    dag=dag,
)

validate_data_task = PythonOperator(
    task_id='validate-data',
    python_callable=validate_data,
    dag=dag,
)

branch_task = BranchPythonOperator(
    task_id='branch_based_on_validation',
    python_callable=decide_which_pathtogo,
    provide_context=True,
    dag=dag,
)

send_alerts_task = PythonOperator(
    task_id='send-alerts',
    python_callable=send_alerts,
    provide_context=True,
    dag=dag,
    trigger_rule='all_success',
)

save_file_task = PythonOperator(
    task_id='split-and-save-data',
    python_callable=split_and_save_data,
    provide_context=True,
    dag=dag,
)

save_data_errors_task = PythonOperator(
    task_id='save-data-errors',
    python_callable=save_data_errors,
    provide_context=True,
    dag=dag,
    trigger_rule='all_success',
)

read_data_task >> validate_data_task >>  branch_task
branch_task >> save_file_task
branch_task >> send_alerts_task
branch_task >> save_data_errors_task