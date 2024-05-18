import os
import random
import pandas as pd
import requests
import datetime
import time
import json
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, JSON
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import DateTime
from airflow.operators.python_operator import PythonOperator
from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.models import Variable
from airflow.hooks.base_hook import BaseHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Connection
from airflow.settings import Session
import great_expectations as ge
from great_expectations.dataset.pandas_dataset import PandasDataset
from great_expectations.data_context.data_context import DataContext
from great_expectations.core.batch import BatchRequest



POSTGRES_CONN_ID ="postgres_default"
RAW_DATA_FOLDER = 'data/raw-data'
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
EXPECTATIONS_FOLDER = os.path.join(CURRENT_DIR, "..", "..", "..", "gx")
GOOD_DATA_FOLDER = os.path.join('..', 'data', 'good-data')
BAD_DATA_FOLDER = os.path.join('..', 'data', 'bad-data')

# Set Airflow variables for good and bad data folders
Variable.set("GOOD_DATA_FOLDER", GOOD_DATA_FOLDER)
Variable.set("BAD_DATA_FOLDER", BAD_DATA_FOLDER)

def read_data(**kwargs):
    files = os.listdir(RAW_DATA_FOLDER)
    if files:
        selected_file = random.choice(files)
        file_path = os.path.join(RAW_DATA_FOLDER, selected_file)
        print("Selected file:", selected_file)  
        print("File path in read_data:", file_path)  
        kwargs['ti'].xcom_push(key='file_path', value=file_path)

def validate_data(**kwargs):
    file_path = kwargs['ti'].xcom_pull(key='file_path', task_ids='read-data')
    print("File path in validate_data:", file_path)  
    asset_name = os.path.basename(file_path).split(".")[0]
    print("asset_name is ", asset_name)
    if file_path:
        df = pd.read_csv(file_path)
        context = DataContext(context_root_dir=EXPECTATIONS_FOLDER)
        suite_name = 'suite'
        expectation_suite = context.get_expectation_suite(suite_name)
        ge_df = PandasDataset(df, expectation_suite=expectation_suite)
        validation_results = ge_df.validate(expectation_suite=expectation_suite)
        run_id = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        kwargs["ti"].xcom_push(key="run_id", value=run_id)
        kwargs['ti'].xcom_push(key='validation_results', value=validation_results)
        
        if validation_results["success"]:
            print(file_path, "is good data")
            kwargs["ti"].xcom_push(key="data_quality", value="good_data")
        else:
            print(file_path, "is bad data")
            kwargs["ti"].xcom_push(key="data_quality", value="bad_data")

        context.build_data_docs()
        data_docs_site = context.get_docs_sites_urls()[0]["site_url"]
        print(f"Data Docs generated at: {data_docs_site}")

    pass

def decide_which_pathtogo(**kwargs):
    ti = kwargs['ti']
    data_quality = ti.xcom_pull(task_ids='validate-data', key='data_quality')
    if data_quality == 'good_data':
        return 'split-and-save-data'
    else:
        return ['send-alerts', 'save-data-errors', 'split-and-save-data']

def send_alerts(**kwargs):
    validation_results = kwargs['ti'].xcom_pull(key='validation_results', task_ids='validate-data')
    run_id = kwargs["ti"].xcom_pull(key="run_id", task_ids="validate-data")
    file_path = kwargs['ti'].xcom_pull(key='file_path', task_ids='read-data')
    df = pd.read_csv(file_path)
    
    failed_rows = [result for result in validation_results["results"] if not result["success"]]
    total_failed_rows = len(failed_rows)
    
    error_summary = os.linesep.join(
        [
            f"- Expectation {index + 1}: {expectation['expectation_config']['expectation_type']} failed with {expectation['result'].get('unexpected_count', 'N/A')} unexpected values."
            for index, expectation in enumerate(failed_rows)
        ]
    )
    # Rebuild the data docs and get the URL
    context = DataContext(context_root_dir=EXPECTATIONS_FOLDER)
    context.build_data_docs()
    data_docs_url = context.get_docs_sites_urls()[0]['site_url']
    print(f"Data Docs report available at: {data_docs_url}")

    teams_webhook_url = "https://epitafr.webhook.office.com/webhookb2/2fa20091-4701-4b5d-9ede-c4ccaed1992b@3534b3d7-316c-4bc9-9ede-605c860f49d2/IncomingWebhook/f96d7e6d99ed4719a0197701d450c1a9/c9156a2d-2a28-42a7-88a0-c157f2a03744"
    headers = {'Content-Type': 'application/json'}
    data_docs_url = "http://192.168.1.37:8000/local_site/index.html"

    if total_failed_rows > 5:
        criticality = 'high'
    elif total_failed_rows > 3:
        criticality = 'medium'
    else:
        criticality = 'low'

    alert_message = {
        "text": f"Data Problem Alert:\nCriticality: {criticality}\nSummary: {total_failed_rows} rows failed validation.\nCheck the detailed report here: {data_docs_url}"
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
    file_path = kwargs['ti'].xcom_pull(key='file_path', task_ids='read-data')
    validation_results = kwargs['ti'].xcom_pull(key='validation_results', task_ids='validate-data')
    total_rows = len(validation_results["results"])
    criticality = kwargs["ti"].xcom_pull(key="criticality", task_ids="send-alerts")
    bad_rows_indices_length = kwargs["ti"].xcom_pull(key="bad_rows_indices_len", task_ids="send-alerts")
    
    error_details = {
        result["expectation_config"]["expectation_type"]: result["result"].get("unexpected_count", "N/A")
        for result in validation_results["results"]
        if not result["success"]
    }
    # Connect to PostgreSQL database
    db_conn_string = "postgresql://postgres:password@127.0.0.1:5432/FlightPrediction"
    engine = create_engine(db_conn_string)
    metadata = MetaData()
    data_quality_table = Table(
        'data_quality_report',
        metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('total_rows', Integer),
        Column('failed_rows', Integer),
        Column('timestamp', DateTime),
        Column('file_path', String(500)),
        Column("criticality", String(500)),
        Column("error_details", JSON),
    )
    metadata.create_all(engine)
    
    try:
        with engine.connect() as connection:
            print("connected to the postgres")            
            insert_query = data_quality_table.insert().values(
                total_rows=total_rows,
                failed_rows=bad_rows_indices_length,
                timestamp=datetime.datetime.now(),
                file_path=file_path,
                criticality=criticality,
                error_details=json.dumps(error_details),
            )
            connection.execute(insert_query)
            print("Data quality report saved to the database.")
    except SQLAlchemyError as e:
        print(f"Error saving data quality report to the database: {e}")

def split_and_save_data(**kwargs):
    ti = kwargs['ti']
    file_path = ti.xcom_pull(key='file_path', task_ids='read-data')
    validation_results = kwargs["ti"].xcom_pull(
        key="validation_results", task_ids="validate-data"
    )
    dataframe = pd.read_csv(file_path)
    bad_rows_indices = set()
    data_quality = ti.xcom_pull(key='data_quality', task_ids='validate-data')
    for result in validation_results["results"]:
        if not result["success"]:
            if (
                "partial_unexpected_list" in result["result"]
                and result["result"]["partial_unexpected_list"]
            ):
                '''Assume partial_unexpected_list contains the row indices
                 (this might need adjustment
                 based on actual data structure) '''
                for value in result["result"]["partial_unexpected_list"]:
                    '''Find all occurrences of this 
                    unexpected value in the specified column'''
                    index_list = dataframe.index[
                        dataframe[result["expectation_config"]["kwargs"]
                           ["column"]] == value
                    ].tolist()
                    bad_rows_indices.update(index_list)

    df_bad = dataframe.iloc[list(bad_rows_indices)]
    df_good = dataframe.drop(index=list(bad_rows_indices))
    os.makedirs(GOOD_DATA_FOLDER, exist_ok=True)
    os.makedirs(BAD_DATA_FOLDER, exist_ok=True)
    
    good_file_path = os.path.join(GOOD_DATA_FOLDER, os.path.basename(file_path))
    bad_file_path = os.path.join(BAD_DATA_FOLDER, os.path.basename(file_path))

    if not df_good.empty:
        df_good.to_csv(good_file_path, index=False)
        print(f"Saved good data to {good_file_path}")

    if not df_bad.empty:
        df_bad.to_csv(bad_file_path, index=False)
        print(f"Saved bad data to {bad_file_path}")

    os.remove(file_path)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2024, 1, 1),
    'retries': 0,
    'email_on_failure': False,
    'email_on_retry': False
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