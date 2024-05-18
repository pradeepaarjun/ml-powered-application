import glob
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from airflow.hooks.http_hook import HttpHook
import json

GOOD_DATA_FOLDER = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', 'data', 'good-data'))

def check_for_new_data(ti):
    good_data_files = glob.glob(os.path.join(GOOD_DATA_FOLDER, '*.csv'))
    print('Files are', good_data_files)
    last_processed_time = Variable.get("last_processed_time", default_var=str(datetime.min))
    print('Last processed time is', last_processed_time)

    new_files = [f for f in good_data_files if datetime.fromtimestamp(os.path.getmtime(f)) > datetime.fromisoformat(last_processed_time)]
    print('New files are', new_files)
    if not new_files:
        print("No new data found, skipping this run.")
        raise AirflowSkipException("No new data found, skipping this run.")

    ti.xcom_push(key='new_files', value=new_files)


def make_predictions(ti):
    new_files = ti.xcom_pull(key='new_files', task_ids='check-for-new-data')
    if not new_files:
        print("No new files found. Skipping make_predictions.")
        return

    try:
        input_data_list = []
        for file in new_files:
            df = pd.read_csv(file)
            for _, row in df.iterrows():
                flight_features = {
                    "airline": row['airline'],
                    "Class": row['Class'],
                    "duration": row['duration'],
                    "days_left": row['days_left'],
                    "prediction_source": "scheduled predictions"
                }
                input_data_list.append(flight_features)

        input_data_json = json.dumps(input_data_list)

        http_hook = HttpHook(method='POST', http_conn_id="fastapi")
        
        response = http_hook.run(
            endpoint="/flights/predict-price/",
            data=input_data_json,
            headers={'Content-Type': 'application/json'}
        )
        
        if response.status_code == 200:
            print("Prediction request successful!")
            response_data = response.json()
            print("Response data:", response_data)
        else:
            print("Prediction request failed. Status code:", response.status_code)
            print("Response content:", response.text)

        # Update the last processed time if the request was successful
        Variable.set("last_processed_time", str(datetime.now()))

    except Exception as e:
        print(f"An error occurred while processing prediction: {e}")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 27),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'predict_new_data_dag', 
     default_args=default_args, 
     schedule_interval="*/5 * * * *")

with dag:
    check_for_new_data_task = PythonOperator(
            task_id='check-for-new-data',
            python_callable=check_for_new_data,
        )

    make_predictions_task = PythonOperator(
            task_id='make-predictions',
            python_callable=make_predictions,
            provide_context=True,
        )

    check_for_new_data_task >> make_predictions_task
