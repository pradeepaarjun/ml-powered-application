from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import requests
import os


GOOD_DATA_FOLDER = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', 'data', 'good-data'))

def check_for_new_data(**kwargs):
    files = os.listdir(GOOD_DATA_FOLDER)
    new_files = [file for file in files if file.endswith('.csv')] 
    if new_files:
        file_paths = [os.path.join(GOOD_DATA_FOLDER, file) for file in new_files]
        kwargs['ti'].xcom_push(key='file_paths', value=file_paths)
    else:
        raise ValueError("No new files found.")


def make_predictions(**kwargs):
    file_paths = kwargs['ti'].xcom_pull(key='file_paths', task_ids='check-for-new-data')
    try:
        for file in file_paths:
            df = pd.read_csv(file)
            for index, row in df.iterrows():
                airline = row['airline']
                Class = row['Class']
                duration = row['duration']
                days_left = row['days_left']
                api_url = "http://127.0.0.1:8000/flights/predict-price/"

                prediction_data = {
                    "airline": airline,
                    "Class": Class,
                    "duration": duration,
                    "days_left": days_left,
                    "prediction_source": "scheduled predictions"
                }

                response = requests.post(api_url, json=[prediction_data])

                if response.status_code == 200:
                    print(f"Prediction for {airline} saved successfully!")
                else:
                    print(f"Failed to save prediction for {airline}: {response.text}")

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