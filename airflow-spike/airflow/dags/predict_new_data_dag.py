from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import requests
import os


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 27),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('predict_new_data_dag', default_args=default_args, schedule_interval="*/5 * * * *")

def process_csv_and_call_api():
    try:
        input_data_dir = os.path.join(os.path.dirname(__file__), "..", "input_data")
        csv_files = [file for file in os.listdir(input_data_dir) if file.endswith(".csv")]
        for csv_file in csv_files:
            file_path = os.path.join(input_data_dir, csv_file)
            df = pd.read_csv(file_path)
            for index, row in df.iterrows():
                airline = row['airline']
                Class = row['Class']
                duration = row['duration']
                days_left = row['days_left']
                api_url = "http://localhost:8000/flights/predict-price/"

                prediction_data = {
                    "airline": airline,
                    "Class": Class,
                    "duration": duration,
                    "days_left": days_left,
                    "prediction_source": "scheduled predictions"
                }

                response = requests.post(api_url, json=prediction_data)

                if response.status_code == 200:
                    print(f"Prediction for {airline} saved successfully!")
                else:
                    print(f"Failed to save prediction for {airline}: {response.text}")

    except Exception as e:
        print(f"An error occurred while processing prediction: {e}")

process_csv_and_call_api_task = PythonOperator(
    task_id='process_csv_and_call_api_task',
    python_callable=process_csv_and_call_api,
    dag=dag
)

process_csv_and_call_api_task
