# Flight price prediction
This web application built for predicting flight prices based on various features such as airline, class, duration, and days left to departure. It provides users with the ability to input flight details manually or upload a CSV file containing multiple flight records for batch prediction.

## Features

- **Manual Input**: Users can input flight details manually and receive instant price predictions.
- **Upload CSV**: Users can upload a CSV file containing multiple flight records for batch prediction.
- **Past Predictions**: View past predictions made by the web app or scheduled prediction jobs.
Prerequisites:
1)Python 3.10
2)Conda (or any other virtual environment manager)
## Getting started

### Installation and setup

#### Setup development environment

1. Clone the repository:

```bash
git clone <repository_url>

2. Create a virtual environment with `Python 3.10` and activate it (with conda or any other environment manager)

```shell
conda create -y python=3.10 --name ml-app
conda activate ml-app

3. Install the required dependencies:

```shell
pip install -r requirements.txt
```

## Usage

### Access the web application:

#### Run the below command:

streamlit run streamlit_app.py

Open a web browser and go to http://localhost:8000/ to access the application.

### Run the Fast-API:

uvicorn fastapi_app:app --reload || option to ensure that the server reacts to any changes made to your code

### Project Structure

Streamlit_app.py: Streamlit web application code for the user interface.

fastAPI.py: FastAPI code for serving prediction endpoints.

models/: Directory containing the trained machine learning models.

database/: Directory containing database models and CRUD operations.

README.md: Documentation file for the project.

##Airflow Integration
Apache Airflow is used to orchestrate and schedule batch prediction jobs. Here’s how to set it up and use it in this project.

##Setting Up Airflow
###Install Airflow:
Airflow requires several dependencies. It's recommended to use the constraints file provided by the Airflow project:

pip install apache-airflow==2.3.0 --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.3.0/constraints-3.10.txt"

###Initialize Airflow Database:
airflow db init

###Create an Airflow User:
```shell
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com
```
###Start Airflow Services:
```shell
airflow webserver --port 8080
airflow scheduler
```
###Access the Airflow UI:
Open a web browser and go to http://localhost:8080/ to access the Airflow UI.

###Airflow Project Structure
airflow/dags/: Directory containing Airflow DAGs (Directed Acyclic Graphs) that define the workflow for batch predictions.
airflow/dag/data_ingestion.py: A DAG that Read, validate against greater expectations, send alerts and save the error report to the postgres
airflow/dags/predict_new_data_dag.py: A DAG that schedules and runs batch prediction jobs.












