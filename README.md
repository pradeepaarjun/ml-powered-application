# Flight price prediction
This web application built for predicting flight prices based on various features such as airline, class, duration, and days left to departure. It provides users with the ability to input flight details manually or upload a CSV file containing multiple flight records for batch prediction.

## Features

- **Manual Input**: Users can input flight details manually and receive instant price predictions.
- **Upload CSV**: Users can upload a CSV file containing multiple flight records for batch prediction.
- **Past Predictions**: View past predictions made by the web app or scheduled prediction jobs.

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