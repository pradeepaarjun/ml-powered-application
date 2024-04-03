import requests
import json
import pandas as pd

base_url = "http://localhost:8000"


flight_data = {
    "airline": "SpiceJet",
    "Class": "Economy",
    "duration": 2.17,
    "days_left": 1
}

csv_file_path = "../../data/Book1.csv"

def test_predict_price_endpoint():
    endpoint = "/flights/predict-price/"
    url = base_url + endpoint
    response = requests.post(url, json=flight_data)
    
    if response.status_code == 200:
        result = response.json()
        print("Predicted price:", result["predicted_price"])
    else:
        print("Error:", response.text)

def test_predict_price_csv_endpoint():
    endpoint = "/flights/predict-price/csv"
    url = base_url + endpoint
    csv_file = open(csv_file_path, "rb")
    files = {'csv_file': csv_file}
    response = requests.post(url, files=files)
    
    if response.status_code == 200:
        result = response.json()
        print("Predicted prices:", result["predicted_prices"])
    else:
        print("Error:", response.text)

if __name__ == "__main__":
    #print("Testing /flights/predict-price/ endpoint:")
    #test_predict_price_endpoint()
    print("\nTesting /flights/predict-price/csv endpoint:")
    test_predict_price_csv_endpoint()