import io
from fastapi import FastAPI, HTTPException, File, UploadFile
from pydantic import BaseModel
import pandas as pd
import joblib
import uvicorn
from flight_prices import FEATURE_COLUMNS
import numpy as np


encoder = joblib.load(open("models/encoder.joblib", "rb"))
model = joblib.load(open("models/model.joblib", "rb"))
app = FastAPI()

class FlightFeatures(BaseModel):
    airline: str
    Class : str 
    duration: float
    days_left: int

@app.post("/flights/predict-price/")
async def predict_price(flight_features: FlightFeatures):
    try:
        airline = flight_features.airline
        Class = flight_features.Class
        duration = flight_features.duration
        days_left = flight_features.days_left
        encoded_features = encoder.transform([[airline, Class]]).toarray().flatten()
        to_predict = np.hstack((encoded_features,duration, days_left))
        predicted_price = model.predict([to_predict])[0]
        predicted_price_json = float(predicted_price)
        return {"predicted_price": predicted_price_json}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.post("/flights/predict-price/csv/")
async def predict_prices_from_csv(csv_file: UploadFile = File(...)):
    try:
        contents = csv_file.file.read()
        df = pd.read_csv(io.BytesIO(contents))
        encoded_feature = encoder.transform(df[['airline','Class']])
        all_features = np.hstack((encoded_feature.toarray(), df[['duration', 'days_left']].values))
        predicted_prices = model.predict(all_features)
        return {"predicted_prices": predicted_prices.tolist()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
if __name__ == '__main__':
    uvicorn.run(app, host='127.0.0.1', port=8000,log_level="info")
