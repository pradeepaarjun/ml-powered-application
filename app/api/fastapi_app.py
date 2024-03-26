import io
from fastapi import FastAPI, HTTPException, File, UploadFile
from pydantic import BaseModel
import pandas as pd
import joblib
import uvicorn
import json

model = joblib.load("models/model.joblib")
app = FastAPI()

class FlightFeatures(BaseModel):
    airline: str
    class_: str
    duration: float
    days_left: int

@app.post("/flights/predict-price/")
async def predict_price(flight_features: FlightFeatures):
    try:
        airLine = flight_features.airline
        Class_= flight_features.class_
        Duration = flight_features.duration
        Days_Left = flight_features.days_left
        input_list = [airLine, Class_, Duration, Days_Left]
        predicted_price = model.predict([input_list])
        return {"predicted_price": predicted_price}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.post("/flights/predict-price/csv")
async def predict_prices_from_csv(csv_file: UploadFile = File(...)):
    try:
        contents = csv_file.file.read()
        df = pd.read_csv(io.BytesIO(contents))
        predicted_prices = model.predict(df[['airline', 'class_', 'duration', 'days_left']])
        return {"predicted_prices": predicted_prices.tolist()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
if __name__ == '__main__':
    uvicorn.run(app, host='127.0.0.1', port=8000,log_level="info")
