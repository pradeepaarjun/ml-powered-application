import io, sys, os
sys.path.append('C:/Pradeepa/SEMESTER2_EPITA/Data Science in production/ml-powered-application')
import pandas as pd
import joblib
import uvicorn
import numpy as np
from datetime import datetime
from database.crud import save_prediction, get_past_predictions
from database.base import Base
from fastapi import FastAPI, HTTPException, File, UploadFile, Depends
from pydantic import BaseModel
from database.crud import engine, SessionLocal
from sqlalchemy.orm import Session


encoder = joblib.load(open("models/encoder.joblib", "rb"))
model = joblib.load(open("models/model.joblib", "rb"))
Base.metadata.create_all(bind=engine)
app = FastAPI()

class FlightFeatures(BaseModel):
    airline: str
    Class : str 
    duration: float
    days_left: int
    prediction_source: str

class PastPredictionsRequest(BaseModel):
    start_date: datetime
    end_date: datetime
    prediction_source: str

class PredictionRequest(BaseModel):
    airline: str
    class_: str
    duration: float
    days_left: int
    predicted_price: float

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
        
@app.post("/flights/predict-price/")
async def predict_price(flight_features: FlightFeatures, db: Session = Depends(get_db)):
    try:
        airline = flight_features.airline
        Class = flight_features.Class
        duration = flight_features.duration
        days_left = flight_features.days_left
        encoded_features = encoder.transform([[airline, Class]]).toarray().flatten()
        to_predict = np.hstack((encoded_features,duration, days_left))
        predicted_price = model.predict([to_predict])[0]
        predicted_price_json = float(predicted_price)
        save_prediction(db, flight_features.airline, flight_features.Class, flight_features.duration, flight_features.days_left, predicted_price_json,flight_features.prediction_source)
        return {"predicted_price": predicted_price_json}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.post("/flights/predict-price/csv/")
async def predict_prices_from_csv(csv_file: UploadFile = File(...), db: Session = Depends(get_db)):
    try:
        contents = csv_file.file.read()
        df = pd.read_csv(io.BytesIO(contents))
        encoded_feature = encoder.transform(df[['airline','Class']])
        all_features = np.hstack((encoded_feature.toarray(), df[['duration', 'days_left']].values))
        predicted_prices = model.predict(all_features)
        df['predicted_price'] = predicted_prices
        for _, row in df.iterrows():
            save_prediction(db, airline=row['airline'], class_=row['Class'], duration=row['duration'], days_left=row['days_left'], predicted_price=row['predicted_price'], source="webapp")
        return {"predicted_prices": predicted_prices.tolist()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    

@app.post("/past-predictions/")
async def retrieve_past_predictions(request_data: PastPredictionsRequest, db: Session = Depends(get_db)):
    try:
        past_predictions = get_past_predictions(db, request_data.start_date, request_data.end_date, request_data.prediction_source)
        return {"past_predictions": past_predictions}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == '__main__':
    uvicorn.run(app, host='127.0.0.1', port=8000,log_level="info")
