from fastapi import HTTPException
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from .models import Prediction
from datetime import datetime
from typing import Dict, List, Union
import logging
import pandas as pd

URL_DATABASE = 'postgresql://postgres:password@localhost:5432/FlightPrediction'
engine = create_engine(URL_DATABASE)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
session = Session()
table = 'predictions'
def save_prediction(db: Session, prediction_data: List[Dict[str, Union[str, float, int]]]):
    try:
        df = pd.DataFrame(prediction_data)
        df.rename(columns={'Class': 'class_'}, inplace=True)
        df['prediction_date'] = datetime.now()
        df.to_sql(table, engine, if_exists='append', index=False)
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    
def get_past_predictions(db: Session, start_date: datetime, end_date: datetime, prediction_source: str) -> List[Prediction]:
    if prediction_source == "webapp":
        return db.query(Prediction).filter(Prediction.prediction_source == "webapp", Prediction.prediction_date.between(start_date, end_date)).all()
    elif prediction_source == "scheduled predictions":
        return db.query(Prediction).filter(Prediction.prediction_source == "scheduled predictions", Prediction.prediction_date.between(start_date, end_date)).all()
    elif prediction_source == "all":
        return db.query(Prediction).filter(Prediction.prediction_date.between(start_date, end_date)).all()
    else:
        return []
