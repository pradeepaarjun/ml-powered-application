from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from .models import Prediction
from datetime import datetime
from typing import List


URL_DATABASE = 'postgresql://postgres:password@localhost:5432/FlightPrediction'
engine = create_engine(URL_DATABASE)
SessionLocal = sessionmaker(bind=engine)
session = Session()

def save_prediction(db: Session, airline: str, class_: str, duration: float, days_left: int, predicted_price: float, prediction_source: str):
    prediction_date = datetime.now()
    prediction = Prediction(airline=airline, class_=class_, duration=duration, days_left=days_left, predicted_price=predicted_price, prediction_date=prediction_date, prediction_source=prediction_source)
    db.add(prediction)
    db.commit()
    db.refresh(prediction)

def get_past_predictions(db: Session, start_date: datetime, end_date: datetime, prediction_source: str) -> List[Prediction]:
    if prediction_source == "webapp":
        return db.query(Prediction).filter(Prediction.prediction_source == "webapp", Prediction.prediction_date.between(start_date, end_date)).all()
    elif prediction_source == "scheduled predictions":
        return db.query(Prediction).filter(Prediction.prediction_source == "scheduled predictions", Prediction.prediction_date.between(start_date, end_date)).all()
    elif prediction_source == "all":
        return db.query(Prediction).filter(Prediction.prediction_date.between(start_date, end_date)).all()
    else:
        return []
