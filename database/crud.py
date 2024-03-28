from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from .models import Prediction
    


URL_DATABASE = 'postgresql://postgres:password@localhost:5432/FlightPrediction'
engine = create_engine(URL_DATABASE)
SessionLocal = sessionmaker(bind=engine)
session = Session()

def save_prediction(db: Session, airline: str, class_: str, duration: float, days_left: int, predicted_price: float):
    prediction = Prediction(airline=airline, class_=class_, duration=duration, days_left=days_left, predicted_price=predicted_price)
    db.add(prediction)
    db.commit()
    db.refresh(prediction)