from sqlalchemy import Column, Integer, Float, String, DateTime
from .base import Base
from datetime import datetime


class Prediction(Base):
    __tablename__ = 'predictions'

    id = Column(Integer, primary_key=True, index=True)
    airline = Column(String)
    class_ = Column(String)
    duration = Column(Float)
    days_left = Column(Integer)
    predicted_price = Column(Float)
    prediction_date = Column(DateTime, default=datetime.now)
    prediction_source = Column(String)
