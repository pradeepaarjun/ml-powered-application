from sqlalchemy import Column, Integer, Float, String
from .base import Base


class Prediction(Base):
    __tablename__ = 'predictions'

    id = Column(Integer, primary_key=True, index=True)
    airline = Column(String)
    class_ = Column(String)
    duration = Column(Float)
    days_left = Column(Integer)
    predicted_price = Column(Float)
