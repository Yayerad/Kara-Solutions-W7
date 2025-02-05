from sqlalchemy import Column, Integer, String, DateTime, JSON
from database import Base

class Detection(Base):
    __tablename__ = "detections"
    id = Column(Integer, primary_key=True)
    image_id = Column(Integer)
    class_name = Column(String)
    confidence = Column(Integer)
    bbox = Column(JSON)
    detected_at = Column(DateTime)