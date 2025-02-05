from sqlalchemy.orm import Session
from . import models

def get_detections(db: Session, skip: int = 0, limit: int = 10):
    return db.query(models.Detection).offset(skip).limit(limit).all()