from fastapi import FastAPI, Depends
from sqlalchemy.orm import Session
import models
import schemas
import crud
from database import SessionLocal, engine

models.Base.metadata.create_all(bind=engine)

app = FastAPI()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.get("/detections/")
def read_detections(skip: int = 0, limit: int = 10, db: Session = Depends(get_db)):
    detections = crud.get_detections(db, skip=skip, limit=limit)
    return detections