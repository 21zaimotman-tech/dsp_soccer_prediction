from fastapi import FastAPI
from pydantic import BaseModel
from typing import List
import uvicorn

app = FastAPI(title="Soccer Prediction API")

class MatchFeatures(BaseModel):
    home_team: str
    away_team: str

class PredictionResponse(BaseModel):
    prediction: str
    model_version: str = "v1.0-dummy"

@app.get("/health")
async def health():
    return {"status": "healthy"}

@app.post("/predict", response_model=List[PredictionResponse])
async def predict(inputs: List[MatchFeatures]):
    # Placeholder for Defense 2 ML integration
    return [{"prediction": "Home Win", "model_version": "v1.0-dummy"} for _ in inputs]

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)