from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel


class PlaygroundFeatureInfo(BaseModel):
    model_id: int
    model_name: str
    features: List[str]
    plant_id: int
    plant_name: str


class PlaygroundPredictionRow(BaseModel):
    timestamp: datetime
    prediction: float


class PlaygroundMetric(BaseModel):
    metric_type: str
    value: float


class PlaygroundPredictionResponse(BaseModel):
    model_id: int
    predictions: List[PlaygroundPredictionRow]
    metrics: List[PlaygroundMetric]
    input_rows: int
    success: bool
    message: str
    validation_errors: Optional[List[str]] = None


class CSVValidationResult(BaseModel):
    is_valid: bool
    errors: List[str]
    timestamps: List[datetime]
    feature_data: List[List[float]]
    row_count: int
