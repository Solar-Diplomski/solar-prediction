from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class PowerPrediction(BaseModel):
    prediction_time: datetime
    model_id: int
    created_at: datetime
    predicted_power: Optional[float] = None
    horizon: float


class ForecastResponse(BaseModel):
    id: int
    prediction_time: datetime
    power_output: Optional[float] = None
