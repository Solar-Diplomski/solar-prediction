from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class PowerPrediction(BaseModel):
    prediction_time: datetime
    model_id: int
    created_at: datetime
    predicted_power_mw: Optional[float] = None
