from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel


class PowerReading(BaseModel):
    timestamp: datetime
    power_w: float


class CSVUploadResponse(BaseModel):
    success: bool
    message: str
    validation_errors: Optional[List[str]] = None


class CSVValidationResult(BaseModel):
    is_valid: bool
    errors: List[str]
    readings: List[PowerReading]
