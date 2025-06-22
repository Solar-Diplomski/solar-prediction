from typing import List
from pydantic import BaseModel
from decimal import Decimal
from datetime import datetime


class HorizonMetricTypesResponse(BaseModel):
    metric_types: List[str]


class HorizonMetric(BaseModel):
    metric_type: str
    horizon: Decimal
    value: Decimal


class HorizonMetricsResponse(BaseModel):
    metrics: List[HorizonMetric]


class CycleMetric(BaseModel):
    time_of_forecast: datetime
    metric_type: str
    value: Decimal
