from typing import List
from pydantic import BaseModel
from decimal import Decimal


class HorizonMetricTypesResponse(BaseModel):
    metric_types: List[str]


class HorizonMetric(BaseModel):
    metric_type: str
    horizon: Decimal
    value: Decimal


class HorizonMetricsResponse(BaseModel):
    metrics: List[HorizonMetric]
