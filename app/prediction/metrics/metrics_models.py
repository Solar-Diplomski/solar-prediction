from typing import List
from pydantic import BaseModel


class HorizonMetricTypesResponse(BaseModel):
    metric_types: List[str]
