from pydantic import BaseModel
from typing import List, Optional


class PowerPlant(BaseModel):
    id: int
    longitude: Optional[float] = None
    latitude: Optional[float] = None
    capacity: Optional[float] = None


class ModelMetadata(BaseModel):
    id: int
    features: List[str]
    plant_id: int
    file_type: str


class Model(BaseModel):
    id: int
    name: str
    type: str
    version: int
    features: List[str]
    plant_id: int
    plant_name: str
    is_active: bool
    file_type: str
