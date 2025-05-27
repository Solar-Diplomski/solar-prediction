from abc import ABC, abstractmethod
from typing import Any, List
import joblib
from pydantic import BaseModel
from typing import Optional
import io


class ModelMetadata(BaseModel):
    id: int
    features: List[str]
    plant_id: int
    file_type: str


class MLModel(ABC):

    def __init__(self, metadata: ModelMetadata, file_content: bytes):
        self.metadata: ModelMetadata = metadata
        self.features: List[str] = metadata.features
        self.plant_id: int = metadata.plant_id
        self._model: Optional[Any] = None
        self._load(file_content)

    @abstractmethod
    def _load(self, file_content: bytes):
        pass

    @abstractmethod
    def predict(self, features: List[List[float]]) -> float:
        pass


class JoblibModel(MLModel):

    def _load(self, file_content: bytes):
        file_like_object = io.BytesIO(file_content)
        self._model = joblib.load(file_like_object)

    def predict(self, features: List[List[float]]) -> List[float]:
        return self._model.predict(features)


class PowerPlant(BaseModel):
    id: int
    longitude: Optional[float] = None
    latitude: Optional[float] = None
    capacity: Optional[float] = None
