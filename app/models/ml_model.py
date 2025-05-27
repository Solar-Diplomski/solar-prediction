from pydantic import BaseModel
from typing import List, Dict


class MLModel(BaseModel):
    """ML Model metadata"""

    id: int
    name: str
    type: str
    version: int
    features: List[str]
    plant_id: int
    plant_name: str
    is_active: bool
    file_type: str


class MLModelWithFile(BaseModel):
    """ML Model metadata with file content"""

    metadata: MLModel
    file_content: bytes


class MLModelState:
    """State management for ML models organized by plant_id"""

    def __init__(self):
        # Dictionary: plant_id -> List[MLModelWithFile]
        self._models: Dict[int, List[MLModelWithFile]] = {}

    def update_models(self, models: List[MLModelWithFile]) -> None:
        """Update the state with a list of models"""
        self._models.clear()
        for model in models:
            plant_id = model.metadata.plant_id
            if plant_id not in self._models:
                self._models[plant_id] = []
            self._models[plant_id].append(model)

    def get_models_for_plant(self, plant_id: int) -> List[MLModelWithFile]:
        """Get all models for a specific plant"""
        return self._models.get(plant_id, [])

    def get_all_models(self) -> Dict[int, List[MLModelWithFile]]:
        """Get all models organized by plant_id"""
        return self._models.copy()

    def has_models_for_plant(self, plant_id: int) -> bool:
        """Check if there are models for a specific plant"""
        return plant_id in self._models and len(self._models[plant_id]) > 0

    def get_total_models_count(self) -> int:
        """Get total number of models across all plants"""
        return sum(len(models) for models in self._models.values())

    def get_plants_with_models(self) -> List[int]:
        """Get list of plant IDs that have models"""
        return list(self._models.keys())

    def clear(self) -> None:
        """Clear all models from the state"""
        self._models.clear()
