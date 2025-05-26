from pydantic import BaseModel
from typing import Dict, List, Optional


class PowerPlant(BaseModel):
    id: int
    longitude: Optional[float] = None
    latitude: Optional[float] = None

    def __hash__(self):
        return hash(self.id)

    def __eq__(self, other):
        if isinstance(other, PowerPlant):
            return self.id == other.id
        return False


class PowerPlantState:

    def __init__(self):
        self._power_plants: Dict[int, PowerPlant] = {}

    def update_power_plants(self, power_plants: List[PowerPlant]) -> None:
        """Update the state with a list of power plants"""
        self._power_plants.clear()
        for plant in power_plants:
            self._power_plants[plant.id] = plant

    def get_power_plant(self, plant_id: int) -> PowerPlant | None:
        """Get a specific power plant by ID"""
        return self._power_plants.get(plant_id)

    def get_all_power_plants(self) -> List[PowerPlant]:
        """Get all power plants as a list"""
        return list(self._power_plants.values())

    def get_power_plants_dict(self) -> Dict[int, PowerPlant]:
        """Get all power plants as a dictionary (id -> PowerPlant)"""
        return self._power_plants.copy()

    def has_power_plant(self, plant_id: int) -> bool:
        """Check if a power plant exists in the state"""
        return plant_id in self._power_plants

    def count(self) -> int:
        """Get the number of power plants in the state"""
        return len(self._power_plants)

    def clear(self) -> None:
        """Clear all power plants from the state"""
        self._power_plants.clear()
