
from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Optional

class DataStream(ABC):

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        pass

    def filter_data(self, data_batch: List[Any], criteria: Optional[str] = None) -> List[Any]:
        try:
            return data_batch
        except Exception:
            return []

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        try:
            return {"stream": "DataStream", "status": "no stats available"}
        except Exception:
            return {}

class SensorStream(DataStream):
    def __init__(self, stream_id):
        self.stream_id = stream_id
        self.stream_type = "Environmental Data"

    def process_batch(self, data_batch):
        print(f"Initializing Sensor Stream...")
        print(f"Stream ID: {self.stream_id}, Type: {self.stream_type}")
        print(f"Processing sensor batch: {data_batch}")

        total = 0
        count = 0
        try:
            for reading in data_batch:
                reading += 0
                total += reading
                count += 1

            avg = total / count if count > 0 else 0
            return f"Sensor analysis: {count} readings processed, avg temp: {avg}°C"

        except Exception:
            return "Sensor analysis: invalid data"
    def get_stats(self):
        try:
            return {
                "stream_id": self.stream_id,
                "type": "Sensor",
                "status": "ready"
            }
        except Exception:
            return {}
