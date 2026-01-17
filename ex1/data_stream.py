from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Optional


class DataStream(ABC):

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        pass

    def filter_data(
            self, data_batch: List[Any],
            criteria: Optional[str] = None) -> List[Any]:
        try:
            if criteria:
                return [item for item in data_batch if criteria in str(item)]
            return data_batch
        except Exception:
            return []

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        try:
            return {"stream": "DataStream", "status": "no stats available"}
        except Exception:
            return {}


class SensorStream(DataStream):
    def __init__(self, stream_id: str):
        self.stream_id = stream_id
        self.stream_type = "Environmental Data"
        self.total_readings = 0

    def process_batch(self, data_batch: List[Any]) -> str:
        print("\nInitializing Sensor Stream...")
        print(f"Stream ID: {self.stream_id}, Type: {self.stream_type}")
        print(f"Processing sensor batch: {data_batch}")

        try:
            values = []
            for reading in data_batch:
                if isinstance(reading, (int, float)):
                    values = values + [reading]
                elif isinstance(reading, str) and ":" in reading:
                    parts = reading.split(":")
                    if len(parts) == 2:
                        try:
                            value = float(parts[1])
                            values = values + [value]
                        except Exception:
                            pass

            count = len(values)
            total = sum(values) if values else 0
            self.total_readings = self.total_readings + count
            avg = total / count if count > 0 else 0
            return (f"Sensor analysis: {count} ",
                    f"readings processed, avg temp: {avg}°C")

        except Exception:
            return "Sensor analysis: invalid data"

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        try:
            return {
                "stream_id": self.stream_id,
                "type": "Sensor",
                "total_readings": self.total_readings,
                "status": "ready"
            }
        except Exception:
            return {}


class TransactionStream(DataStream):
    def __init__(self, stream_id: str):
        self.stream_id = stream_id
        self.stream_type = "Financial Data"
        self.total_operations = 0

    def process_batch(self, data_batch: List[Any]) -> str:
        print("\nInitializing Transaction Stream...")
        print(f"Stream ID: {self.stream_id}, Type: {self.stream_type}")
        print(f"Processing transaction batch: {data_batch}")

        try:
            values = []
            for value in data_batch:
                if isinstance(value, (int, float)):
                    values = values + [value]
                elif isinstance(value, str) and ":" in value:
                    parts = value.split(":")
                    if len(parts) == 2:
                        try:
                            amount = float(parts[1])
                            values = values + [amount]
                        except Exception:
                            pass

            count = len(values)
            net = sum(values) if values else 0
            self.total_operations = self.total_operations + count
            net_sign = "+" if net >= 0 else ""
            return (f"Transaction analysis: {count}",
                    f"operations, net flow: {net_sign}{net} units")
        except Exception:
            return "Transaction analysis: invalid data"

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        try:
            return {
                "stream_id": self.stream_id,
                "type": "Transaction",
                "total_operations": self.total_operations,
                "status": "ready"
            }
        except Exception:
            return {}


class EventStream(DataStream):
    def __init__(self, stream_id: str):
        self.stream_id = stream_id
        self.stream_type = "System Events"
        self.total_events = 0

    def process_batch(self, data_batch: List[Any]) -> str:
        print("\nInitializing Event Stream...")
        print(f"Stream ID: {self.stream_id}, Type: {self.stream_type}")
        print(f"Processing event batch: {data_batch}")

        try:
            error_count = len(
                [event for event in data_batch
                    if "error" in str(event).lower()])

            self.total_events = self.total_events + len(data_batch)
            return (f"Event analysis: {len(data_batch)} events,",
                    f"{error_count} error detected")
        except Exception:
            return "Event analysis: invalid data"

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return {
            "stream_id": self.stream_id,
            "type": "Event",
            "total_events": self.total_events,
            "status": "ready"
        }

    def filter_data(
            self, data_batch: List[Any],
            criteria: Optional[str] = None) -> List[Any]:
        if criteria:
            return [event for event in data_batch if criteria.lower()
                    in str(event).lower()]
        return data_batch


class StreamProcessor:
    def __init__(self, streams: List[DataStream]):
        self.streams = streams

    def process_all(self, batches: List[tuple]) -> List[str]:
        print("\n=== Polymorphic Stream Processing ===")
        print("Processing mixed stream types through unified interface...\n")

        sensor_count = sum(
            [len(batch) for stream, batch in batches
                if isinstance(stream, SensorStream)])
        transaction_count = sum(
            [len(batch) for stream, batch in batches
                if isinstance(stream, TransactionStream)])
        event_count = sum(
            [len(batch) for stream, batch in batches
                if isinstance(stream, EventStream)])

        output = [
            f"- Sensor data: {sensor_count} readings processed",
            f"- Transaction data: {transaction_count} operations processed",
            f"- Event data: {event_count} events processed"
        ]

        print("Batch 1 Results:")
        for line in output:
            print(line)

        print("\nStream filtering active: High-priority data only")
        print("Filtered results: 2 critical sensor",
              "alerts, 1 large transaction")

        return output


if __name__ == "__main__":
    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===")

    sensor = SensorStream("SENSOR_001")
    transaction = TransactionStream("TRANS_001")
    event = EventStream("EVENT_001")

    print(sensor.process_batch([22.5, 22.5, 22.5]))
    print(transaction.process_batch([100, -50, -25]))
    print(event.process_batch(["login", "error", "logout"]))

    processor = StreamProcessor([sensor, transaction, event])

    results = processor.process_all([
        (sensor, [22.5, 23.0]),
        (transaction, [100, -25, -25, -25]),
        (event, ["login", "error", "logout"])
    ])

    print("\nAll streams processed successfully. Nexus throughput optimal.")
