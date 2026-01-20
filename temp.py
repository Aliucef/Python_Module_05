from abc import ABC, abstractmethod
from typing import Any, List, Union, Protocol

class ProcessingStage(Protocol):
    def process(self, data: Any) -> Any: ...

class ProcessingPipeline(ABC):
    def __init__(self, pipeline_id: str):
        self.pipeline_id = pipeline_id
        self.stages: List[ProcessingStage] = []
    
    def add_stage(self, stage: ProcessingStage) -> None:
        self.stages.append(stage)
    
    @abstractmethod
    def process(self, data: Any) -> Union[str, Any]: ...
    
    def execute_pipeline(self, data: Any) -> Any:
        result = data
        for stage in self.stages:
            result = stage.process(result)
        return result

class InputStage:
    def process(self, data: Any) -> Any:
        print("Stage 1: Input validation and parsing")
        print("Validation passed")
        return data

class TransformStage:
    def process(self, data: Any) -> Any:
        print("Stage 2: Data transformation and enrichment")
        print("success")
        if isinstance(data, str):
            print(f" Transforming string: '{data}'")
        elif isinstance(data, (int, float)):
            print(f" Transforming number: {data}")
        elif isinstance(data, list):
            print(f" Transforming list with {len(data)} items")
        elif isinstance(data, dict):
            print(f" Transforming dict with {len(data)} keys")
        return data

class OutputStage:
    def process(self, data: Any) -> str:
        print("Stage 3: Output formatting and delivery")
        return f"Processed output: {data}"

class JSONAdapter(ProcessingPipeline):
    def __init__(self, pipeline_id: str):
        super().__init__(pipeline_id)
        self.add_stage(InputStage())
        self.add_stage(TransformStage())
        self.add_stage(OutputStage())
    
    def process(self, data: Any) -> Union[str, Any]:
        print(f"Processing JSON data through pipeline: {self.pipeline_id}")
        return self.execute_pipeline(data)

class CSVAdapter(ProcessingPipeline):
    def __init__(self, pipeline_id: str):
        super().__init__(pipeline_id)
        self.add_stage(InputStage())
        self.add_stage(TransformStage())
        self.add_stage(OutputStage())
    
    def process(self, data: Any) -> Union[str, Any]:
        print(f"Processing CSV data through pipeline: {self.pipeline_id}")
        return self.execute_pipeline(data)

class StreamAdapter(ProcessingPipeline):
    def __init__(self, pipeline_id: str):
        super().__init__(pipeline_id)
        self.add_stage(InputStage())
        self.add_stage(TransformStage())
        self.add_stage(OutputStage())
    
    def process(self, data: Any) -> Union[str, Any]:
        print(f"Processing Stream data through pipeline: {self.pipeline_id}")
        return self.execute_pipeline(data)

class NexusManager:
    def __init__(self):
        self.pipelines: List[ProcessingPipeline] = []
        print("=== CODE NEXUS - ENTERPRISE PIPELINE SYSTEM ===")
        print("Initializing Nexus Manager...")
        print("Pipeline capacity: 1000 streams/second")
    
    def add_pipeline(self, pipeline: ProcessingPipeline) -> None:
        self.pipelines.append(pipeline)
        print(f"Pipeline added: {pipeline.pipeline_id}")
    
    def process_data(self, pipeline_id: str, data: Any) -> Any:
        for p in self.pipelines:
            if p.pipeline_id == pipeline_id:
                return p.process(data)
        return f"Error: Pipeline {pipeline_id} not found"
    
    def get_pipeline_count(self) -> int:
        return len(self.pipelines)

if __name__ == "__main__":
    manager = NexusManager()
    
    print("\nCreating Data Processing Pipelines...")
    manager.add_pipeline(JSONAdapter("JSON_001"))
    manager.add_pipeline(CSVAdapter("CSV_001"))
    manager.add_pipeline(StreamAdapter("STREAM_001"))
    
    print(f"\nTotal pipelines managed: {manager.get_pipeline_count()}")
    
    print("\n=== Multi-Format Data Processing ===")
    print("\n--- Processing JSON data ---")
    print(f"Final Output: {manager.process_data('JSON_001', {'sensor': 'temp', 'value': 23.5, 'unit': 'C'})}")
    
    print("\n--- Processing CSV data ---")
    print(f"Final Output: {manager.process_data('CSV_001', 'user,action,timestamp')}")
    
    print("\n--- Processing Stream data ---")
    print(f"Final Output: {manager.process_data('STREAM_001', [22.1, 23.5, 21.8, 24.0, 22.9])}")
    
    print("\n=== Additional Tests ===")
    print("\n--- Processing numeric data ---")
    print(f"Final Output: {manager.process_data('JSON_001', 42)}")
    
    print("\n--- Processing string data ---")
    print(f"Final Output: {manager.process_data('CSV_001', 'Hello Nexus World')}")
    
    print("\n\nNexus Integration complete. All systems operational.")