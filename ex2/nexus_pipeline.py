from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Protocol


class ProcessingStage(Protocol):
    """Defines what a 'stage' must have - just a process() method"""

    def process(self, data: Any) -> Any:
        pass


class ProcessingPipeline(ABC):
    """Base class for all pipelines - manages stages"""

    def __init__(self, pipeline_id: str):
        self.pipeline_id = pipeline_id
        self.stages: List[ProcessingStage] = []

    def add_stage(self, stage: ProcessingStage) -> None:
        """Add a processing stage to the pipeline"""
        self.stages = self.stages + [stage]

    @abstractmethod
    def process(self, data: Any) -> Union[str, Any]:
        """Must be overridden by subclasses"""
        pass

    def execute_pipeline(self, data: Any) -> Any:
        """Run data through all stages sequentially"""
        current_data = data

        try:
            for stage in self.stages:
                current_data = stage.process(current_data)
            return current_data
        except Exception as e:
            return {"error": f"Pipeline execution failed: {str(e)}"}


class InputStage:
    """Validates and prepares incoming data"""

    def process(self, data: Any) -> Union[str, Any]:
        print("Stage 1: Input validation and parsing")
        try:
            if not self.validate_data(data):
                return {"error": "Invalid data", "stage": "input"}

            processed_data = {
                "input": data,
                "validation": True,
                "stage": "input"
            }
            return processed_data
        except Exception as e:
            return {"error": f"Input stage error: {str(e)}", "stage": "input"}

    def validate_data(self, data: Any) -> bool:
        """check if data is valid"""
        if data is None:
            print("Validation failed: Data is None")
            return False
        if isinstance(data, str) and not data.strip():
            print("Validation failed: string is Empty")
            return False
        if isinstance(data, list) and len(data) == 0:
            print("Validation failed: List is Empty")
            return False
        if isinstance(data, dict) and len(data) == 0:
            print("Validation failed: Dict is Empty")
            return False
        print("Validation passed")
        return True


class TransformStage:
    """Transforms/enriches the data"""

    def process(self, data: Any) -> Dict:
        print("Stage 2: Data transformation and enrichment")
        try:
            extracted_data = self.extract_data(data)
            if extracted_data is None:
                return {"error": "Failed to extract data",
                        "stage": "transform"}

            transformed_data = self.transform_data(extracted_data)

            result = {
                "input": data,
                "transformed": transformed_data,
                "enriched": True,
                "stage": "transform"
            }
            return result

        except Exception as e:
            return {
                "error": f"Transform stage error: {str(e)}",
                "stage": "transform"}

    def extract_data(self, data: Any):
        """Extract actual data from the previous stage's output"""
        if isinstance(data, dict):
            if "input" in data:
                print("success")
                return data["input"]
            else:
                print("dict does not have input field in it")
                return data

        else:
            print("data is not a dict")
            return data

    def transform_data(self, data: Any) -> Any:
        """Apply transformations based on data type"""

        if isinstance(data, str):
            print(f" Transforming string: '{data}'")
            return {
                "original": data,
                "uppercase": data.upper(),
                "length": len(data),
                "type": "string"
            }

        elif isinstance(data, (int, float)):
            print(f" Transforming number: {data}")
            return {
                "original": data,
                "doubled": data * 2,
                "squared": data ** 2,
                "type": "numeric"
            }

        elif isinstance(data, list):
            print(f" Transforming list with {len(data)} items")
            return {
                "original": data,
                "count": len(data),
                "first": data[0] if data else None,
                "last": data[-1] if data else None,
                "type": "list"
            }

        elif isinstance(data, dict):
            print(f" Transforming dict with {len(data)} keys")
            return {
                "original": data,
                "keys": list(data.keys()),
                "key_count": len(data),
                "type": "dict"
            }

        else:
            print(f" Unknown type: {type(data)}, passing through")
            return data


class OutputStage:
    """This transforms the extracted data into a readable phrase"""

    def process(self, data: Any) -> str:
        print("Stage 3: Output formatting and delivery")

        try:
            extracted_data = self.extract_data(data)

            if extracted_data is None:
                return "Error: No data to format"

            formatted_output = self.format_output(extracted_data)

            return formatted_output

        except Exception as e:
            return f"Output stage error: {str(e)}"

    def extract_data(self, data: Any) -> Any:
        if isinstance(data, dict):
            if "transformed" in data:
                return data["transformed"]
            else:
                return data
        else:
            return data

    def format_output(self, data: Any) -> str:
        if isinstance(data, dict):
            data_type = data.get("type") if "type" in data else None

            if data_type == "string":
                original = data.get("original") if "original" in data else ""
                length = data.get("length") if "length" in data else 0
                return (f"Processed text: {length}",
                        f"characters, content: '{original}'")

            elif data_type == "numeric":
                original = data.get("original") if "original" in data else 0
                doubled = data.get("doubled") if "doubled" in data else 0
                return f"Processed number: {original} (doubled: {doubled})"

            elif data_type == "list":
                count = data.get("count") if "count" in data else 0
                first = data.get("first") if "first" in data else None
                last = data.get("last") if "last" in data else None
                return (f"Processed list: {count}",
                        f"items (first: {first}, last: {last})")

            elif data_type == "dict":
                key_count = data.get("key_count") if "key_count" in data else 0
                keys = data.get("keys") if "keys" in data else []
                keys_str = ", ".join([str(k) for k in keys])
                return f"Processed dict: {key_count} keys ({keys_str})"

        return f"Output: {str(data)}"


class JSONAdapter(ProcessingPipeline):
    """Specialized pipeline for JSON data"""

    def __init__(self, pipeline_id: str):
        super().__init__(pipeline_id)
        self.add_stage(InputStage())
        self.add_stage(TransformStage())
        self.add_stage(OutputStage())

    def process(self, data: Any) -> Union[str, Any]:
        """Override: JSON-specific processing"""
        print(f"Processing JSON data through pipeline: {self.pipeline_id}")
        try:
            result = self.execute_pipeline(data)
            return result
        except Exception as e:
            return f"JSON processing error: {str(e)}"


class CSVAdapter(ProcessingPipeline):
    """Specialized pipeline for CSV data"""

    def __init__(self, pipeline_id: str):
        super().__init__(pipeline_id)
        self.add_stage(InputStage())
        self.add_stage(TransformStage())
        self.add_stage(OutputStage())

    def process(self, data: Any) -> Union[str, Any]:
        """Override: CSV-specific processing"""
        print(f"Processing CSV data through pipeline: {self.pipeline_id}")
        try:
            result = self.execute_pipeline(data)
            return result
        except Exception as e:
            return f"CSV processing error: {str(e)}"


class StreamAdapter(ProcessingPipeline):
    """Specialized pipeline for streaming data"""

    def __init__(self, pipeline_id: str):
        super().__init__(pipeline_id)
        self.add_stage(InputStage())
        self.add_stage(TransformStage())
        self.add_stage(OutputStage())

    def process(self, data: Any) -> Union[str, Any]:
        """Override: Stream-specific processing"""
        print(f"Processing Stream data through pipeline: {self.pipeline_id}")
        try:
            result = self.execute_pipeline(data)
            return result
        except Exception as e:
            return f"Stream processing error: {str(e)}"


class NexusManager:
    """Manages multiple pipelines polymorphically"""

    def __init__(self):
        self.pipelines: List[ProcessingPipeline] = []
        print("=== CODE NEXUS - ENTERPRISE PIPELINE SYSTEM ===")
        print("Initializing Nexus Manager...")
        print("Pipeline capacity: 1000 streams/second")

    def add_pipeline(self, pipeline: ProcessingPipeline) -> None:
        """Add a pipeline to manage"""
        self.pipelines = self.pipelines + [pipeline]
        print(f"Pipeline added: {pipeline.pipeline_id}")

    def process_data(self, pipeline_id: str, data: Any) -> Any:
        """Process data through a specific pipeline"""
        try:
            for pipeline in self.pipelines:
                if pipeline.pipeline_id == pipeline_id:
                    return pipeline.process(data)
            return f"Error: Pipeline {pipeline_id} not found"
        except Exception as e:
            return f"Manager error: {str(e)}"

    def get_pipeline_count(self) -> int:
        """Get number of managed pipelines"""
        return len(self.pipelines)


if __name__ == "__main__":
    # Create Nexus Manager
    manager = NexusManager()

    # Create pipelines
    print("\nCreating Data Processing Pipelines...")
    json_pipeline = JSONAdapter("JSON_001")
    csv_pipeline = CSVAdapter("CSV_001")
    stream_pipeline = StreamAdapter("STREAM_001")

    # Add pipelines to manager
    manager.add_pipeline(json_pipeline)
    manager.add_pipeline(csv_pipeline)
    manager.add_pipeline(stream_pipeline)

    print(f"\nTotal pipelines managed: {manager.get_pipeline_count()}")

    # Test JSON processing
    print("\n=== Multi-Format Data Processing ===")
    print("\n--- Processing JSON data ---")
    json_data = {"sensor": "temp", "value": 23.5, "unit": "C"}
    result = manager.process_data("JSON_001", json_data)
    print(f"Final Output: {result}")

    # Test CSV processing
    print("\n--- Processing CSV data ---")
    csv_data = "user,action,timestamp"
    result = manager.process_data("CSV_001", csv_data)
    print(f"Final Output: {result}")

    # Test Stream processing
    print("\n--- Processing Stream data ---")
    stream_data = [22.1, 23.5, 21.8, 24.0, 22.9]
    result = manager.process_data("STREAM_001", stream_data)
    print(f"Final Output: {result}")

    # Test with different data types
    print("\n=== Additional Tests ===")
    print("\n--- Processing numeric data ---")
    result = manager.process_data("JSON_001", 42)
    print(f"Final Output: {result}")

    print("\n--- Processing string data ---")
    result = manager.process_data("CSV_001", "Hello Nexus World")
    print(f"Final Output: {result}")

    print("\n\nNexus Integration complete. All systems operational.")
