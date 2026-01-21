from abc import ABC, abstractmethod
from typing import Any


class DataProcessor(ABC):

    @abstractmethod
    def process(self, data: Any) -> str:
        pass

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    def format_output(self, result: str) -> str:
        return f"Output: {result}"


class NumericProcessor(DataProcessor):

    def __init__(self):
        self.verbose = True

    def process(self, data: Any) -> str:
        if self.verbose:
            print("Initializing Numeric Processor...")
            print(f"Processing data: {data}")

        if not self.validate(data):
            return "Invalid numeric data"

        try:
            total = sum(data)
            avg = total / len(data)
            return (f"Processed {len(data)} numeric",
                    f"values, sum={total}, avg={avg}")
        except Exception:
            return "Invalid numeric data"

    def validate(self, data: Any) -> bool:
        try:
            if not data or len(data) == 0:
                return False
            for item in data:
                _ = item + 0
            if self.verbose:
                print("Validation: Numeric data verified")
            return True
        except Exception:
            return False


class TextProcessor(DataProcessor):

    def __init__(self):
        self.verbose = True

    def process(self, data: Any) -> str:
        if self.verbose:
            print("Initializing Text Processor...")
            print(f'Processing data: "{data}"')

        if not self.validate(data):
            return "Invalid text data"

        try:
            char_count = len(data)
            word_count = len(data.split())
            return (f"Processed text: {char_count}",
                    f"characters, {word_count} words")
        except Exception:
            return "Invalid text data"

    def validate(self, data: Any) -> bool:
        try:
            if not isinstance(data, str) or len(data) == 0:
                return False
            _ = "" + data
            if self.verbose:
                print("Validation: Text data verified")
            return True
        except Exception:
            return False


class LogProcessor(DataProcessor):

    def __init__(self):
        self.verbose = True

    def process(self, data: Any) -> str:
        if self.verbose:
            print("Initializing Log Processor...")
            print(f'Processing data: "{data}"')

        if not self.validate(data):
            return "Invalid log data"

        try:
            if ":" in data:
                level, message = data.split(":", 1)
                level = level.strip()
                message = message.strip()

                if level == "ERROR":
                    return f"[ALERT] ERROR level detected: {message}"
                elif level == "INFO":
                    return f"[INFO] INFO level detected: {message}"
                elif level == "WARNING":
                    return f"[WARN] WARNING level detected: {message}"
                else:
                    return f"[LOG] {level} detected: {message}"
            else:
                return f"[LOG] Unknown detected: {data}"
        except Exception:
            return "Invalid log data"

    def validate(self, data: Any) -> bool:
        try:
            if not isinstance(data, str) or len(data) == 0:
                return False
            _ = "" + data
            if self.verbose:
                print("Validation: Log entry verified")
            return True
        except Exception:
            return False


if __name__ == "__main__":
    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===")

    num_proc = NumericProcessor()
    result1 = num_proc.process([1, 2, 3, 4, 5])
    print(num_proc.format_output(result1))

    print()

    text_proc = TextProcessor()
    result2 = text_proc.process("Hello Nexus World")
    print(text_proc.format_output(result2))

    print()

    log_proc = LogProcessor()
    result3 = log_proc.process("ERROR: Connection timeout")
    print(log_proc.format_output(result3))

    print("\n=== Polymorphic Processing Demo ===")
    print("Processing multiple data types through same interface...")

    processors = [NumericProcessor(), TextProcessor(), LogProcessor()]
    data_samples = [[1, 2, 3], "Hello Nexus", "INFO: System ready"]

    for processor in processors:
        processor.verbose = False

    print()

    for i in range(len(processors)):
        processor = processors[i]
        data = data_samples[i]
        result = processor.process(data)
        print(f"Result {i + 1}: {result}")

    print()

    print("Foundation systems online. Nexus ready for advanced streams.")
