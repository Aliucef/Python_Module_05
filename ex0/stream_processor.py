from abc import ABC, abstractmethod
from typing import Any


def ft_sum(data) -> int | float:
    total: int | float = 0
    for d in data:
        total += d
    return total


def ft_len(data) -> int:
    count: int = 0
    for i in data:
        count += 1
    return count


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

    def process(self, data: Any) -> str:
        print("Initializing Numeric Processor...")
        print(f"Processing data: {data}")

        try:
            if not self.validate(data):
                return "Invalid numeric data"
            total: int | float = ft_sum(data)
            avg: float = total / ft_len(data)
            return (f"Processed {ft_len(data)} numeric values,",
                    f" sum={total}, avg={avg}")

        except Exception:
            return "Invalid numeric data"

    def validate(self, data: Any) -> bool:
        try:
            count: int = 0
            for item in data:
                item + 0
                count += 1

            if count == 0:
                return False

            print("Validation: Numeric data verified")
            return True

        except Exception:
            return False


class TextProcessor(DataProcessor):

    def process(self, data: Any) -> str:
        print("Initializing Text Processor...")
        print(f"Processing data: \"{data}\"")

        try:
            if not self.validate(data):
                return "Invalid text data"

            words_count: int = 0
            letters_count: int = 0
            is_in_word: bool = False

            for letters in data:
                letters_count += 1
                if letters == " ":
                    if is_in_word:
                        is_in_word = False
                        words_count += 1
                else:
                    is_in_word = True
            if is_in_word:
                words_count += 1

            return (f"Processed text: {letters_count}",
                    f"characters, {words_count} words")

        except Exception:
            return "invalid Text data"

    def validate(self, data: Any) -> bool:
        try:
            count: int = 0
            for c in data:
                _: str = "" + c
                count += 1
            if count == 0:
                return False

            print("Validation: Text data verified")
            return True
        except Exception:
            return False


class LogProcessor(DataProcessor):

    def process(self, data: Any) -> str:
        print("Initializing Log Processor...")
        print(f'Processing data: "{data}"')

        try:
            if not self.validate(data):
                return "Invalid log data"

            prefix: str = ""
            message_started: bool = False
            for c in data:
                if c == ":":
                    message_started = True
                    break
                prefix += "" + c

            message: str = ""
            if message_started:
                after_colon: bool = False
                for c in data:
                    if not after_colon:
                        if c == ":":
                            after_colon = True
                        continue
                    message += "" + c

                msg: str = ""
                leading: bool = True
                for ch in message:
                    if leading and ch == " ":
                        continue
                    leading = False
                    msg += ch
                message = msg
            else:
                message = ""

            if prefix == "ERROR":
                return f"[ALERT] ERROR level detected: {message}"
            elif prefix == "INFO":
                return f"[INFO] INFO level detected: {message}"
            elif prefix == "WARNING":
                return f"[WARN] WARNING level detected: {message}"
            else:
                return f"[LOG] {prefix} detected: {message}"

        except Exception:
            return "Invalid log data"

    def validate(self, data: Any) -> bool:
        try:
            count: int = 0
            for c in data:
                _: str = "" + c
                count += 1
            if count == 0:
                return False

            print("Validation: Log entry verified")
            return True
        except Exception:
            return False


if __name__ == "__main__":
    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===")

    processors: list[DataProcessor] = [
        NumericProcessor(), TextProcessor(), LogProcessor()]
    data_samples: list[list[int] | str] = [
        [1, 2, 3], "Hello Nexus", "INFO: System ready"]

    print("=== Polymorphic Processing Demo ===")
    print("Processing multiple data types through same interface...")

    i: int = 0
    while i < ft_len(processors):
        processor: DataProcessor = processors[i]
        data: list[int] | str = data_samples[i]
        result: str = processor.process(data)
        print(f"Result {i + 1}: {result}\n")
        i += 1

    print("Foundation systems online. Nexus ready for advanced streams.")
