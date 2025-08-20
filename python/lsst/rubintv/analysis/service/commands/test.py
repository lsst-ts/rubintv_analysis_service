from dataclasses import dataclass
from ..command import BaseCommand
from ..data import DataCenter


@dataclass(kw_only=True)
class TestErrorCommand(BaseCommand):
    """Test command that always returns an error for testing purposes."""

    error_type: str = "test_error"
    error_message: str = "This is a test error message"
    include_traceback: bool = True
    response_type: str = "error"

    def build_contents(self, _: DataCenter) -> dict:
        content = {
            "error": self.error_type,
            "description": self.error_message,
        }

        if self.include_traceback:
            content["traceback"] = (
                'Traceback (most recent call last):\\n  File "test.py", line 1, in <module>\\n    raise Exception("Test error")\\nException: Test error'
            )

        return content


TestErrorCommand.register("test error")
