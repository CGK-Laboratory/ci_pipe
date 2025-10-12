import io
import re
import unittest

from rich.console import Console

from ci_pipe.pipeline import CIPipe
from ci_pipe.plotter import Plotter
from tests.ci_pipe_test_case import CIPipeTestCase


def strip_ansi(text: str) -> str:
    """Removes ANSI codes (colors, styles) from a string."""
    ansi_escape = re.compile(r'\x1B\[[0-?]*[ -/]*[@-~]')
    return ansi_escape.sub('', text)


class TestPlotter(CIPipeTestCase):

    def get_output(self, console_output):
        """Gets console output without ANSI codes."""
        console_output.seek(0)
        return strip_ansi(console_output.read())

    def setUp(self):
        # Given
        self.output = io.StringIO()
        self.console = Console(file=self.output, force_terminal=True)
        self.plotter = Plotter(console=self.console)
        self.trace = {
            "Main Branch": {
                "steps": [
                    {"index": 1, "name": "Add one", "params": {"value": 1}, "outputs": {"numbers": [2]}},
                    {"index": 2, "name": "Multiply by two", "params": {"factor": 2}, "outputs": {"numbers": [4]}},
                ]
            }
        }

    def test_01_get_step_info_existing_step(self):
        # When
        self.plotter.get_step_info(self.trace, step_number=1, branch="Main Branch")
        # Then
        out = self.get_clean_output()
        self.assertIn("Step 1 - Add one", out)
        self.assertIn("value: 1", out)
        self.assertIn('"numbers": [', out)
        self.assertIn("2", out)

    def test_02_get_step_info_nonexistent_step(self):
        # When
        self.plotter.get_step_info(self.trace, step_number=99, branch="Main Branch")
        # Then
        out = self.get_clean_output()
        self.assertIn("Step 99 not found in branch 'Main Branch'", out)

    def test_03_get_all_trace_from_branch_existing_branch(self):
        # When
        self.plotter.get_all_trace_from_branch(self.trace, "Main Branch")
        # Then
        out = self.get_clean_output()
        self.assertIn("Pipeline Trace of branch: Main Branch", out)
        self.assertIn("1. Add one", out)
        self.assertIn("2. Multiply by two", out)

    def test_04_get_all_trace_from_branch_nonexistent_branch(self):
        # When
        self.plotter.get_all_trace_from_branch(self.trace, "NonExistent")
        # Then
        out = self.get_clean_output()
        self.assertIn("Branch 'NonExistent' not found", out)

    def test_05_info_displays_step_with_tracebuilder(self):
        # Given
        console = Console(file=self.output, force_terminal=True)
        plotter = Plotter(console=console)
        pipeline_input = {'numbers': [0]}

        # When
        pipeline = CIPipe(
            inputs=pipeline_input,
            file_system=self._file_system,
            plotter=plotter,
            trace_repository=self._trace_repository
        )
        pipeline.step("Add one", self.add_one)
        pipeline.info(1)

        # Then
        out = self.get_output(self.output)
        assert "Step 1 - Add one" in out
        assert "Value" in out
        assert "Add one" in out
        assert '"numbers": [' in out
        assert "1" in out

    def test_06_trace_displays_branch_and_steps_with_tracebuilder(self):
        # Given
        plotter = Plotter(console=self.console)
        pipeline_input = {'numbers': [0]}

        # When
        pipeline = CIPipe(inputs=pipeline_input, file_system=self._file_system, plotter=plotter, trace_repository=self._trace_repository)
        pipeline.step("Add one", self.add_one)
        pipeline.step("Multiply by two", self.multiply_by_two)
        pipeline.trace()

        # Then
        out = self.get_output(self.output)
        assert "Pipeline Trace of branch: Main Branch" in out
        assert "1. Add one" in out
        assert "2. Multiply by two" in out

    def get_clean_output(self):
        """Gets the console output without ANSI codes."""
        self.output.seek(0)
        raw = self.output.read()
        return strip_ansi(raw)

if __name__ == "__main__":
    unittest.main()
