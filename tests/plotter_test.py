import hashlib
import io
import re

from rich.console import Console

from ci_pipe.pipeline import CIPipe
from ci_pipe.plotter import Plotter
from ci_pipe.trace.trace_repository import TraceRepository
from tests.ci_pipe_test_case import CIPipeTestCase


class PlotterTestCase(CIPipeTestCase):
    def setUp(self):
        super().setUp()
        self._output = io.StringIO()
        self._console = Console(file=self._output, force_terminal=True)
        self._plotter = Plotter(console=self._console)

    def test_01_plotter_get_step_info_prints_step_from_modeled_trace(self):
        # Given
        pipeline_input = {'numbers': [0]}
        self._trace_builder.with_inputs({
            "numbers": [{
                "ids": [hashlib.sha256(("numbers" + str(0)).encode()).hexdigest()],
                "value": 0
            }]
        }).with_outputs_directory(self._expected_output_directory()
                                  ).with_empty_branch().with_steps_in_branch({
            "index": 1,
            "name": "Add one",
            "params": {},
            "outputs": {
                "numbers": [{
                    "ids": [hashlib.sha256(("numbers" + str(0)).encode()).hexdigest()],
                    "value": 1
                }]
            }
        })
        trace_repository = TraceRepository(self._file_system, "trace.json")

        pipeline = CIPipe(pipeline_input,
                          file_system=self._file_system,
                          outputs_directory='output',
                          trace_repository=trace_repository)
        pipeline.step("Add one", self.add_one)

        # When
        self._plotter.get_step_info(pipeline._trace, step_number=1, branch="Main Branch")


        # Then
        out = self._out()
        self.assertIn("Step 1 - Add one", out)
        self.assertIn('"numbers": [', out)
        self.assertIn("1", out)

    def test_02_plotter_prints_full_branch_flow(self):
        # Given
        pipeline_input = {'numbers': [0]}
        self._trace_builder.with_inputs({
            "numbers": [{
                "ids": [hashlib.sha256(("numbers" + str(0)).encode()).hexdigest()],
                "value": 0
            }]
        }).with_outputs_directory(self._expected_output_directory()
                                  ).with_empty_branch().with_steps_in_branch({
            "index": 1,
            "name": "Add one",
            "params": {},
            "outputs": {
                "numbers": [{
                    "ids": [hashlib.sha256(("numbers" + str(0)).encode()).hexdigest()],
                    "value": 1
                }]
            }
        }).with_steps_in_branch({
            "index": 2,
            "name": "Multiply by two",
            "params": {},
            "outputs": {
                "numbers": [{
                    "ids": [hashlib.sha256(("numbers" + str(0)).encode()).hexdigest()],
                    "value": 2
                }]
            }
        })
        trace_repository = TraceRepository(self._file_system, "trace.json")
        pipeline = CIPipe(pipeline_input, file_system=self._file_system, outputs_directory='output',
                          trace_repository=trace_repository)
        pipeline.step("Add one", self.add_one)
        pipeline.step("Multiply by two", self.multiply_by_two)

        # When
        self._plotter.get_all_trace_from_branch(pipeline._trace, "Main Branch")

        # Then
        out = self._out()
        self.assertIn("Pipeline Trace of branch: Main Branch", out)
        self.assertIn("1. Add one", out)
        self.assertIn("2. Multiply by two", out)

    def test_03_plotter_handles_missing_branch(self):
        # Given
        pipeline_input = {'numbers': [0]}
        self._trace_builder.with_inputs({
            "numbers": [{
                "ids": [hashlib.sha256(("numbers" + str(0)).encode()).hexdigest()],
                "value": 0
            }]
        }).with_outputs_directory(self._expected_output_directory())
        trace_repository = TraceRepository(self._file_system, "trace.json")
        pipeline = CIPipe(pipeline_input, file_system=self._file_system, outputs_directory='output',
                          trace_repository=trace_repository)

        # When
        self._plotter.get_all_trace_from_branch(pipeline._trace, "NonExistent")

        # Then
        out = self._out()
        self.assertIn("Branch 'NonExistent' not found", out)

    def _expected_output_directory(self) -> str:
        return "output"

    def _strip_ansi(self, s: str) -> str:
        return re.sub(r'\x1B\[[0-?]*[ -/]*[@-~]', '', s)

    def _out(self) -> str:
        self._output.seek(0)
        return self._strip_ansi(self._output.read())
