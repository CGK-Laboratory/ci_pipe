import hashlib
import unittest

from ci_pipe.pipeline import CIPipe
from ci_pipe.schema_validator import SchemaValidator
from ci_pipe.trace.trace_repository import TraceRepository
from tests.ci_pipe_test_case import CIPipeTestCase


class TraceTestCase(CIPipeTestCase):
    def test_01_a_pipeline_without_steps_generates_initial_trace(self):
        # Given
        pipeline_input = {'numbers': [0]}
        self._trace_builder.with_inputs(
            {
                "numbers": [
                    {
                        "ids": [hashlib.sha256(("numbers" + str(0)).encode()).hexdigest()],
                        "value": 0
                    }
                ]
            }
        ).with_outputs_directory(self._expected_output_directory())
        validator = SchemaValidator.new_for(self._trace_builder)
        trace_repository = TraceRepository(self._file_system, "trace.json", validator)

        # When
        CIPipe(pipeline_input, file_system=self._file_system, outputs_directory='output', trace_repository=trace_repository)

        # Then
        self._assert_trace_file_is_valid(validator, trace_repository)

    def test_02_a_pipeline_with_steps_generates_steps_in_trace(self):
        # Given
        pipeline_input = {'numbers': [0]}
        self._trace_builder.with_inputs(
            {
                "numbers": [
                    {
                        "ids": [hashlib.sha256(("numbers" + str(0)).encode()).hexdigest()],
                        "value": 0
                    }
                ]
            }
        ).with_outputs_directory(
            self._expected_output_directory()).with_empty_branch().with_steps_in_branch(
            {
                "index": 1,
                "name": "Add one",
                "params": {},
                "outputs": {
                    "numbers": [
                        {
                            "ids": [hashlib.sha256(("numbers" + str(0)).encode()).hexdigest()],
                            "value": 1
                        }
                    ]
                }
            }
        )
        validator = SchemaValidator.new_for(self._trace_builder)
        trace_repository = TraceRepository(self._file_system, "trace.json", validator)
        pipeline = CIPipe(pipeline_input, file_system=self._file_system, outputs_directory='output',
                          trace_repository=trace_repository)

        # When
        pipeline.step("Add one", self.add_one)

        # Then
        self._assert_trace_file_is_valid(validator, trace_repository)

    def test_03_a_pipeline_with_defaults_generates_defaults_in_trace(self):
        # Given
        pipeline_input = {'numbers': [1]}
        self._trace_builder.with_inputs(
            {
                "numbers": [
                    {
                        "ids": [hashlib.sha256(("numbers" + str(1)).encode()).hexdigest()],
                        "value": 1
                    }
                ]
            }
        ).with_defaults(
            {
                "factor": 2
            },
        ).with_outputs_directory(self._expected_output_directory()).with_empty_branch().with_steps_in_branch(
            {
                "index": 1,
                "name": "Scale by 2",
                "params": {
                    "factor": 2
                },
                "outputs": {
                    "numbers": [
                        {
                            "ids": [hashlib.sha256(("numbers" + str(1)).encode()).hexdigest()],
                            "value": 2
                        }
                    ]
                }
            }
        )
        validator = SchemaValidator.new_for(self._trace_builder)
        trace_repository = TraceRepository(self._file_system, "trace.json", validator)

        # When
        pipeline = CIPipe(pipeline_input, file_system=self._file_system, outputs_directory='output',
                          trace_repository=trace_repository)
        pipeline.set_defaults(factor=2)
        pipeline.step("Scale by 2", self.scale)

        # Then
        self._assert_trace_file_is_valid(validator, trace_repository)

    def test_04_a_branched_pipeline_generates_branches_on_trace(self):
        # Given
        pipeline_input = {'numbers': [1]}
        self._trace_builder.with_inputs(
            {
                "numbers": [
                    {
                        "ids": [hashlib.sha256(("numbers" + str(1)).encode()).hexdigest()],
                        "value": 1
                    }
                ]
            }
        ).with_outputs_directory(self._expected_output_directory()).with_empty_branch(
            branch_name="Main Branch").with_empty_branch(branch_name="Secondary Branch").with_steps_in_branch(
            steps_json={
                "index": 1,
                "name": "Add one",
                "params": {},
                "outputs": {
                    "numbers": [
                        {
                            "ids": [hashlib.sha256(("numbers" + str(1)).encode()).hexdigest()],
                            "value": 2
                        }
                    ]
                }
            },
            branch_name="Main Branch"
        ).with_steps_in_branch(
            steps_json={
                "index": 1,
                "name": "Add one",
                "params": {},
                "outputs": {
                    "numbers": [
                        {
                            "ids": [hashlib.sha256(("numbers" + str(1)).encode()).hexdigest()],
                            "value": 2
                        }
                    ]
                }
            },
            branch_name="Secondary Branch"
        ).with_steps_in_branch(
            steps_json={
                "index": 2,
                "name": "Add one",
                "params": {},
                "outputs": {
                    "numbers": [
                        {
                            "ids": [hashlib.sha256(("numbers" + str(1)).encode()).hexdigest()],
                            "value": 3
                        }
                    ]
                }
            },
            branch_name="Secondary Branch"
        )
        validator = SchemaValidator.new_for(self._trace_builder)
        trace_repository = TraceRepository(self._file_system, "trace.json", validator)

        # When
        pipeline = CIPipe(pipeline_input, file_system=self._file_system, outputs_directory='output',
                          trace_repository=trace_repository)
        pipeline.step("Add one", self.add_one)
        branched_pipeline = pipeline.branch("Secondary Branch")
        branched_pipeline.step("Add one", self.add_one)

        # Then
        self._assert_trace_file_is_valid(validator, trace_repository)

    def _expected_output_directory(self) -> str:
        return "output"

    def _assert_trace_file_is_valid(self, validator, trace_repository):
        self.assertTrue(trace_repository.exists())
        json_data = trace_repository.load().to_dict()
        self.assertTrue(validator.validate(json_data))


if __name__ == '__main__':
    unittest.main()
