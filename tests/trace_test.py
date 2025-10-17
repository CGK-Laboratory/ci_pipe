import hashlib
import unittest

from ci_pipe.pipeline import CIPipe
from ci_pipe.schema_validator import SchemaValidator
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

        # When
        pipeline = CIPipe(pipeline_input, file_system=self._file_system, outputs_directory='output',
                          validator=validator)

        # Then
        self.assertTrue(pipeline.assert_trace_is_valid())

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
        pipeline = CIPipe(pipeline_input, file_system=self._file_system, outputs_directory='output',
                          validator=validator)

        # When
        pipeline.step("Add one", self.add_one)

        # Then
        self.assertTrue(pipeline.assert_trace_is_valid())

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

        # When
        pipeline = CIPipe(pipeline_input, file_system=self._file_system, outputs_directory='output',
                          validator=validator)
        pipeline.set_defaults(factor=2)
        pipeline.step("Scale by 2", self.scale)

        # Then
        self.assertTrue(pipeline.assert_trace_is_valid())

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

        # When
        pipeline = CIPipe(pipeline_input, file_system=self._file_system, outputs_directory='output',
                          validator=validator)
        pipeline.step("Add one", self.add_one)
        branched_pipeline = pipeline.branch("Secondary Branch")
        branched_pipeline.step("Add one", self.add_one)

        # Then
        self.assertTrue(pipeline.assert_trace_is_valid())

    def _expected_output_directory(self) -> str:
        return "output"


if __name__ == '__main__':
    unittest.main()
