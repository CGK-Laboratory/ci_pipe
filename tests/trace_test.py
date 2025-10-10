import hashlib
import json
import unittest

from ci_pipe.pipeline import CIPipe
from ci_pipe.trace_builder import TraceBuilder
from tests.ci_pipe_test_case import CIPipeTestCase


class TraceTestCase(CIPipeTestCase):
    def test_01_a_pipeline_without_steps_generates_initial_trace(self):
        # Given
        pipeline_input = {'numbers': [0]}

        # When
        CIPipe(pipeline_input, file_system=self._file_system, outputs_directory='output',
               trace_builder=self._trace_builder)

        # Then
        self.assertTrue(self._file_system.exists('trace.json'))
        trace_content = self._file_system.read('trace.json')

        trace_data = json.loads(trace_content)
        expected_trace = {
            "pipeline": {
                "inputs": {
                    "numbers": [
                        {
                            "ids": [hashlib.sha256(("numbers" + str(0)).encode()).hexdigest()],
                            "value": 0
                        }
                    ]
                },
                "defaults": {},
                "outputs_directory": "output"
            }
        }
        self.assertEqual(trace_data, expected_trace)

    def test_02_a_pipeline_with_steps_generates_steps_in_trace(self):
        # Given
        pipeline_input = {'numbers': [0]}

        # When
        trace_builder = TraceBuilder(file_name="trace.json", file_system=self._file_system)
        pipeline = CIPipe(pipeline_input, file_system=self._file_system, outputs_directory='output',
                          trace_builder=trace_builder)
        pipeline.step("Add one", self.add_one)

        # Then
        self.assertTrue(self._file_system.exists('trace.json'))
        trace_content = self._file_system.read('trace.json')

        trace_data = json.loads(trace_content)
        expected_trace = {
            "pipeline": {
                "inputs": {
                    "numbers": [
                        {
                            "ids": [hashlib.sha256(("numbers" + str(0)).encode()).hexdigest()],
                            "value": 0
                        }
                    ]
                },
                "defaults": {},
                "outputs_directory": "output"
            },
            "Main Branch": {
                "steps": [
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
                ]
            }
        }
        self.assertEqual(trace_data, expected_trace)

    def test_03_a_pipeline_with_defaults_generates_defaults_in_trace(self):
        # Given
        pipeline_input = {'numbers': [1]}

        # When
        pipeline = CIPipe(pipeline_input, file_system=self._file_system, outputs_directory='output',
                          trace_builder=self._trace_builder)
        pipeline.set_defaults(factor=2)
        pipeline.step("Scale by 2", self.scale)

        # Then
        self.assertTrue(self._file_system.exists('trace.json'))
        trace_content = self._file_system.read('trace.json')

        trace_data = json.loads(trace_content)
        expected_trace = {
            "pipeline": {
                "inputs": {
                    "numbers": [
                        {
                            "ids": [hashlib.sha256(("numbers" + str(1)).encode()).hexdigest()],
                            "value": 1
                        }
                    ]
                },
                "defaults": {
                    "factor": 2
                },
                "outputs_directory": "output"
            },
            "Main Branch": {
                "steps": [
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
                ]
            }
        }
        self.assertEqual(trace_data, expected_trace)

    def test_04_a_branched_pipeline_generates_branches_on_trace(self):
        # Given
        pipeline_input = {'numbers': [1]}

        # When
        pipeline = CIPipe(pipeline_input, file_system=self._file_system, outputs_directory='output',
                          trace_builder=self._trace_builder)
        pipeline.step("Add one", self.add_one)
        branched_pipeline = pipeline.branch("Secondary Branch")
        branched_pipeline.step("Add one", self.add_one)

        # Then
        self.assertTrue(self._file_system.exists('trace.json'))
        trace_content = self._file_system.read('trace.json')

        trace_data = json.loads(trace_content)
        expected_trace = {
            "pipeline": {
                "inputs": {
                    "numbers": [
                        {
                            "ids": [hashlib.sha256(("numbers" + str(1)).encode()).hexdigest()],
                            "value": 1
                        }
                    ]
                },
                "defaults": {},
                "outputs_directory": "output"
            },
            "Main Branch": {
                "steps": [
                    {
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
                    }
                ]
            },
            "Secondary Branch": {
                "steps": [
                    {
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
                    {
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
                    }
                ]
            }
        }
        self.assertEqual(trace_data, expected_trace)


if __name__ == '__main__':
    unittest.main()
