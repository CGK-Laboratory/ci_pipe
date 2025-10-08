import unittest
import json
import hashlib

from ci_pipe.pipeline import CIPipe
from ci_pipe.trace_builder import TraceBuilder
from external_dependencies.file_system.in_memory_file_system import InMemoryFileSystem


class TraceTestCase(unittest.TestCase):
    def test_01_a_pipeline_without_steps_generates_initial_trace(self):
        # Given
        pipeline_input = {'numbers': [0]}
        file_system = InMemoryFileSystem()

        # When
        trace_builder = TraceBuilder(file_name = "trace.json", file_system=file_system)
        CIPipe(pipeline_input, file_system=file_system, outputs_directory='output', trace_builder=trace_builder)

        # Then
        self.assertTrue(file_system.exists('trace.json'))
        trace_content = file_system.read('trace.json')

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
        file_system = InMemoryFileSystem()

        # When
        trace_builder = TraceBuilder(file_name="trace.json", file_system=file_system)
        pipeline = CIPipe(pipeline_input, file_system=file_system, outputs_directory='output', trace_builder=trace_builder)
        pipeline.step("Add one", self._add_one)

        # Then
        self.assertTrue(file_system.exists('trace.json'))
        trace_content = file_system.read('trace.json')

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
        file_system = InMemoryFileSystem()

        # When
        trace_builder = TraceBuilder(file_name="trace.json", file_system=file_system)
        pipeline = CIPipe(pipeline_input, file_system=file_system, outputs_directory='output', trace_builder=trace_builder)
        pipeline.set_defaults(factor=2)
        pipeline.step("Scale by 2", self._scale)

        # Then
        self.assertTrue(file_system.exists('trace.json'))
        trace_content = file_system.read('trace.json')

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
        file_system = InMemoryFileSystem()

        # When
        trace_builder = TraceBuilder(file_name="trace.json", file_system=file_system)
        pipeline = CIPipe(pipeline_input, file_system=file_system, outputs_directory='output', trace_builder=trace_builder)
        pipeline.step("Add one", self._add_one)
        branched_pipeline = pipeline.branch("Secondary Branch")
        branched_pipeline.step("Add one", self._add_one)

        # Then
        self.assertTrue(file_system.exists('trace.json'))
        trace_content = file_system.read('trace.json')

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


    # Pipeline step functions
    def _add_one(self, inputs):
        return {'numbers': [{'ids': [inputs('numbers')[0]['ids'][0]], 'value': inputs('numbers')[0]['value'] + 1}]}
    
    def _scale(self, inputs, *, factor=1):
        return {'numbers': [{'ids': x['ids'], 'value': x['value'] * factor} for x in inputs('numbers')]}



if __name__ == '__main__':
    unittest.main()
