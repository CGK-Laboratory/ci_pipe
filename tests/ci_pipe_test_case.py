import unittest

from ci_pipe.trace.trace_repository import TraceRepository
from external_dependencies.file_system.in_memory_file_system import InMemoryFileSystem
from tests.ci_pipe_trace_builder import CIPipeTraceBuilder


class CIPipeTestCase(unittest.TestCase):
    def setUp(self):
        self._file_system = InMemoryFileSystem()
        self._trace_builder = CIPipeTraceBuilder()
        self._trace_repository = TraceRepository(self._file_system, "trace.json")

    # Pipeline step functions

    def add_one(self, inputs):
        return {'numbers': [{'ids': [inputs('numbers')[0]['ids'][0]], 'value': inputs('numbers')[0]['value'] + 1}]}

    def add_one_with_different_key(self, inputs):
        return {'another_numbers': [{'ids': [inputs('numbers')[0]['ids'][0]], 'value': inputs('numbers')[0]['value'] + 1}]}

    def multiply_by_two(self, inputs):
        return {'numbers': [{'ids': x['ids'], 'value': x['value'] * 2} for x in inputs('numbers')]}

    def sum_all(self, inputs):
        return {'numbers_sum': [{'ids': [inputs('numbers')[0]['ids'][0], inputs('another_numbers')[0]['ids'][0]], 'value': inputs('numbers')[0]['value'] + inputs('another_numbers')[0]['value']}]}

    def sum_all_separately(self, inputs):
        return {
            'numbers_sum': [{'ids': [inputs('numbers')[0]['ids'][0]], 'value': inputs('numbers')[0]['value']}],
            'another_numbers_sum': [{'ids': [inputs('another_numbers')[0]['ids'][0]], 'value': inputs('another_numbers')[0]['value']}]
        }

    def scale(self, inputs, *, factor=1):
        return {'numbers': [{'ids': x['ids'], 'value': x['value'] * factor} for x in inputs('numbers')]}
