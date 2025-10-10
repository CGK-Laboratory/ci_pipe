import unittest

from ci_pipe.trace_builder import TraceBuilder
from external_dependencies.file_system.in_memory_file_system import InMemoryFileSystem


class CIPipeTestCase(unittest.TestCase):
    def setUp(self):
        self._file_system = InMemoryFileSystem()
        self._trace_builder = TraceBuilder(file_name="trace.json", file_system=self._file_system)

    # Pipeline step functions

    def add_one(self, inputs):
        prev = inputs('numbers').first()
        return {
            'numbers': [
                {'ids': list(prev.ids()), 'value': prev.value() + 1}
            ]
        }

    def add_one_with_different_key(self, inputs):
        prev = inputs('numbers').first()
        return {
            'another_numbers': [{'ids': list(prev.ids()), 'value': prev.value() + 1}]
        }

    def sum_all(self, inputs):
        a = inputs('numbers').first()
        b = inputs('another_numbers').first()
        return {
            'numbers_sum': [{
                'ids': list(a.ids() + b.ids()),
                'value': a.value() + b.value()
            }]
        }

    def sum_all_separately(self, inputs):
        a = inputs('numbers').first()
        b = inputs('another_numbers').first()
        return {
            'numbers_sum': [{'ids': list(a.ids()), 'value': a.value()}],
            'another_numbers_sum': [{'ids': list(b.ids()), 'value': b.value()}],
        }

    def multiply_by_two(self, inputs):
        r = inputs('numbers')
        return {'numbers': [
            {'ids': list(ids_), 'value': v * 2}
            for ids_, v in zip(r.ids(), r.values())
        ]}

    def scale(self, inputs, *, factor=1):
        records = inputs('numbers')._records
        return {
            'numbers': [
                {'ids': list(r.ids()), 'value': r.value() * factor}
                for r in records
            ]
        }
