import unittest

from ci_pipe.pipeline import CIPipe
from external_dependencies.file_system.in_memory_file_system import InMemoryFileSystem


class PipelineTestCase(unittest.TestCase):
    def test_01_a_pipeline_without_steps_returns_input_as_output(self):
        # Given
        pipeline_input = {'numbers': [0]}

        # When
        pipeline = CIPipe(pipeline_input, InMemoryFileSystem())

        # Then
        output = pipeline.output('numbers')
        self.assertEqual(len(output), 1)
        self.assertIn('ids', output[0])
        self.assertIn('value', output[0])
        self.assertEqual(output[0]['value'], 0)

    def test_02_a_pipeline_can_not_produce_output_for_non_existent_key(self):
        # Given
        pipeline_input = {'numbers': [0]}

        # When
        pipeline = CIPipe(pipeline_input, InMemoryFileSystem())

        # Then
        with self.assertRaises(KeyError):
            pipeline.output('non_existent_key')

    def test_03_a_pipeline_with_a_single_step_produces_step_output(self):
        # Given
        pipeline_input = {'numbers': [0]}

        # When
        pipeline = CIPipe(pipeline_input, InMemoryFileSystem())
        pipeline.step("Add one", self._add_one)

        # Then
        output = pipeline.output('numbers')
        self.assertEqual(len(output), 1)
        self.assertIn('ids', output[0])
        self.assertIn('value', output[0])
        self.assertEqual(output[0]['value'], 1)

    def test_04_a_pipeline_with_multiple_steps_produces_last_step_output(self):
        # Given
        pipeline_input = {'numbers': [0]}

        # When
        pipeline = CIPipe(pipeline_input, InMemoryFileSystem())
        pipeline.step("Add one", self._add_one)
        pipeline.step("Add another one", self._add_one)

        # Then
        output = pipeline.output('numbers')
        self.assertEqual(len(output), 1)
        self.assertIn('ids', output[0])
        self.assertIn('value', output[0])
        self.assertEqual(output[0]['value'], 2)

    def test_05_a_pipeline_with_multiple_steps_can_produce_intermediate_step_output(self):
        # Given
        pipeline_input = {'numbers': [0]}

        # When
        pipeline = CIPipe(pipeline_input, InMemoryFileSystem())
        pipeline.step("Add one", self._add_one)
        pipeline.step("Add one with different key", self._add_one_with_different_key)

        # Then
        output_numbers = pipeline.output('numbers')
        output_another = pipeline.output('another_numbers')
        self.assertEqual(len(output_numbers), 1)
        self.assertEqual(output_numbers[0]['value'], 1)
        self.assertEqual(len(output_another), 1)
        self.assertEqual(output_another[0]['value'], 2)

    def test_06_a_pipeline_can_have_steps_with_multiple_inputs(self):
        # Given
        pipeline_input = {'numbers': [0]}

        # When
        pipeline = CIPipe(pipeline_input, InMemoryFileSystem())
        pipeline.step("Add one", self._add_one)
        pipeline.step("Add one with different key", self._add_one_with_different_key)
        pipeline.step("Sum all", self._sum_all)

        # Then
        output = pipeline.output('numbers_sum')
        self.assertEqual(len(output), 1)
        self.assertEqual(output[0]['value'], 3)

    def test_07_a_pipeline_step_can_produce_multiple_outputs(self):
        # Given
        pipeline_input = {'numbers': [0]}

        # When
        pipeline = CIPipe(pipeline_input, InMemoryFileSystem())
        pipeline.step("Add one", self._add_one)
        pipeline.step("Add one with different key", self._add_one_with_different_key)
        pipeline.step("Sum all separately", self._sum_all_separately)

        # Then
        output_numbers_sum = pipeline.output('numbers_sum')
        output_another_sum = pipeline.output('another_numbers_sum')
        self.assertEqual(len(output_numbers_sum), 1)
        self.assertEqual(output_numbers_sum[0]['value'], 1)
        self.assertEqual(len(output_another_sum), 1)
        self.assertEqual(output_another_sum[0]['value'], 2)

    def test_08_can_forward_named_args_to_step(self):
        # Given
        pipeline_input = {'numbers': [1, 2, 3]}

        # When
        pipeline = CIPipe(pipeline_input, InMemoryFileSystem())
        pipeline.step("Scale by 2", self._scale, factor=2)

        # Then
        output = pipeline.output('numbers')
        self.assertEqual(len(output), 3)
        self.assertListEqual([x['value'] for x in output], [2, 4, 6])

    def test_09_can_define_default_named_args_for_steps(self):
        # Given
        pipeline_input = {'numbers': [1, 2, 3]}

        # When
        pipeline = CIPipe(pipeline_input, InMemoryFileSystem())
        pipeline.set_defaults(factor=3)
        pipeline.step("Scale by default factor", self._scale)

        # Then
        output = pipeline.output('numbers')
        self.assertEqual(len(output), 3)
        self.assertListEqual([x['value'] for x in output], [3, 6, 9])

    def test_10_step_named_args_override_pipeline_default_named_args(self):
        # Given
        pipeline_input = {'numbers': [1, 2, 3]}

        # When
        pipeline = CIPipe(pipeline_input, InMemoryFileSystem())
        pipeline.set_defaults(factor=3)
        pipeline.step("Scale by 2", self._scale, factor=2)

        # Then
        output = pipeline.output('numbers')
        self.assertEqual(len(output), 3)
        self.assertListEqual([x['value'] for x in output], [2, 4, 6])

    def test_11_when_no_default_is_set_and_no_arg_is_passed_use_function_default(self):
        # Given
        pipeline_input = {'numbers': [1, 2, 3]}

        # When
        pipeline = CIPipe(pipeline_input, InMemoryFileSystem())
        pipeline.step("Scale by function default factor", self._scale)

        # Then
        output = pipeline.output('numbers')
        self.assertEqual(len(output), 3)
        self.assertListEqual([x['value'] for x in output], [1, 2, 3])

    def test_12_pipeline_branch_starts_with_same_steps(self):
        # Given
        pipeline_input = {'numbers': [1]}

        # When
        pipeline = CIPipe(pipeline_input, InMemoryFileSystem())
        pipeline.step("Add one", self._add_one)
        new_pipeline_branch = pipeline.branch("Secondary Branch")
        new_pipeline_branch.step("Add one", self._add_one)

        # Then
        output_main = pipeline.output('numbers')
        output_branch = new_pipeline_branch.output('numbers')
        self.assertEqual(output_main[0]['value'], 2)
        self.assertEqual(output_branch[0]['value'], 3)

    def test_13_pipeline_branch_starts_with_same_defaults(self):
        # Given
        pipeline_input = {'numbers': [1]}

        # When
        pipeline = CIPipe(pipeline_input, InMemoryFileSystem())
        pipeline.set_defaults(factor=2)
        new_pipeline_branch = pipeline.branch("Secondary Branch")
        new_pipeline_branch.step("Scale by default factor", self._scale)

        # Then
        output = new_pipeline_branch.output('numbers')
        self.assertEqual(output[0]['value'], 2)

    def test_14_pipeline_new_step_does_not_affect_previous_branch(self):
        # Given
        pipeline_input = {'numbers': [1]}

        # When
        pipeline = CIPipe(pipeline_input, InMemoryFileSystem())
        pipeline.step("Add one", self._add_one)
        new_pipeline_branch = pipeline.branch("Secondary Branch")
        pipeline.step("Add one", self._add_one)

        # Then
        output_main = pipeline.output('numbers')
        output_branch = new_pipeline_branch.output('numbers')
        self.assertEqual(output_main[0]['value'], 3)
        self.assertEqual(output_branch[0]['value'], 2)

    def test_15_can_not_set_defaults_after_adding_steps(self):
        # Given
        pipeline_input = {'numbers': [1]}

        # When
        pipeline = CIPipe(pipeline_input, InMemoryFileSystem())
        pipeline.step("Add one", self._add_one)

        # Then
        with self.assertRaises(Exception):
            pipeline.set_defaults(factor=2)

    # Pipeline step functions
    def _add_one(self, inputs):
        return {'numbers': [{'ids': [inputs('numbers')[0]['ids'][0]], 'value': inputs('numbers')[0]['value'] + 1}]}

    def _add_one_with_different_key(self, inputs):
        return {'another_numbers': [{'ids': [inputs('numbers')[0]['ids'][0]], 'value': inputs('numbers')[0]['value'] + 1}]}

    def _sum_all(self, inputs):
        return {'numbers_sum': [{'ids': [inputs('numbers')[0]['ids'][0], inputs('another_numbers')[0]['ids'][0]], 'value': inputs('numbers')[0]['value'] + inputs('another_numbers')[0]['value']}]}
    
    def _sum_all_separately(self, inputs):
        return {
            'numbers_sum': [{'ids': [inputs('numbers')[0]['ids'][0]], 'value': inputs('numbers')[0]['value']}],
            'another_numbers_sum': [{'ids': [inputs('another_numbers')[0]['ids'][0]], 'value': inputs('another_numbers')[0]['value']}]
        }

    def _scale(self, inputs, *, factor=1):
        return {'numbers': [{'ids': x['ids'], 'value': x['value'] * factor} for x in inputs('numbers')]}


if __name__ == '__main__':
    unittest.main()
