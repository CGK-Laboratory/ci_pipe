import os
import tempfile
import unittest

from ci_pipe.pipeline import CIPipe
from ci_pipe.trace_builder import TraceBuilder
from external_dependencies.file_system.in_memory_file_system import InMemoryFileSystem


class PipelineTestCase(unittest.TestCase):
    def setUp(self):
        self._file_system = InMemoryFileSystem()
        self._trace_builder = TraceBuilder(file_name="trace.json", file_system=self._file_system)

    def test_01_a_pipeline_without_steps_returns_input_as_output(self):
        # Given
        pipeline_input = {'numbers': [0]}

        # When
        pipeline = CIPipe(pipeline_input, file_system=self._file_system)

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
        pipeline = CIPipe(pipeline_input, file_system=self._file_system)

        # Then
        with self.assertRaises(KeyError):
            pipeline.output('non_existent_key')

    def test_03_a_pipeline_with_a_single_step_produces_step_output(self):
        # Given
        pipeline_input = {'numbers': [0]}

        # When
        pipeline = CIPipe(pipeline_input, file_system=self._file_system)
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
        pipeline = CIPipe(pipeline_input, file_system=self._file_system)
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
        pipeline = CIPipe(pipeline_input, file_system=self._file_system)
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
        pipeline = CIPipe(pipeline_input, file_system=self._file_system)
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
        pipeline = CIPipe(pipeline_input, file_system=self._file_system)
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
        pipeline = CIPipe(pipeline_input, file_system=self._file_system)
        pipeline.step("Scale by 2", self._scale, factor=2)

        # Then
        output = pipeline.output('numbers')
        self.assertEqual(len(output), 3)
        self.assertListEqual([x['value'] for x in output], [2, 4, 6])

    def test_09_can_define_default_named_args_for_steps(self):
        # Given
        pipeline_input = {'numbers': [1, 2, 3]}

        # When
        pipeline = CIPipe(pipeline_input, file_system=self._file_system)
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
        pipeline = CIPipe(pipeline_input, file_system=self._file_system)
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
        pipeline = CIPipe(pipeline_input, file_system=self._file_system)
        pipeline.step("Scale by function default factor", self._scale)

        # Then
        output = pipeline.output('numbers')
        self.assertEqual(len(output), 3)
        self.assertListEqual([x['value'] for x in output], [1, 2, 3])

    def test_12_pipeline_branch_starts_with_same_steps(self):
        # Given
        pipeline_input = {'numbers': [1]}

        # When
        pipeline = CIPipe(pipeline_input, file_system=self._file_system)
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
        pipeline = CIPipe(pipeline_input, file_system=self._file_system)
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
        pipeline = CIPipe(pipeline_input, file_system=self._file_system)
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
        pipeline = CIPipe(pipeline_input, file_system=self._file_system)
        pipeline.step("Add one", self._add_one)

        # Then
        with self.assertRaises(RuntimeError):
            pipeline.set_defaults(factor=2)

    def test_16_a_pipeline_can_build_input_videos_from_directory(self):
        # Given
        self._file_system.makedirs('input_dir')
        self._file_system.write('input_dir/file1.isxd', '')
        self._file_system.write('input_dir/file2.tiff', '')
        pipeline_input = 'input_dir'

        # When
        pipeline = CIPipe.with_videos_from_directory(pipeline_input, file_system=self._file_system)

        # Then
        output_isxd = pipeline.output('videos-isxd')
        self.assertEqual(len(output_isxd), 1)
        self.assertIn('ids', output_isxd[0])
        self.assertIn('value', output_isxd[0])
        self.assertEqual(output_isxd[0]['value'], 'input_dir/file1.isxd')
        output_tiff = pipeline.output('videos-tiff')
        self.assertEqual(len(output_tiff), 1)
        self.assertIn('ids', output_tiff[0])
        self.assertIn('value', output_tiff[0])
        self.assertEqual(output_tiff[0]['value'], 'input_dir/file2.tiff')

    def test_17_set_defaults_from_file(self):
        # Given
        fs = InMemoryFileSystem()
        pipeline_input = {'numbers': [{'ids': ['1'], 'value': 1}]}
        pipeline = CIPipe(pipeline_input, file_system=fs)

        config_path = "config.yaml"
        fs.write(config_path, "factor: 5\nmultiplier: 2")

        # When
        pipeline.set_defaults(defaults_path=config_path)

        # Then
        self.assertEqual(pipeline._defaults.get('factor'), 5)
        self.assertEqual(pipeline._defaults.get('multiplier'), 2)

    def test_18_set_defaults_from_kwargs_only(self):
        # Given
        fs = InMemoryFileSystem()
        pipeline_input = {'numbers': [{'ids': ['1'], 'value': 1}]}
        pipeline = CIPipe(pipeline_input, file_system=fs)

        # When
        pipeline.set_defaults(factor=3, multiplier=4)

        # Then
        self.assertEqual(pipeline._defaults.get('factor'), 3)
        self.assertEqual(pipeline._defaults.get('multiplier'), 4)

    def test_19_set_defaults_combined_file_and_kwargs(self):
        # Given
        fs = InMemoryFileSystem()
        pipeline_input = {'numbers': [{'ids': ['1'], 'value': 1}]}
        pipeline = CIPipe(pipeline_input, file_system=fs)

        config_path = "config.yaml"
        fs.write(config_path, "factor: 5\nmultiplier: 2")

        # When
        pipeline.set_defaults(factor=10, defaults_path=config_path)

        # Then
        self.assertEqual(pipeline._defaults.get('factor'), 10)
        self.assertEqual(pipeline._defaults.get('multiplier'), 2)

    def test_22_pass_defaults_directly_in_pipeline_creation_kwargs(self):
        # Given
        fs = InMemoryFileSystem()
        pipeline_input = {'numbers': [{'ids': ['1'], 'value': 1}]}

        # When
        pipeline = CIPipe(pipeline_input, file_system=fs, defaults={'factor': 3, 'multiplier': 4})

        # Then
        self.assertEqual(pipeline._defaults.get('factor'), 3)
        self.assertEqual(pipeline._defaults.get('multiplier'), 4)

    def test_23_pass_defaults_from_file_directly_in_pipeline_creation(self):
        # Given
        fs = InMemoryFileSystem()
        pipeline_input = {'numbers': [{'ids': ['1'], 'value': 1}]}

        config_path = "config.yaml"
        fs.write(config_path, "factor: 5\nmultiplier: 2")

        # When
        pipeline = CIPipe(pipeline_input, file_system=fs, defaults_path=config_path)

        # Then
        self.assertEqual(pipeline._defaults.get('factor'), 5)
        self.assertEqual(pipeline._defaults.get('multiplier'), 2)

    def test_24_pass_combined_defaults_directly_in_pipeline_creation(self):
        # Given
        fs = InMemoryFileSystem()
        pipeline_input = {'numbers': [{'ids': ['1'], 'value': 1}]}

        config_path = "config.yaml"
        fs.write(config_path, "multiplier: 2")

        # When
        pipeline = CIPipe(pipeline_input, file_system=fs, defaults={'factor': 10},defaults_path=config_path)

        # Then
        self.assertEqual(pipeline._defaults.get('factor'), 10)
        self.assertEqual(pipeline._defaults.get('multiplier'), 2)

            
    def test_25_a_pipeline_does_not_execute_step_if_it_was_already_executed_before(self):
        # Given
        pipeline_input = {'numbers': [0]}
        pipeline = CIPipe(pipeline_input, file_system=self._file_system, outputs_directory='output', trace_builder=self._trace_builder)
        pipeline.step("Add one", self._add_one)
        initial_trace_content = self._file_system.read('trace.json')

        # When
        pipeline.step("Add one", self._add_one)

        # Then
        final_trace_content = self._file_system.read('trace.json')
        output = pipeline.output('numbers')
        self.assertEqual(len(output), 1)
        self.assertIn('ids', output[0])
        self.assertIn('value', output[0])
        self.assertEqual(output[0]['value'], 1)
        self.assertEqual(initial_trace_content, final_trace_content)

    def test_26_a_pipeline_cannot_resume_execution_from_new_pipeline_without_the_same_trace_file_and_output_directory(self):
        # Given
        pipeline_input = {'numbers': [0]}
        initial_output_directory = 'output'
        trace_file_name = 'trace.json'
        trace_builder = TraceBuilder(file_name=trace_file_name, file_system=self._file_system)
        pipeline = CIPipe(
            pipeline_input,
            file_system=self._file_system,
            outputs_directory=initial_output_directory,
            trace_builder=trace_builder)
        pipeline.step("Add one", self._add_one)
        new_output_directory = 'new_output'
        new_pipeline = CIPipe(
            pipeline_input,
            file_system=self._file_system,
            outputs_directory=new_output_directory,
            trace_builder=trace_builder)

        # When
        with self.assertRaises(ValueError) as result:
            new_pipeline.step("Add one", self._add_one)

        # Then
        error_message = result.exception.args[0]
        self.assertEqual(error_message, CIPipe.RESUME_EXECUTION_ERROR_MESSAGE)


    def test_27_a_pipeline_can_resume_execution_from_new_pipeline_with_the_same_trace_file_and_output_directory(self):
        # Given
        pipeline_input = {'numbers': [0]}
        outputs_dir = 'output'
        trace_file_name = 'trace.json'
        trace_builder = TraceBuilder(file_name=trace_file_name, file_system=self._file_system)
        pipeline = CIPipe(
            pipeline_input,
            file_system=self._file_system,
            outputs_directory=outputs_dir,
            trace_builder=trace_builder)
        pipeline.step("Add one", self._add_one)
        initial_trace_content = self._file_system.read(trace_file_name)

        # When
        resume_pipeline = CIPipe(
            pipeline_input,
            file_system=self._file_system,
            outputs_directory=outputs_dir,
            trace_builder=trace_builder)

        resume_pipeline.step("Add one again", self._add_one)
        final_trace_content = self._file_system.read(trace_file_name)
        output_after_new_step = resume_pipeline.output('numbers')

        # Then
        self.assertEqual(output_after_new_step[0]['value'], 2)
        self.assertNotEqual(initial_trace_content, final_trace_content)


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
