import unittest

from ci_pipe.errors.defaults_after_step_error import DefaultsAfterStepsError
from ci_pipe.errors.output_key_not_found_error import OutputKeyNotFoundError
from ci_pipe.errors.resume_execution_error import ResumeExecutionError
from ci_pipe.pipeline import CIPipe
from ci_pipe.trace_builder import TraceBuilder
from tests.ci_pipe_test_case import CIPipeTestCase


class PipelineTestCase(CIPipeTestCase):
    def test_01_a_pipeline_without_steps_returns_input_as_output(self):
        # Given
        pipeline_input = {'numbers': [0]}

        # When
        pipeline = CIPipe(pipeline_input, file_system=self._file_system)

        # Then
        result = pipeline.output_result('numbers')
        input_value = pipeline_input['numbers'][0]
        self.assertTrue(result.has_size_of(1))
        self.assertEqual(result.first().value(), input_value)

    def test_02_a_pipeline_can_not_produce_output_for_non_existent_key(self):
        # Given
        pipeline_input = {'numbers': [0]}

        # When
        pipeline = CIPipe(pipeline_input, file_system=self._file_system)

        # Then
        with self.assertRaises(OutputKeyNotFoundError):
            pipeline.output('non_existent_key')

    def test_03_a_pipeline_with_a_single_step_produces_step_output(self):
        # Given
        pipeline_input = {'numbers': [0]}
        pipeline = CIPipe(pipeline_input, file_system=self._file_system)

        # When
        pipeline.step("Add one", self.add_one)

        # Then
        result = pipeline.output_result('numbers')
        self.assertTrue(result.has_size_of(1))
        self.assertEqual(result.first().value(), 1)

    def test_04_a_pipeline_with_multiple_steps_produces_last_step_output(self):
        # Given
        pipeline_input = {'numbers': [0]}
        pipeline = CIPipe(pipeline_input, file_system=self._file_system)
        pipeline.step("Add one", self.add_one)

        # When
        pipeline.step("Add another one", self.add_one)

        # Then
        result = pipeline.output_result('numbers')
        self.assertTrue(result.has_size_of(1))
        self.assertEqual(result.first().value(), 2)

    def test_05_a_pipeline_with_multiple_steps_can_produce_intermediate_step_output(self):
        # Given
        pipeline_input = {'numbers': [0]}
        pipeline = CIPipe(pipeline_input, file_system=self._file_system)
        pipeline.step("Add one", self.add_one)

        # When
        pipeline.step("Add one with different key", self.add_one_with_different_key)

        # Then
        numbers_result = pipeline.output_result('numbers')
        another_numbers_result = pipeline.output_result('another_numbers')
        self.assertTrue(numbers_result.has_size_of(1))
        self.assertEqual(numbers_result.first().value(), 1)
        self.assertTrue(another_numbers_result.has_size_of(1))
        self.assertEqual(another_numbers_result.first().value(), 2)

    def test_06_a_pipeline_can_have_steps_with_multiple_inputs(self):
        # Given
        pipeline_input = {'numbers': [0]}
        pipeline = CIPipe(pipeline_input, file_system=self._file_system)

        # When
        pipeline.step("Add one", self.add_one)
        pipeline.step("Add one with different key", self.add_one_with_different_key)
        pipeline.step("Sum all", self.sum_all)

        # Then
        result = pipeline.output_result('numbers_sum')
        self.assertTrue(result.has_size_of(1))
        self.assertEqual(result.first().value(), 3)

    def test_07_a_pipeline_step_can_produce_multiple_outputs(self):
        # Given
        pipeline_input = {'numbers': [0]}
        pipeline = CIPipe(pipeline_input, file_system=self._file_system)

        # When
        pipeline.step("Add one", self.add_one)
        pipeline.step("Add one with different key", self.add_one_with_different_key)
        pipeline.step("Sum all separately", self.sum_all_separately)

        # Then
        numbers_sum_result = pipeline.output_result('numbers_sum')
        another_numbers_sum_result = pipeline.output_result('another_numbers_sum')
        self.assertTrue(numbers_sum_result.has_size_of(1))
        self.assertEqual(numbers_sum_result.first().value(), 1)
        self.assertTrue(another_numbers_sum_result.has_size_of(1))
        self.assertEqual(another_numbers_sum_result.first().value(), 2)

    def test_08_can_forward_named_args_to_step(self):
        # Given
        pipeline_input = {'numbers': [1, 2, 3]}
        pipeline = CIPipe(pipeline_input, file_system=self._file_system)

        # When
        pipeline.step("Scale by 2", self.scale, factor=2)

        # Then
        result = pipeline.output_result('numbers')
        self.assertTrue(result.has_size_of(3))
        self.assertListEqual(result.values(), [2, 4, 6])

    def test_09_can_define_default_named_args_for_steps(self):
        # Given
        pipeline_input = {'numbers': [1, 2, 3]}
        pipeline = CIPipe(pipeline_input, file_system=self._file_system)

        # When
        pipeline.set_defaults(factor=3)
        pipeline.step("Scale by default factor", self.scale)

        # Then
        result = pipeline.output_result('numbers')
        self.assertTrue(result.has_size_of(3))
        self.assertListEqual(result.values(), [3, 6, 9])

    def test_10_step_named_args_override_pipeline_default_named_args(self):
        # Given
        pipeline_input = {'numbers': [1, 2, 3]}
        pipeline = CIPipe(pipeline_input, file_system=self._file_system)

        # When
        pipeline.set_defaults(factor=3)
        pipeline.step("Scale by 2", self.scale, factor=2)

        # Then
        result = pipeline.output_result('numbers')
        self.assertTrue(result.has_size_of(3))
        self.assertListEqual(result.values(), [2, 4, 6])

    def test_11_when_no_default_is_set_and_no_arg_is_passed_use_function_default(self):
        # Given
        pipeline_input = {'numbers': [1, 2, 3]}
        pipeline = CIPipe(pipeline_input, file_system=self._file_system)

        # When
        pipeline.step("Scale by function default factor", self.scale)

        # Then
        result = pipeline.output_result('numbers')
        self.assertTrue(result.has_size_of(3))
        self.assertListEqual(result.values(), [1, 2, 3])

    def test_12_pipeline_branch_starts_with_same_steps(self):
        # Given
        pipeline_input = {'numbers': [1]}
        pipeline = CIPipe(pipeline_input, file_system=self._file_system)

        # When
        pipeline.step("Add one", self.add_one)
        new_pipeline_branch = pipeline.branch("Secondary Branch")
        new_pipeline_branch.step("Add one", self.add_one)

        # Then
        result = pipeline.output_result('numbers')
        result_branch = new_pipeline_branch.output_result('numbers')
        self.assertTrue(result.has_size_of(1))
        self.assertTrue(result_branch.has_size_of(1))
        self.assertEqual(result.first().value(), 2)
        self.assertEqual(result_branch.first().value(), 3)

    def test_13_pipeline_branch_starts_with_same_defaults(self):
        # Given
        pipeline_input = {'numbers': [1]}
        pipeline = CIPipe(pipeline_input, file_system=self._file_system)

        # When
        pipeline.set_defaults(factor=2)
        new_pipeline_branch = pipeline.branch("Secondary Branch")
        new_pipeline_branch.step("Scale by default factor", self.scale)

        # Then
        result_branch = new_pipeline_branch.output_result('numbers')
        self.assertTrue(result_branch.has_size_of(1))
        self.assertEqual(result_branch.first().value(), 2)

    def test_14_pipeline_new_step_does_not_affect_previous_branch(self):
        # Given
        pipeline_input = {'numbers': [1]}
        pipeline = CIPipe(pipeline_input, file_system=self._file_system)

        # When
        pipeline.step("Add one", self.add_one)
        new_pipeline_branch = pipeline.branch("Secondary Branch")
        pipeline.step("Add one", self.add_one)

        # Then
        result = pipeline.output_result('numbers')
        result_branch = new_pipeline_branch.output_result('numbers')
        self.assertTrue(result.has_size_of(1))
        self.assertTrue(result_branch.has_size_of(1))
        self.assertEqual(result.first().value(), 3)
        self.assertEqual(result_branch.first().value(), 2)

    def test_15_can_not_set_defaults_after_adding_steps(self):
        # Given
        pipeline_input = {'numbers': [1]}
        pipeline = CIPipe(pipeline_input, file_system=self._file_system)
        pipeline.step("Add one", self.add_one)

        # When / Then
        with self.assertRaises(DefaultsAfterStepsError):
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
        isxd_result = pipeline.output_result('videos-isxd')
        tiff_result = pipeline.output_result('videos-tiff')
        self.assertTrue(isxd_result.has_size_of(1))
        self.assertEqual(isxd_result.first().value(), 'input_dir/file1.isxd')
        self.assertTrue(tiff_result.has_size_of(1))
        self.assertEqual(tiff_result.first().value(), 'input_dir/file2.tiff')

    def test_17_a_pipeline_does_not_execute_step_if_it_was_already_executed_before(self):
        # Given
        pipeline_input = {'numbers': [0]}
        pipeline = CIPipe(pipeline_input, file_system=self._file_system, outputs_directory='output',
                          trace_builder=self._trace_builder)
        pipeline.step("Add one", self.add_one)
        initial_trace_content = self._file_system.read('trace.json')

        # When
        pipeline.step("Add one", self.add_one)

        # Then
        final_trace_content = self._file_system.read('trace.json')
        result = pipeline.output_result('numbers')
        self.assertTrue(result.has_size_of(1))
        self.assertEqual(result.first().value(), 1)
        self.assertEqual(initial_trace_content, final_trace_content)

    def test_18_a_pipeline_cannot_resume_execution_from_new_pipeline_without_the_same_trace_file_and_output_directory(
            self):
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
        pipeline.step("Add one", self.add_one)
        new_output_directory = 'new_output'
        new_pipeline = CIPipe(
            pipeline_input,
            file_system=self._file_system,
            outputs_directory=new_output_directory,
            trace_builder=trace_builder)

        # When
        with self.assertRaises(ResumeExecutionError) as result:
            new_pipeline.step("Add one", self.add_one)

        # Then
        error_message = result.exception.args[0]
        self.assertEqual(error_message, CIPipe.RESUME_EXECUTION_ERROR_MESSAGE)

    def test_19_a_pipeline_can_resume_execution_from_new_pipeline_with_the_same_trace_file_and_output_directory(self):
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
        pipeline.step("Add one", self.add_one)
        initial_trace_content = self._file_system.read(trace_file_name)

        # When
        resume_pipeline = CIPipe(
            pipeline_input,
            file_system=self._file_system,
            outputs_directory=outputs_dir,
            trace_builder=trace_builder)

        resume_pipeline.step("Add one again", self.add_one)
        final_trace_content = self._file_system.read(trace_file_name)
        output_after_new_step = resume_pipeline.output('numbers')

        # Then
        result = resume_pipeline.output_result('numbers')
        self.assertTrue(result.has_size_of(1))
        self.assertTrue(result.first().value(), 2)
        self.assertNotEqual(initial_trace_content, final_trace_content)



if __name__ == '__main__':
    unittest.main()
