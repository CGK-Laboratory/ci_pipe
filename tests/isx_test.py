import unittest

from ci_pipe.errors.isx_backend_not_configured_error import ISXBackendNotConfiguredError
from ci_pipe.pipeline import CIPipe
from external_dependencies.isx.in_memory_isx import InMemoryISX
from tests.ci_pipe_test_case import CIPipeTestCase


class ISXTestCase(CIPipeTestCase):
    def test_01_a_pipeline_without_isx_can_not_run_isx_step(self):
        # Given
        pipeline_input = {'videos': ['simulation1.isxd']}

        # When
        pipeline = CIPipe(pipeline_input, self._file_system)

        # Then
        with self.assertRaises(ISXBackendNotConfiguredError):
            pipeline.isx.preprocess_videos()

    def test_02_a_pipeline_with_isx_can_run_isx_preprocess_videos(self):
        # Given
        self._file_system.makedirs('input_dir')
        self._file_system.write('input_dir/file1.isxd', '')
        self._file_system.write('input_dir/file2.isxd', '')
        pipeline_input = 'input_dir'

        # When
        pipeline = CIPipe.with_videos_from_directory(pipeline_input, file_system=self._file_system,
                                                     isx=InMemoryISX(self._file_system))
        pipeline.isx.preprocess_videos()

        # Then
        result = pipeline.output_result('videos-isxd')
        self.assertTrue(result.has_size_of(2))
        self.assertEqual(result.first().value(), 'output/Main Branch - Step 1 - ISX Preprocess Videos/file1-PP.isxd')
        self.assertEqual(result.last().value(), 'output/Main Branch - Step 1 - ISX Preprocess Videos/file2-PP.isxd')
        self.assertTrue(self._file_system.exists('output/Main Branch - Step 1 - ISX Preprocess Videos/file1-PP.isxd'))
        self.assertTrue(self._file_system.exists('output/Main Branch - Step 1 - ISX Preprocess Videos/file2-PP.isxd'))


if __name__ == '__main__':
    unittest.main()
