import unittest

from ci_pipe.pipeline import CIPipe
from tests.ci_pipe_test_case import CIPipeTestCase
from external_dependencies.isx.in_memory_isx import InMemoryISX

class PipelineCleanUpTestCase(CIPipeTestCase):
    def test_01_clean_up_after_key_reutilization(self):
        # Given
        self._file_system.makedirs('input_dir')
        self._file_system.write('input_dir/file1.isxd', '')
        pipeline_input = 'input_dir'

        # When
        pipeline = CIPipe.with_videos_from_directory(
            pipeline_input,
            file_system=self._file_system,
            isx=InMemoryISX(self._file_system),
        )
        pipeline.isx.preprocess_videos()
        pipeline.isx.preprocess_videos()

        # Then
        self.assertFalse(self._file_system.exists('output/Main Branch - Step 1 - ISX Preprocess Videos/file1-PP.isxd'))
        self._assert_output_files(
            pipeline,
            'videos-isxd',
            [
                'output/Main Branch - Step 2 - ISX Preprocess Videos/file1-PP-PP.isxd',
            ],
            self._file_system,
        )
        self.assertTrue(self._file_system.exists('input_dir/file1.isxd'))

    def test02_clean_up_after_key_reutilization_in_multiple_files(self):
        # Given
        self._file_system.makedirs('input_dir')
        self._file_system.write('input_dir/file1.isxd', '')
        self._file_system.write('input_dir/file2.isxd', '')
        pipeline_input = 'input_dir'

        # When
        pipeline = CIPipe.with_videos_from_directory(
            pipeline_input,
            file_system=self._file_system,
            isx=InMemoryISX(self._file_system),
        )
        pipeline.isx.preprocess_videos()
        pipeline.isx.preprocess_videos()

        # Then
        self.assertFalse(self._file_system.exists('output/Main Branch - Step 1 - ISX Preprocess Videos/file1-PP.isxd'))
        self.assertFalse(self._file_system.exists('output/Main Branch - Step 1 - ISX Preprocess Videos/file2-PP.isxd'))
        self._assert_output_files(
            pipeline,
            'videos-isxd',
            [
                'output/Main Branch - Step 2 - ISX Preprocess Videos/file1-PP-PP.isxd',
                'output/Main Branch - Step 2 - ISX Preprocess Videos/file2-PP-PP.isxd',
            ],
            self._file_system,
        )
        self.assertTrue(self._file_system.exists('input_dir/file1.isxd'))
        self.assertTrue(self._file_system.exists('input_dir/file2.isxd'))

    def test03_clean_up_after_key_reutilization_with_different_keys(self):
        # This will need to use a situation like test_11_a_pipeline_with_isx_can_run_isx_longitudinal_registration, because we need to test with different keys being reutilized
        # Given
        self._file_system.makedirs('input_dir')
        self._file_system.write('input_dir/file1.isxd', '')
        pipeline_input = 'input_dir'

        # When
        pipeline = CIPipe.with_videos_from_directory(
            pipeline_input,
            file_system=self._file_system,
            isx=InMemoryISX(self._file_system),
        )
        pipeline.isx.preprocess_videos()
        pipeline.isx.extract_neurons_pca_ica()
        pipeline.isx.detect_events_in_cells()
        pipeline.isx.longitudinal_registration()

        # Then
        self.assertFalse(self._file_system.exists('output/Main Branch - Step 1 - ISX Preprocess Videos/file1-PP.isxd'))
        self.assertFalse(self._file_system.exists('output/Main Branch - Step 2 - ISX Extract Neurons PCA-ICA/file1-PP-PCA-ICA.isxd'))
        self._assert_output_files(
            pipeline,
            'events-isxd',
            [
                'output/Main Branch - Step 3 - ISX Detect Events In Cells/file1-PP-PCA-ICA-ED.isxd',
            ],
            self._file_system,
        )
        self._assert_output_files(
            pipeline,
            'videos-isxd',
            [
                'output/Main Branch - Step 4 - ISX Longitudinal Registration/file1-PP-LR.isxd',
            ],
            self._file_system,
        )
        self._assert_output_files(
            pipeline,
            'cellsets-isxd',
            [
                'output/Main Branch - Step 4 - ISX Longitudinal Registration/file1-PP-PCA-ICA-LR.isxd',
            ],
            self._file_system,
        )
        self.assertTrue(self._file_system.exists('input_dir/file1.isxd'))

    def test04_clean_up_can_be_disabled(self):
        # Given
        self._file_system.makedirs('input_dir')
        self._file_system.write('input_dir/file1.isxd', '')
        pipeline_input = 'input_dir'

        # When
        pipeline = CIPipe.with_videos_from_directory(
            pipeline_input,
            file_system=self._file_system,
            isx=InMemoryISX(self._file_system),
            auto_clean_up_enabled=False,
        )
        pipeline.isx.preprocess_videos()
        pipeline.isx.preprocess_videos()

        # Then
        self.assertTrue(self._file_system.exists('output/Main Branch - Step 1 - ISX Preprocess Videos/file1-PP.isxd'))
        self._assert_output_files(
            pipeline,
            'videos-isxd',
            [
                'output/Main Branch - Step 2 - ISX Preprocess Videos/file1-PP-PP.isxd',
            ],
            self._file_system,
        )
        self.assertTrue(self._file_system.exists('input_dir/file1.isxd'))

    def _assert_output_files(self, pipeline, key, expected_paths, file_system):
        output = pipeline.output(key)
        self.assertEqual(len(output), len(expected_paths))
        for i, expected in enumerate(expected_paths):
            self.assertEqual(output[i]['value'], expected)
            self.assertTrue(file_system.exists(expected))
        
        
if __name__ == '__main__':
    unittest.main()