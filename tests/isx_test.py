import unittest

from ci_pipe.pipeline import CIPipe
from external_dependencies.file_system.in_memory_file_system import InMemoryFileSystem
from external_dependencies.isx.in_memory_isx import InMemoryISX


class ISXTestCase(unittest.TestCase):
    def test_01_a_pipeline_without_isx_can_not_run_isx_step(self):
        # Given
        pipeline_input = {'videos': ['simulation1.isxd']}

        # When
        pipeline = CIPipe(pipeline_input, InMemoryFileSystem())

        # Then
        with self.assertRaises(RuntimeError):
            pipeline.isx.preprocess_videos()

    def test_02_a_pipeline_with_isx_can_run_isx_preprocess_videos(self):
        # Given
        file_system = InMemoryFileSystem()
        file_system.makedirs('input_dir')
        file_system.write('input_dir/file1.isxd', '')
        file_system.write('input_dir/file2.isxd', '')
        pipeline_input = 'input_dir'

        # When
        pipeline = CIPipe.with_videos_from_directory(pipeline_input, file_system=file_system, isx=InMemoryISX(file_system))
        pipeline.isx.preprocess_videos()

        # Then
        self._assert_output_files(
            pipeline,
            'videos-isxd',
            [
                'output/Main Branch - Step 1 - ISX Preprocess Videos/file1-PP.isxd',
                'output/Main Branch - Step 1 - ISX Preprocess Videos/file2-PP.isxd',
            ],
            file_system,
        )

    def test_03_a_pipeline_with_isx_can_run_isx_bandpass_filter_videos(self):
        # Given
        file_system = InMemoryFileSystem()
        file_system.makedirs('input_dir')
        file_system.write('input_dir/file1.isxd', '')
        file_system.write('input_dir/file2.isxd', '')
        pipeline_input = 'input_dir'

        # When
        pipeline = CIPipe.with_videos_from_directory(pipeline_input, file_system=file_system, isx=InMemoryISX(file_system))
        pipeline.isx.bandpass_filter_videos()

        # Then
        self._assert_output_files(
            pipeline,
            'videos-isxd',
            [
                'output/Main Branch - Step 1 - ISX Bandpass Filter Videos/file1-BP.isxd',
                'output/Main Branch - Step 1 - ISX Bandpass Filter Videos/file2-BP.isxd',
            ],
            file_system,
        )

    def test_04_a_pipeline_with_isx_can_run_isx_motion_correction_videos(self):
        # Given
        file_system = InMemoryFileSystem()
        file_system.makedirs('input_dir')
        file_system.write('input_dir/file1.isxd', '')
        file_system.write('input_dir/file2.isxd', '')
        pipeline_input = 'input_dir'

        # When
        pipeline = CIPipe.with_videos_from_directory(pipeline_input, file_system=file_system, isx=InMemoryISX(file_system))
        pipeline.isx.motion_correction_videos()

        # Then
        self._assert_output_files(
            pipeline,
            'videos-isxd',
            [
                'output/Main Branch - Step 1 - ISX Motion Correction Videos/file1-MC.isxd',
                'output/Main Branch - Step 1 - ISX Motion Correction Videos/file2-MC.isxd',
            ],
            file_system,
        )
        self._assert_output_files(
            pipeline,
            'motion-correction-translations',
            [
                'output/Main Branch - Step 1 - ISX Motion Correction Videos/file1-translations.csv',
                'output/Main Branch - Step 1 - ISX Motion Correction Videos/file2-translations.csv',
            ],
            file_system,
        )
        self._assert_output_files(
            pipeline,
            'motion-correction-crop-rect',
            [
                'output/Main Branch - Step 1 - ISX Motion Correction Videos/file1-series-crop-rect.csv',
                'output/Main Branch - Step 1 - ISX Motion Correction Videos/file2-series-crop-rect.csv',
            ],
            file_system,
        )
        self._assert_output_files(
            pipeline,
            'motion-correction-mean-images',
            [
                'output/Main Branch - Step 1 - ISX Motion Correction Videos/file1-series-mean-image.isxd',
                'output/Main Branch - Step 1 - ISX Motion Correction Videos/file2-series-mean-image.isxd',
            ],
            file_system,
        )

    def _assert_output_files(self, pipeline, key, expected_paths, file_system):
        output = pipeline.output(key)
        self.assertEqual(len(output), len(expected_paths))
        for i, expected in enumerate(expected_paths):
            self.assertEqual(output[i]['value'], expected)
            self.assertTrue(file_system.exists(expected))

if __name__ == '__main__':
    unittest.main()
