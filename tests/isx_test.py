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
        pipeline = CIPipe(pipeline_input, file_system=self._file_system)

        # Then
        with self.assertRaises(ISXBackendNotConfiguredError):
            pipeline.isx.preprocess_videos()

    def test_02_a_pipeline_with_isx_can_run_isx_preprocess_videos(self):
        # Given
        self._initialize_directory_with_two_videos()
        pipeline_input = 'input_dir'

        # When
        pipeline = CIPipe.with_videos_from_directory(
            pipeline_input,
            file_system=self._file_system,
            isx=InMemoryISX(self._file_system),
        )
        pipeline.isx.preprocess_videos()

        # Then
        self._assert_output_files(
            pipeline,
            'videos-isxd',
            [
                'output/Main Branch - Step 1 - ISX Preprocess Videos/file1-PP.isxd',
                'output/Main Branch - Step 1 - ISX Preprocess Videos/file2-PP.isxd',
            ],
            self._file_system,
        )

    def test_03_a_pipeline_with_isx_can_run_isx_bandpass_filter_videos(self):
        # Given
        self._initialize_directory_with_two_videos()
        pipeline_input = 'input_dir'

        # When
        pipeline = CIPipe.with_videos_from_directory(
            pipeline_input,
            file_system=self._file_system,
            isx=InMemoryISX(self._file_system),
        )

        pipeline.isx.bandpass_filter_videos()

        # Then
        self._assert_output_files(
            pipeline,
            'videos-isxd',
            [
                'output/Main Branch - Step 1 - ISX Bandpass Filter Videos/file1-BP.isxd',
                'output/Main Branch - Step 1 - ISX Bandpass Filter Videos/file2-BP.isxd',
            ],
            self._file_system,
        )

    def test_04_a_pipeline_with_isx_can_run_isx_motion_correction_videos(self):
        # Given
        self._initialize_directory_with_two_videos()
        pipeline_input = 'input_dir'

        # When
        pipeline = CIPipe.with_videos_from_directory(
            pipeline_input,
            file_system=self._file_system,
            isx=InMemoryISX(self._file_system),
        )
        pipeline.isx.motion_correction_videos()

        # Then
        self._assert_output_files(
            pipeline,
            'videos-isxd',
            [
                'output/Main Branch - Step 1 - ISX Motion Correction Videos/file1-MC.isxd',
                'output/Main Branch - Step 1 - ISX Motion Correction Videos/file2-MC.isxd',
            ],
            self._file_system,
        )
        self._assert_output_files(
            pipeline,
            'motion-correction-translations',
            [
                'output/Main Branch - Step 1 - ISX Motion Correction Videos/file1-translations.csv',
                'output/Main Branch - Step 1 - ISX Motion Correction Videos/file2-translations.csv',
            ],
            self._file_system,
        )
        self._assert_output_files(
            pipeline,
            'motion-correction-crop-rect',
            [
                'output/Main Branch - Step 1 - ISX Motion Correction Videos/file1-series-crop-rect.csv',
                'output/Main Branch - Step 1 - ISX Motion Correction Videos/file2-series-crop-rect.csv',
            ],
            self._file_system,
        )
        self._assert_output_files(
            pipeline,
            'motion-correction-mean-images',
            [
                'output/Main Branch - Step 1 - ISX Motion Correction Videos/file1-series-mean-image.isxd',
                'output/Main Branch - Step 1 - ISX Motion Correction Videos/file2-series-mean-image.isxd',
            ],
            self._file_system,
        )

    def test_05_a_pipeline_with_isx_can_run_isx_normalize_dff_videos(self):
        # Given
        self._initialize_directory_with_two_videos()
        pipeline_input = 'input_dir'

        # When
        pipeline = CIPipe.with_videos_from_directory(
            pipeline_input,
            file_system=self._file_system,
            isx=InMemoryISX(self._file_system),
        )
        pipeline.isx.normalize_dff_videos()

        # Then
        self._assert_output_files(
            pipeline,
            'videos-isxd',
            [
                'output/Main Branch - Step 1 - ISX Normalize DFF Videos/file1-DFF.isxd',
                'output/Main Branch - Step 1 - ISX Normalize DFF Videos/file2-DFF.isxd',
            ],
            self._file_system,
        )

    def test_06_a_pipeline_with_isx_can_run_isx_extract_neurons_pca_ica(self):
        # Given
        self._initialize_directory_with_two_videos()
        pipeline_input = 'input_dir'

        # When
        pipeline = CIPipe.with_videos_from_directory(
            pipeline_input,
            file_system=self._file_system,
            isx=InMemoryISX(self._file_system),
        )
        pipeline.isx.extract_neurons_pca_ica()

        # Then
        self._assert_output_files(
            pipeline,
            'cellsets-isxd',
            [
                'output/Main Branch - Step 1 - ISX Extract Neurons PCA ICA/file1-PCA-ICA.isxd',
                'output/Main Branch - Step 1 - ISX Extract Neurons PCA ICA/file2-PCA-ICA.isxd',
            ],
            self._file_system,
        )

    def test_07_a_pipeline_with_isx_can_run_isx_detect_events_in_cells(self):
        # Given
        self._initialize_directory_with_two_videos()
        pipeline_input = 'input_dir'

        # When
        pipeline = CIPipe.with_videos_from_directory(
            pipeline_input,
            file_system=self._file_system,
            isx=InMemoryISX(self._file_system),
        )
        pipeline.isx.extract_neurons_pca_ica()
        pipeline.isx.detect_events_in_cells()

        # Then
        self._assert_output_files(
            pipeline,
            'events-isxd',
            [
                'output/Main Branch - Step 2 - ISX Detect Events In Cells/file1-PCA-ICA-ED.isxd',
                'output/Main Branch - Step 2 - ISX Detect Events In Cells/file2-PCA-ICA-ED.isxd',
            ],
            self._file_system,
        )

    def test_08_a_pipeline_with_isx_can_run_isx_auto_accept_reject_cells(self):
        # Given
        self._initialize_directory_with_two_videos()
        pipeline_input = 'input_dir'

        # When
        pipeline = CIPipe.with_videos_from_directory(
            pipeline_input,
            file_system=self._file_system,
            isx=InMemoryISX(self._file_system),
        )
        pipeline.isx.extract_neurons_pca_ica()
        pipeline.isx.detect_events_in_cells()
        pipeline.isx.auto_accept_reject_cells()

        # Then
        self._assert_output_files(
            pipeline,
            'cellsets-isxd',
            [
                'output/Main Branch - Step 3 - ISX Auto Accept Reject Cells/file1-PCA-ICA.isxd',
                'output/Main Branch - Step 3 - ISX Auto Accept Reject Cells/file2-PCA-ICA.isxd',
            ],
            self._file_system,
        )


    def test_09_a_pipeline_with_isx_can_export_last_videos_to_tiff(self):
        # Given
        self._file_system.makedirs('input_dir')
        self._file_system.write('input_dir/file1.isxd', '')
        self._file_system.write('input_dir/file2.isxd', '')
        pipeline_input = 'input_dir'

        # When
        pipeline = CIPipe.with_videos_from_directory(pipeline_input, file_system=self._file_system, isx=InMemoryISX(self._file_system))
        pipeline.isx.preprocess_videos()
        pipeline.isx.export_movie_to_tiff()

        # Then
        self._assert_output_files(
            pipeline,
            'videos-tiff',
            [
                'output/Main Branch - Step 2 - ISX Export movie to TIFF/file1-PP.tiff',
                'output/Main Branch - Step 2 - ISX Export movie to TIFF/file2-PP.tiff',
            ],
            self._file_system,
        )

    def test_10_a_pipeline_with_isx_can_export_last_videos_to_nwb(self):
        # Given
        self._file_system.makedirs('input_dir')
        self._file_system.write('input_dir/file1.isxd', '')
        self._file_system.write('input_dir/file2.isxd', '')
        pipeline_input = 'input_dir'

        # When
        pipeline = CIPipe.with_videos_from_directory(pipeline_input, file_system=self._file_system, isx=InMemoryISX(self._file_system))
        pipeline.isx.preprocess_videos()
        pipeline.isx.export_movie_to_nwb()

        # Then
        self._assert_output_files(
            pipeline,
            'videos-nwb',
            [
                'output/Main Branch - Step 2 - ISX Export movie to NWB/file1-PP.nwb',
                'output/Main Branch - Step 2 - ISX Export movie to NWB/file2-PP.nwb',
            ],
            self._file_system,
        )

    def test_11_a_pipeline_with_isx_can_run_isx_longitudinal_registration(self):
        # Given
        self._file_system.makedirs('input_dir')
        self._file_system.write('input_dir/file1.isxd', '')
        self._file_system.write('input_dir/file2.isxd', '')
        pipeline_input = 'input_dir'

        # When
        pipeline = CIPipe.with_videos_from_directory(pipeline_input, file_system=self._file_system, isx=InMemoryISX(self._file_system))
        pipeline.isx.extract_neurons_pca_ica()
        pipeline.isx.detect_events_in_cells()
        pipeline.isx.longitudinal_registration()

        # Then
        self._assert_output_files(
            pipeline,
            'videos-isxd',
            [
                'output/Main Branch - Step 3 - ISX Longitudinal Registration/file1-LR.isxd',
                'output/Main Branch - Step 3 - ISX Longitudinal Registration/file2-LR.isxd',
            ],
            self._file_system,
        )
        self._assert_output_files(
            pipeline,
            'cellsets-isxd',
            [
                'output/Main Branch - Step 3 - ISX Longitudinal Registration/file1-PCA-ICA-LR.isxd',
                'output/Main Branch - Step 3 - ISX Longitudinal Registration/file2-PCA-ICA-LR.isxd',
            ],
            self._file_system,
        )
        self._assert_output_files(
            pipeline,
            'longitudinal-registration-correspondences-table',
            [
                'output/Main Branch - Step 3 - ISX Longitudinal Registration/LR-correspondences-table.csv',
            ],
            self._file_system,
        )
        self._assert_output_files(
            pipeline,
            'longitudinal-registration-crop-rect',
            [
                'output/Main Branch - Step 3 - ISX Longitudinal Registration/LR-crop-rect.csv',
            ],
            self._file_system,
        )
        self._assert_output_files(
            pipeline,
            'longitudinal-registration-transform',
            [
                'output/Main Branch - Step 3 - ISX Longitudinal Registration/LR-transform.csv',
            ],
            self._file_system,
        )

    def test_12_a_pipeline_with_isx_can_run_isx_longitudinal_registration_with_num_cells_desc_as_reference_selection(self):
        # Given
        self._file_system.makedirs('input_dir')
        self._file_system.write('input_dir/file1.isxd', '')
        self._file_system.write('input_dir/file2.isxd', '')
        pipeline_input = 'input_dir'

        # When
        pipeline = CIPipe.with_videos_from_directory(pipeline_input, file_system=self._file_system, isx=InMemoryISX(self._file_system))
        pipeline.isx.extract_neurons_pca_ica()
        pipeline.isx.detect_events_in_cells()
        pipeline.isx.longitudinal_registration(isx_lr_reference_selection_strategy='by_num_cells_desc')

        # Then
        self._assert_output_files(
            pipeline,
            'videos-isxd',
            [
                'output/Main Branch - Step 3 - ISX Longitudinal Registration/file1-LR.isxd',
                'output/Main Branch - Step 3 - ISX Longitudinal Registration/file2-LR.isxd',
            ],
            self._file_system,
        )
        self._assert_output_files(
            pipeline,
            'cellsets-isxd',
            [
                'output/Main Branch - Step 3 - ISX Longitudinal Registration/file1-PCA-ICA-LR.isxd',
                'output/Main Branch - Step 3 - ISX Longitudinal Registration/file2-PCA-ICA-LR.isxd',
            ],
            self._file_system,
        )

    def _assert_output_files(self, pipeline, key, expected_paths, file_system):
        output = pipeline.output(key)
        self.assertEqual(len(output), len(expected_paths))
        for i, expected in enumerate(expected_paths):
            self.assertEqual(output[i]['value'], expected)
            self.assertTrue(file_system.exists(expected))

    def _initialize_directory_with_two_videos(self):
        self._file_system.makedirs('input_dir')
        self._file_system.write('input_dir/file1.isxd', '')
        self._file_system.write('input_dir/file2.isxd', '')


if __name__ == '__main__':
    unittest.main()
