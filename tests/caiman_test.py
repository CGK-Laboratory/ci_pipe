import unittest

from ci_pipe.errors.caiman_backend_not_configured_error import CaimanBackendNotConfiguredError
from ci_pipe.pipeline import CIPipe
from external_dependencies.caiman.in_memory_caiman import InMemoryCaiman
from tests.ci_pipe_test_case import CIPipeTestCase


class CaimanTestCase(CIPipeTestCase):
    def test_01_a_pipeline_without_caiman_can_not_run_caiman_step(self):
        # Given
        pipeline_input = {'videos': ['data_endoscope.tif']}

        # When
        pipeline = CIPipe(pipeline_input, file_system=self._file_system)

        # Then
        with self.assertRaises(CaimanBackendNotConfiguredError):
            pipeline.caiman.motion_correction()

    def test_02_a_pipeline_with_caiman_can_execute_motion_correction_algorithm(self):
        # Given
        pipeline_input = 'input_dir'
        self._file_system.makedirs(pipeline_input)
        self._file_system.write(f'{pipeline_input}/file1.tiff', '')
        pipeline = CIPipe.with_videos_from_directory(
            pipeline_input,
            file_system=self._file_system,
            caiman=InMemoryCaiman(self._file_system)
        )

        # When
        pipeline.caiman.motion_correction()

        # Then
        self._assert_output_files(
            pipeline,
            'videos-caiman',
            [
                'output/Main Branch - Step 1 - Caiman Motion Correction/file1-MC.tiff',
            ],
            self._file_system,
        )

    def _assert_output_files(self, pipeline, key, expected_paths, file_system):
        output = pipeline.output(key)
        self.assertEqual(len(output), len(expected_paths))
        for i, expected in enumerate(expected_paths):
            self.assertEqual(output[i]['value'], expected)
            self.assertTrue(file_system.exists(expected))


if __name__ == '__main__':
    unittest.main()
