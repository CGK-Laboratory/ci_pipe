import unittest

from ci_pipe.errors.caiman_backend_not_configured_error import CaimanBackendNotConfiguredError
from ci_pipe.pipeline import CIPipe
from tests.ci_pipe_test_case import CIPipeTestCase


class CaimanTestCase(CIPipeTestCase):
    def test_01_a_pipeline_without_caiman_can_not_run_caiman_step(self):
        # Given
        pipeline_input = {'videos': ['data_endoscope.tif']}

        # When
        pipeline = CIPipe(pipeline_input, file_system=self._file_system)

        # Then
        with self.assertRaises(CaimanBackendNotConfiguredError):
            pipeline.caiman.preprocess_videos()

if __name__ == '__main__':
    unittest.main()
