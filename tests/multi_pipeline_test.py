import unittest

from ci_pipe.multi_pipeline import MultiCIPipe
from external_dependencies.isx.in_memory_isx import InMemoryISX
from tests.ci_pipe_test_case import CIPipeTestCase


class MultiPipelineTestCase(CIPipeTestCase):
    def test_01_multi_pipeline_contains_a_pipeline(self):
        # Given
        self._file_system.makedirs('input_dir')
        self._file_system.makedirs('input_dir/pipeline1')
        self._file_system.write('input_dir/pipeline1/file1.isxd', '')
        pipeline_input = 'input_dir'
        
        # When
        multi_pipe = MultiCIPipe(
            pipeline_input,
            file_system=self._file_system,
        )

        # Then
        self.assertIsNotNone(multi_pipe.pipeline("pipeline1"))
        self.assertIsNone(multi_pipe.pipeline("pipeline2"))

    def test_02_multi_pipeline_contains_multiple_pipelines(self):
        # Given
        self._file_system.makedirs('input_dir')
        self._file_system.makedirs('input_dir/pipeline1')
        self._file_system.write('input_dir/pipeline1/file1.isxd', '')
        self._file_system.makedirs('input_dir/pipeline2')
        self._file_system.write('input_dir/pipeline2/file2.isxd', '')
        pipeline_input = 'input_dir'

        # When
        multi_pipe = MultiCIPipe(
            pipeline_input,
            file_system=self._file_system,
        )

        # Then
        self.assertIsNotNone(multi_pipe.pipeline("pipeline1"))
        self.assertIsNotNone(multi_pipe.pipeline("pipeline2"))

    def test_03_multi_pipeline_proxies_module_calls_to_all_pipelines(self):
        # Given
        self._file_system.makedirs('input_dir')
        self._file_system.makedirs('input_dir/pipeline1')
        self._file_system.write('input_dir/pipeline1/file1.isxd', '')
        self._file_system.makedirs('input_dir/pipeline2')
        self._file_system.write('input_dir/pipeline2/file2.isxd', '')
        pipeline_input = 'input_dir'

        # When
        multi_pipe = MultiCIPipe(
            pipeline_input,
            file_system=self._file_system,
            isx=InMemoryISX(self._file_system),
        )
        multi_pipe.isx.preprocess_videos()

        # Then
        self.assertTrue(self._file_system.exists('output/pipeline1/Main Branch - Step 1 - ISX Preprocess Videos/file1-PP.isxd'))
        self.assertTrue(self._file_system.exists('output/pipeline2/Main Branch - Step 1 - ISX Preprocess Videos/file2-PP.isxd'))

    def test_04_can_branch_multi_pipeline(self):
        # Given
        self._file_system.makedirs('input_dir')
        self._file_system.makedirs('input_dir/pipeline1')
        self._file_system.write('input_dir/pipeline1/file1.isxd', '')
        self._file_system.makedirs('input_dir/pipeline2')
        self._file_system.write('input_dir/pipeline2/file2.isxd', '')
        pipeline_input = 'input_dir'

        # When
        multi_pipe = MultiCIPipe(
            pipeline_input,
            file_system=self._file_system,
            isx=InMemoryISX(self._file_system),
        )
        multi_pipe.isx.preprocess_videos()
        branched_multi_pipe = multi_pipe.branch("Second branch")
        branched_multi_pipe.isx.preprocess_videos()

        # Then
        self.assertTrue(self._file_system.exists('output/pipeline1/Main Branch - Step 1 - ISX Preprocess Videos/file1-PP.isxd'))
        self.assertTrue(self._file_system.exists('output/pipeline2/Main Branch - Step 1 - ISX Preprocess Videos/file2-PP.isxd'))
        self.assertIsInstance(branched_multi_pipe, MultiCIPipe)
        self.assertTrue(self._file_system.exists('output/pipeline1/Second branch - Step 2 - ISX Preprocess Videos/file1-PP-PP.isxd'))
        self.assertTrue(self._file_system.exists('output/pipeline2/Second branch - Step 2 - ISX Preprocess Videos/file2-PP-PP.isxd'))

    def test_05_set_defaults_on_multi_pipeline_propagates_to_pipelines(self):
        # Given
        self._file_system.makedirs('input_dir')
        self._file_system.makedirs('input_dir/pipeline1')
        self._file_system.write('input_dir/pipeline1/file1.isxd', '')
        pipeline_input = 'input_dir'

        # When
        multi_pipe = MultiCIPipe(
            pipeline_input,
            file_system=self._file_system,
        )
        multi_pipe.set_defaults(default_value=1)

        # Then
        self.assertEqual(multi_pipe.pipeline("pipeline1").defaults().get("default_value"), 1)

    def test_06_multi_pipeline_gives_specific_values(self):
        # Given
        self._file_system.makedirs('input_dir')
        self._file_system.makedirs('input_dir/pipeline1')
        self._file_system.write('input_dir/pipeline1/file1.isxd', '')
        self._file_system.makedirs('input_dir/pipeline2')
        self._file_system.write('input_dir/pipeline2/file2.isxd', '')
        pipeline_input = 'input_dir'

        # When
        multi_pipe = MultiCIPipe(
            pipeline_input,
            file_system=self._file_system,
            isx=InMemoryISX(self._file_system),
        )

        # Then
        values = multi_pipe.values('videos-isxd')
        self.assertCountEqual(values, ['input_dir/pipeline1/file1.isxd', 'input_dir/pipeline2/file2.isxd'])
    
        


if __name__ == '__main__':
    unittest.main()