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
        output = pipeline.output('videos-isxd')
        self.assertEqual(len(output), 2)
        self.assertEqual(output[0]['value'], 'output/Main Branch - Step 1 - ISX Preprocess Videos/file1-PP.isxd')
        self.assertEqual(output[1]['value'], 'output/Main Branch - Step 1 - ISX Preprocess Videos/file2-PP.isxd')
        self.assertTrue(file_system.exists('output/Main Branch - Step 1 - ISX Preprocess Videos/file1-PP.isxd'))
        self.assertTrue(file_system.exists('output/Main Branch - Step 1 - ISX Preprocess Videos/file2-PP.isxd'))

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
        output = pipeline.output('videos-isxd')
        self.assertEqual(len(output), 2)
        self.assertEqual(output[0]['value'], 'output/Main Branch - Step 1 - ISX Bandpass Filter Videos/file1-BP.isxd')
        self.assertEqual(output[1]['value'], 'output/Main Branch - Step 1 - ISX Bandpass Filter Videos/file2-BP.isxd')
        self.assertTrue(file_system.exists('output/Main Branch - Step 1 - ISX Bandpass Filter Videos/file1-BP.isxd'))
        self.assertTrue(file_system.exists('output/Main Branch - Step 1 - ISX Bandpass Filter Videos/file2-BP.isxd'))

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
        output_videos = pipeline.output('videos-isxd')
        self.assertEqual(len(output_videos), 2)
        self.assertEqual(output_videos[0]['value'], 'output/Main Branch - Step 1 - ISX Motion Correction Videos/file1-MC.isxd')
        self.assertEqual(output_videos[1]['value'], 'output/Main Branch - Step 1 - ISX Motion Correction Videos/file2-MC.isxd')
        self.assertTrue(file_system.exists('output/Main Branch - Step 1 - ISX Motion Correction Videos/file1-MC.isxd'))
        self.assertTrue(file_system.exists('output/Main Branch - Step 1 - ISX Motion Correction Videos/file2-MC.isxd'))
        output_translations = pipeline.output('motion-correction-translations')
        self.assertEqual(len(output_translations), 2)
        self.assertEqual(output_translations[0]['value'], 'output/Main Branch - Step 1 - ISX Motion Correction Videos/file1-translations.csv')
        self.assertEqual(output_translations[1]['value'], 'output/Main Branch - Step 1 - ISX Motion Correction Videos/file2-translations.csv')
        self.assertTrue(file_system.exists('output/Main Branch - Step 1 - ISX Motion Correction Videos/file1-translations.csv'))
        self.assertTrue(file_system.exists('output/Main Branch - Step 1 - ISX Motion Correction Videos/file2-translations.csv'))
        output_crop_rect = pipeline.output('motion-correction-crop-rect')
        self.assertEqual(len(output_crop_rect), 2)
        self.assertEqual(output_crop_rect[0]['value'], 'output/Main Branch - Step 1 - ISX Motion Correction Videos/file1-series-crop-rect.csv')
        self.assertEqual(output_crop_rect[1]['value'], 'output/Main Branch - Step 1 - ISX Motion Correction Videos/file2-series-crop-rect.csv')
        self.assertTrue(file_system.exists('output/Main Branch - Step 1 - ISX Motion Correction Videos/file1-series-crop-rect.csv'))
        self.assertTrue(file_system.exists('output/Main Branch - Step 1 - ISX Motion Correction Videos/file2-series-crop-rect.csv'))
        output_mean_images = pipeline.output('motion-correction-mean-images')
        self.assertEqual(len(output_mean_images), 2)
        self.assertEqual(output_mean_images[0]['value'], 'output/Main Branch - Step 1 - ISX Motion Correction Videos/file1-series-mean-image.isxd')
        self.assertEqual(output_mean_images[1]['value'], 'output/Main Branch - Step 1 - ISX Motion Correction Videos/file2-series-mean-image.isxd')
        self.assertTrue(file_system.exists('output/Main Branch - Step 1 - ISX Motion Correction Videos/file1-series-mean-image.isxd'))
        self.assertTrue(file_system.exists('output/Main Branch - Step 1 - ISX Motion Correction Videos/file2-series-mean-image.isxd'))

if __name__ == '__main__':
    unittest.main()
