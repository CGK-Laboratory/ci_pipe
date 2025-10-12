from ci_pipe.decorators import step

class ISXModule():
    # TODO: Consider moving these constants to a different config file
    PREPROCESS_VIDEOS_STEP = "ISX Preprocess Videos"
    BANDPASS_FILTER_VIDEOS_STEP = "ISX Bandpass Filter Videos"
    MOTION_CORRECTION_VIDEOS_STEP = "ISX Motion Correction Videos"
    NORMALIZE_DFF_VIDEOS_STEP = "ISX Normalize DFF Videos"
    PREPROCESS_VIDEOS_SUFIX = "PP"
    BANDPASS_FILTER_VIDEOS_SUFIX = "BP"
    MOTION_CORRECTION_VIDEOS_SUFIX = "MC"
    MOTION_CORRECTION_VIDEOS_TRANSLATIONS_SUFIX = "translations"
    MOTION_CORRECTION_VIDEOS_CROP_RECT_SUFIX = "crop-rect"
    MOTION_CORRECTION_VIDEOS_MEAN_IMAGES_SUFIX = "mean-image"
    NORMALIZE_DFF_VIDEOS_SUFIX = "DFF"

    def __init__(self, isx, ci_pipe):
        if isx is None:
            raise RuntimeError("CIPipe 'isx' attribute was not provided. Cannot use an ISX step.")
        self._isx = isx
        self._ci_pipe = ci_pipe

    @step(PREPROCESS_VIDEOS_STEP)
    def preprocess_videos(
        self,
        inputs,
        *,
        isx_pp_temporal_downsample_factor=1,
        isx_pp_spatial_downsample_factor=1,
        isx_pp_crop_rect=None,
        isx_pp_crop_rect_format="tlbr",
        isx_pp_fix_defective_pixels=True,
        isx_pp_trim_early_frames=True
    ):
        output = []
        output_dir = self._ci_pipe.create_output_directory_for_next_step(self.PREPROCESS_VIDEOS_STEP)

        for input in inputs('videos-isxd'):
            input_path = input['value']
            output_path = self._isx.make_output_file_path(input_path, output_dir, self.PREPROCESS_VIDEOS_SUFIX)

            self._isx.preprocess(
                input_movie_files=[input_path],
                output_movie_files=[output_path],
                temporal_downsample_factor=isx_pp_temporal_downsample_factor,
                spatial_downsample_factor=isx_pp_spatial_downsample_factor,
                crop_rect=isx_pp_crop_rect,
                crop_rect_format=isx_pp_crop_rect_format,
                fix_defective_pixels=isx_pp_fix_defective_pixels,
                trim_early_frames=isx_pp_trim_early_frames
            )

            output.append({'ids': input['ids'], 'value': output_path})

        return {
            'videos-isxd': output
        }

    @step(BANDPASS_FILTER_VIDEOS_STEP)
    def bandpass_filter_videos(
        self,
        inputs,
        *,
        isx_bp_low_cutoff=0.005,
        isx_bp_high_cutoff=0.5,
        isx_bp_retain_mean=False,
        isx_bp_subtract_global_minimum=True
    ):
        output = []
        output_dir = self._ci_pipe.create_output_directory_for_next_step(self.BANDPASS_FILTER_VIDEOS_STEP)

        for input in inputs('videos-isxd'):
            input_path = input['value']
            output_path = self._isx.make_output_file_path(input_path, output_dir, self.BANDPASS_FILTER_VIDEOS_SUFIX)

            self._isx.spatial_filter(
                input_movie_files=[input_path],
                output_movie_files=[output_path],
                low_cutoff=isx_bp_low_cutoff,
                high_cutoff=isx_bp_high_cutoff,
                retain_mean=isx_bp_retain_mean,
                subtract_global_minimum=isx_bp_subtract_global_minimum
            )

            output.append({'ids': input['ids'], 'value': output_path})

        return {
            'videos-isxd': output
        }

    @step(MOTION_CORRECTION_VIDEOS_STEP)
    def motion_correction_videos(
        self,
        inputs,
        *,
        isx_mc_series_name="series",
        isx_mc_max_translation=20,
        isx_mc_low_bandpass_cutoff=0.004,
        isx_mc_high_bandpass_cutoff=0.016,
        isx_mc_roi=None,
        isx_mc_reference_segment_index=0,
        isx_mc_reference_frame_index=0,
        isx_mc_global_registration_weight=1,
        isx_mc_preserve_input_dimensions=False
    ):
        output_videos = []
        output_translations = []
        output_crop_rects = []
        output_mean_images = []
        output_dir = self._ci_pipe.create_output_directory_for_next_step(self.MOTION_CORRECTION_VIDEOS_STEP)

        for input in inputs('videos-isxd'):
            input_path = input['value']
            output_video_path = self._isx.make_output_file_path(input_path, output_dir, self.MOTION_CORRECTION_VIDEOS_SUFIX)
            output_translations_path = self._isx.make_output_file_path(input_path, output_dir, self.MOTION_CORRECTION_VIDEOS_TRANSLATIONS_SUFIX, ext='csv')
            output_crop_rect_path = self._isx.make_output_file_path(input_path, output_dir, f'{isx_mc_series_name}-{self.MOTION_CORRECTION_VIDEOS_CROP_RECT_SUFIX}', ext='csv')
            output_mean_image_path = self._isx.make_output_file_path(input_path, output_dir, f'{isx_mc_series_name}-{self.MOTION_CORRECTION_VIDEOS_MEAN_IMAGES_SUFIX}')

            self._isx.project_movie(
                input_movie_files=[input_path],
                output_image_file=output_mean_image_path
            )

            self._isx.motion_correct(
                input_movie_files=[input_path],
                output_movie_files=[output_video_path],
                max_translation=isx_mc_max_translation,
                low_bandpass_cutoff=isx_mc_low_bandpass_cutoff,
                high_bandpass_cutoff=isx_mc_high_bandpass_cutoff,
                roi=isx_mc_roi,
                reference_segment_index=isx_mc_reference_segment_index,
                reference_frame_index=isx_mc_reference_frame_index,
                reference_file_name=output_mean_image_path,
                global_registration_weight=isx_mc_global_registration_weight,
                output_translation_files=[output_translations_path],
                output_crop_rect_file=output_crop_rect_path,
                preserve_input_dimensions=isx_mc_preserve_input_dimensions
            )

            output_videos.append({'ids': input['ids'], 'value': output_video_path})
            output_translations.append({'ids': input['ids'], 'value': output_translations_path})
            output_crop_rects.append({'ids': input['ids'], 'value': output_crop_rect_path})
            output_mean_images.append({'ids': input['ids'], 'value': output_mean_image_path})

        return {
            'videos-isxd': output_videos,
            'motion-correction-translations': output_translations,
            'motion-correction-crop-rect': output_crop_rects,
            'motion-correction-mean-images': output_mean_images
        }
    
    @step(NORMALIZE_DFF_VIDEOS_STEP)
    def normalize_dff_videos(
        self,
        inputs,
        *,
        isx_dff_f0_type='mean'
    ):
        output = []
        output_dir = self._ci_pipe.create_output_directory_for_next_step(self.NORMALIZE_DFF_VIDEOS_STEP)

        for input in inputs('videos-isxd'):
            input_path = input['value']
            output_path = self._isx.make_output_file_path(input_path, output_dir, self.NORMALIZE_DFF_VIDEOS_SUFIX)

            self._isx.dff(
                input_movie_files=[input_path],
                output_movie_files=[output_path],
                f0_type=isx_dff_f0_type
            )

            output.append({'ids': input['ids'], 'value': output_path})

        return {
            'videos-isxd': output
        }