from ci_pipe.decorators import step

class ISXModule():
    # TODO: Consider moving these constants to a different config file
    PREPROCESS_VIDEOS_STEP = "ISX Preprocess Videos"
    BANDPASS_FILTER_VIDEOS_STEP = "ISX Bandpass Filter Videos"
    PREPROCESS_VIDEOS_SUFIX = "PP"
    BANDPASS_FILTER_VIDEOS_SUFIX = "BP"

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

