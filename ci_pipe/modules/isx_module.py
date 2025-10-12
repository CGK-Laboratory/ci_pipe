from ci_pipe.decorators import step
from ci_pipe.errors.isx_backend_not_configured_error import ISXBackendNotConfiguredError


class ISXModule():
    # TODO: Consider moving these constants to a different config file
    PREPROCESS_VIDEOS_STEP = "ISX Preprocess Videos"
    PREPROCESS_VIDEOS_SUFIX = "PP"

    def __init__(self, isx, ci_pipe):
        if isx is None:
            raise ISXBackendNotConfiguredError()
        self._isx = isx
        self._ci_pipe = ci_pipe

    @step(PREPROCESS_VIDEOS_STEP)
    def preprocess_videos(
        self,
        inputs,
        temporal_downsample_factor=1,
        spatial_downsample_factor=1,
        crop_rect=None,
        crop_rect_format="tlbr",
        fix_defective_pixels=True,
        trim_early_frames=True
    ):
        output = []
        output_dir = self._ci_pipe.output_directory_for_next_step(self.PREPROCESS_VIDEOS_STEP)
        self._ci_pipe.create_output_directory_for_next_step(self.PREPROCESS_VIDEOS_STEP)

        for input in inputs('videos-isxd'):
            input_path = input['value']
            output_path = self._isx.make_output_file_path(input_path, output_dir, self.PREPROCESS_VIDEOS_SUFIX)

            self._isx.preprocess(
            input_movie_files=[input_path],
            output_movie_files=[output_path],
            temporal_downsample_factor=temporal_downsample_factor,
            spatial_downsample_factor=spatial_downsample_factor,
            crop_rect=crop_rect,
            crop_rect_format=crop_rect_format,
            fix_defective_pixels=fix_defective_pixels,
            trim_early_frames=trim_early_frames
            )

            output.append({'ids': input['ids'], 'value': output_path})

        return {
            'videos-isxd': output
        }
