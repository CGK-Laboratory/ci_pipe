from ci_pipe.decorators import step
from ci_pipe.errors.caiman_backend_not_configured_error import CaimanBackendNotConfiguredError


class CaimanModule:
    MOTION_CORRECTION_STEP = "Caiman Motion Correction"
    MOTION_CORRECTION_VIDEOS_SUFFIX = "MC"

    def __init__(self, caiman, ci_pipe):
        if caiman is None:
            raise CaimanBackendNotConfiguredError()
        self._caiman = caiman
        self._ci_pipe = ci_pipe

    @step(MOTION_CORRECTION_STEP)
    def motion_correction(
            self,
            inputs,
            *,
            caiman_strides=(48, 48),
            caiman_overlaps=(24, 24),
            caiman_max_shifts=(6, 6),
            caiman_max_deviation_rigid=3,
            caiman_pw_rigid=True,
            caiman_shifts_opencv=True,
            caiman_border_nan='copy',
            caiman_save_movie=True
    ):
        # TODO: Think if we should grab all potential extensions accepted by motion correction
        output = []
        output_dir = self._ci_pipe.create_output_directory_for_next_step(self.MOTION_CORRECTION_STEP)

        for input_data in inputs('videos-tif'):
            motion_correct_handler = self._caiman.motion_correction.MotionCorrect(
                fname=input_data['value'],
                strides=caiman_strides,
                overlaps=caiman_overlaps,
                max_shifts=caiman_max_shifts,
                max_deviation_rigid=caiman_max_deviation_rigid,
                pw_rigid=caiman_pw_rigid,
                shifts_opencv=caiman_shifts_opencv,
                border_nan=caiman_border_nan,
            )
            motion_correct_handler.motion_correct(save_movie=caiman_save_movie)
            mmap_files = motion_correct_handler.mmap_file
            print("MMAP FILES: ", mmap_files)

            if isinstance(mmap_files, (list, tuple)):
                if len(mmap_files) != 1:
                    raise ValueError(f"Expected one output file, got: {mmap_files}")
                output_location_path = mmap_files[0]
            else:
                output_location_path = mmap_files

            output_path = self._ci_pipe.make_output_file_path(
                output_location_path,
                output_dir,
                self.MOTION_CORRECTION_VIDEOS_SUFFIX)
            self._ci_pipe.copy_file_to_output_directory(output_location_path, self.MOTION_CORRECTION_STEP, )
            output.append({'ids': input_data['ids'], 'value': output_path})

        print("NEW OUTPUTS: ", output)
        return {"videos-caiman": output}
