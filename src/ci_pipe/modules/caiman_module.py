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
            caiman_border_nan='copy'
    ):
        # TODO: Think if we should grab all potential extensions accepted by motion correction
        paths_of_videos_to_process = inputs('videos-tiff')
        print("VIDEOS-TIFF: ", paths_of_videos_to_process)
        parameters_dictionary = {
            'fnames': paths_of_videos_to_process,
            'strides': caiman_strides,
            'overlaps': caiman_overlaps,
            'max_shifts': caiman_max_shifts,
            'max_deviation_shift': caiman_max_deviation_rigid,
            'pw_rigid': caiman_pw_rigid,
            'shifts_opencv': caiman_shifts_opencv,
            'border_nan': caiman_border_nan
        }
        parameters = self._caiman.CNMFParams(params_dict=parameters_dictionary)
        motion_correct_handler = self._caiman.MotionCorrect(
            executed_steps=paths_of_videos_to_process,
            **parameters.motion)
        output = []
        output_dir = self._ci_pipe.create_output_directory_for_next_step(self.MOTION_CORRECTION_STEP)

        motion_correct_handler.motion_correct() # this is executing all file paths...
        outputs = self._caiman.make_output_file_paths(motion_correct_handler.mmap_file, output_dir, self.MOTION_CORRECTION_VIDEOS_SUFFIX)
        outputs_plus_ids = {'ids': paths_of_videos_to_process['ids'], 'value': outputs}
        print("NEW OUTPUTS: ", outputs_plus_ids)
        # el problema que estoy teniendo es que necesito recorrer cada input dentro de videos-tiff para poder ejecutar el algoritmo y guardar el archivo de salida, pero revisar la construccion de parametros porque se pasa ahí y acá necesito manejarlo diferente al modulo de isx.
        return {"videos-caiman": outputs_plus_ids}