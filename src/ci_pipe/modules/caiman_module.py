from ci_pipe.decorators import step
from ci_pipe.errors.caiman_backend_not_configured_error import CaimanBackendNotConfiguredError


class CaimanModule:
    MOTION_CORRECTION_STEP = "Caiman Motion Correction"

    def __init__(self, caiman, ci_pipe):
        if caiman is None:
            raise CaimanBackendNotConfiguredError()
        self._caiman = caiman
        self._ci_pipe = ci_pipe

    @step(MOTION_CORRECTION_STEP)
    def motion_correction(self, inputs,):
        path_to_videos_to_process = inputs('videos-tiff')
        print("VIDEOS TO PROCESS: ", path_to_videos_to_process)
        motion_correct_handler = self._caiman.MotionCorrect(file_name=path_to_videos_to_process, )
        parameters = self._xxx_params(path_to_videos_to_process)
        path_to_corrected_videos = motion_correct_handler.motion_correct(path_to_videos_to_process, **parameters.motion,)
        return {"movies-caiman": path_to_corrected_videos}

    def _xxx_params(self, movie_path,):
        # TODO: Check what to do with params which are dependant on numpy
        # For now I have removed them since they belong to a different category of params
        # such as CNMF parameters for source extraction and deconvolution and not motion correction oens
        parameter_dict = {'fnames': movie_path,
                          'fr': 30,
                          'dxy': (2., 2.),
                          'decay_time': 0.4,
                          'strides': (48, 48),
                          'overlaps': (24, 24),
                          'max_shifts': (6,6),
                          'max_deviation_rigid': 3,
                          'pw_rigid': True,
                          'p': 1,
                          'nb': 2,
                          'rf': 15,
                          'K': 4,
                          'stride': 10,
                          'method_init': 'greedy_roi',
                          'rolling_sum': True,
                          'only_init': True,
                          'ssub': 1,
                          'tsub': 1,
                          'merge_thr': 0.85,
                          'bas_nonneg': True,
                          'min_SNR': 2.0,
                          'rval_thr': 0.85,
                          'use_cnn': True,
                          'min_cnn_thr': 0.99,
                          'cnn_lowest': 0.1}

        parameters = self._caiman.CNMFParams(params_dict=parameter_dict)
        return parameters