from ci_pipe.errors.caiman_backend_not_configured_error import CaimanBackendNotConfiguredError


class CaimanModule:
    MOTION_CORRECTION_STEP = "Caiman Motion Correction"
    SOURCE_EXTRACTION_STEP = "Caiman Source Extraction"
    DECONVOLUTION_STEP = "Caiman Spike / Event Inference"
    EVALUATION_STEP = "Caiman Automated Component Evaluation"

    def __init__(self, caiman, ci_pipe):
        if caiman is None:
            raise CaimanBackendNotConfiguredError()
        self._caiman = caiman
        self._ci_pipe = ci_pipe

    def motion_correct(self, input_movie_files, output_movie_files, **kwargs):
        mc = self._caiman(
            input_movie_files,
            pw_rigid=True,               # piecewise-rigid (False = rigid)
            gSig_filt=(3, 3),            # 1p high-pass filter size
            max_shifts=(6, 6),           # max allowed shift (px)
            strides=(48, 48),            # patch grid stride for pw-rigid
            overlaps=(24, 24),           # patch overlaps
            border_nan='copy'            # how to deal with borders
        )
        mc.motion_correct(save_movie=True)
