from .ci_pipe_error import CIPipeError


class CaimanBackendNotConfiguredError(CIPipeError):
    def __init__(self):
        super().__init__("Caiman backend is not configured for this pipeline.")