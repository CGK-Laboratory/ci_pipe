from .ci_pipe_error import CIPipeError


class ISXBackendNotConfiguredError(CIPipeError):
    def __init__(self):
        super().__init__("ISX backend is not configured for this pipeline.")