from .ci_pipe_error import CIPipeError


class DefaultsAfterStepsError(CIPipeError):
    def __init__(self):
        super().__init__("Defaults must be set before adding any steps.")