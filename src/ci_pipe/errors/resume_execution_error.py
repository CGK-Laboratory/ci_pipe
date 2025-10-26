from ci_pipe.errors.ci_pipe_error import CIPipeError


class ResumeExecutionError(CIPipeError):
    def __init__(self):
        super().__init__("Cannot resume execution without the same trace file and output directory")