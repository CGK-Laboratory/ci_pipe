from .ci_pipe_error import CIPipeError


class OutputKeyNotFoundError(CIPipeError):
    def __init__(self, key: str):
        super().__init__(
            f"Key '{key}' not found in any step output or pipeline input.",
            context={"key": key},
        )