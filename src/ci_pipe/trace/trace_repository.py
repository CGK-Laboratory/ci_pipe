import json

from ci_pipe.trace.ci_pipe_trace import CIPipeTrace


class TraceRepository:
    def __init__(self, file_system, filename, validator=None):
        self._file_system = file_system
        self._filename = filename
        self._validator = validator

    def load(self) -> CIPipeTrace:
        try:
            json_trace = json.loads(self._file_system.read(self._filename))
        except Exception:
            json_trace = {}
        return CIPipeTrace.from_dict(json_trace)

    def save(self, trace: CIPipeTrace):
        trace_as_json = trace.to_dict()
        self._file_system.write(self._filename, json.dumps(trace_as_json, indent=4))

    def exists(self):
        return self._file_system.exists(self._filename)

    # TODO: Think if we need to handle this onSave instead of here
    def validate(self):
        if not self._validator:
            return True
        data_as_json = self.load().to_dict()
        return self._validator.validate(data_as_json)
    
    def trace_path(self):
        return self._filename