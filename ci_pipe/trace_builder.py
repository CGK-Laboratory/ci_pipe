import json

from external_dependencies.file_system.persistent_file_system import PersistentFileSystem

class TraceBuilder:
    
    def __init__(self, file_name = "trace.json", file_system=None):
        self._file_name = file_name
        self._file_system = file_system or PersistentFileSystem()

    def build_initial_trace(self, pipeline_inputs, defaults):
        trace = self._initial_trace(defaults, pipeline_inputs)
        self._file_system.write(self._file_name, json.dumps(trace, indent=4))

    def update_trace_with_steps(self, steps, branch_name):
        trace = self._load_trace_from_file()
        if branch_name not in trace:
            trace[branch_name] = {"steps": []}
        trace[branch_name]["steps"] = self._steps_to_json(steps)
        self._file_system.write(self._file_name, json.dumps(trace, indent=4))

    def _steps_to_json(self, steps):
        steps_json = []
        for idx, step in enumerate(steps, 1):
            self._append_step_to_trace(steps_json, idx, step, step.step_output())
        return steps_json

    def _append_step_to_trace(self, steps_json, idx, step, outputs_json):
        steps_json.append({
                "index": idx,
                "name": step.name(),
                "params": step.arguments(),
                "outputs": outputs_json
            })

    def _initial_trace(self, defaults, inputs_json):
        return {
            "pipeline": {
                "inputs": inputs_json,
                "defaults": defaults or {}
            }
        }

    def _load_trace_from_file(self):
        try:
            trace = json.loads(self._file_system.read(self._file_name))
        except Exception:
            trace = self._initial_trace({}, {})
        return trace