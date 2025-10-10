import json

from external_dependencies.file_system.persistent_file_system import PersistentFileSystem

class TraceBuilder:
    
    def __init__(self, file_name = "trace.json", file_system=None):
        self._file_name = file_name
        self._file_system = file_system or PersistentFileSystem()

    def build_initial_trace(self, pipeline_inputs, defaults, outputs_directory = None):
        trace = self._initial_trace(defaults, pipeline_inputs, outputs_directory)
        self._file_system.write(self._file_name, json.dumps(trace, indent=4))

    def update_trace_with_steps(self, steps, branch_name):
        trace = self._load_trace_from_file()
        if branch_name not in trace:
            trace[branch_name] = {"steps": []}
        trace[branch_name]["steps"] = self._steps_to_json(steps)
        self._file_system.write(self._file_name, json.dumps(trace, indent=4))

    def was_step_already_executed(self, branch_name, step_name):
        trace = self._load_trace_from_file()
        if branch_name not in trace:
            return False
        steps = trace[branch_name]["steps"]
        for step in steps:
            if step["name"] == step_name:
                return True
        return False

    def filename(self):
        return self._file_name

    def load_steps_from(self, branch_name):
        trace = self._load_trace_from_file()
        branch = trace.get(branch_name) or {}
        return list(branch.get("steps") or [])

    def restore_callables_functions_for(self, branch_name):
        items = []
        for restored_step in self.load_steps_from(branch_name):
            name = restored_step.get("name")
            params = restored_step.get("params") or {}
            outputs = restored_step.get("outputs") or {}

            def make_output_fn_from(snapshot):
                def _fn(_inputs):
                    return snapshot
                return _fn

            items.append((name, params, make_output_fn_from(outputs)))
        return items

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

    def _initial_trace(self, defaults, inputs_json, outputs_directory):
        return {
            "pipeline": {
                "inputs": inputs_json,
                "defaults": defaults or {},
                "outputs_directory": outputs_directory
            }
        }

    def _load_trace_from_file(self):
        try:
            trace = json.loads(self._file_system.read(self._file_name))
        except Exception:
            trace = self._initial_trace({}, {}, outputs_directory=None)
        return trace