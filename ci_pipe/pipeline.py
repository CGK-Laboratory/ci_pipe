from ci_pipe.step import Step
from ci_pipe.trace_builder import TraceBuilder
from external_dependencies.file_system.persistent_file_system import PersistentFileSystem

import inspect
import hashlib

class CIPipe:
    def __init__(self, inputs, branch_name='Main Branch', outputs_directory='output', steps=None, file_system=None, trace_builder=None, defaults=None, plotter=None):
        self._pipeline_inputs = self._inputs_with_ids(inputs)
        self._raw_pipeline_inputs = inputs
        self._steps = steps or []
        self._defaults = defaults or {}
        self._branch_name = branch_name
        self._outputs_directory = outputs_directory
        self._file_system = file_system or PersistentFileSystem()
        self._trace_builder = trace_builder
        self._plotter = plotter

        self._build_initial_trace()

    def output(self, key):
        for step in reversed(self._steps):
            if key in step.step_output():
                return step.step_output()[key]
        if key in self._pipeline_inputs:
            return self._pipeline_inputs[key]
        raise KeyError(f"Key '{key}' not found in any step output or pipeline input.")
    
    def step(self, step_name, step_function, *args, **kwargs):
        self._populate_default_params(step_function, kwargs)
        new_step = Step(step_name, self.output, step_function, args, kwargs)
        self._steps.append(new_step)
        self._update_trace_if_trace_builder_provided()
        return self
    
    def info(self, step_number):
        self._plotter.get_step_info(self._trace_builder._load_trace_from_file(), step_number, self._branch_name)

    def trace(self):
        self._plotter.get_all_trace_from_branch(self._trace_builder._load_trace_from_file(), self._branch_name)
    
    def branch(self, branch_name):
        new_pipe = CIPipe(self._raw_pipeline_inputs.copy(), branch_name=branch_name, steps=self._steps.copy(), file_system=self._file_system, trace_builder=self._trace_builder, defaults=self._defaults.copy())
        return new_pipe
    
    def set_defaults(self, **defaults):
        if self._steps:
            raise RuntimeError("Defaults must be set before adding any steps.")
        self._load_defaults(defaults)
        self._build_initial_trace()
        return self
    
    def _load_defaults(self, defaults):
        for defaults_key, defaults_value in defaults.items():
            self._defaults[defaults_key] = defaults_value
    
    def _populate_default_params(self, step_function, kwargs):
        for name, param in inspect.signature(step_function).parameters.items():
            if name in kwargs:
                continue
            if param.kind == param.KEYWORD_ONLY:
                if name in self._defaults:
                    kwargs[name] = self._defaults[name]
                elif param.default is not inspect.Parameter.empty:
                    kwargs[name] = param.default
    
    def _build_initial_trace(self):
        if self._trace_builder and not self._steps:
            self._trace_builder.build_initial_trace(self._pipeline_inputs, self._defaults)

    def _update_trace_if_trace_builder_provided(self):
        if self._trace_builder:
            self._trace_builder.update_trace_with_steps(self._steps, self._branch_name)

    def _inputs_with_ids(self, inputs):
        inputs_with_ids = {}
        for key, values in inputs.items():
            for value in values:
                entry_id = self._hash_id(key, value)
                inputs_with_ids.setdefault(key, []).append({'ids': [entry_id], 'value': value})
        return inputs_with_ids

    def _hash_id(self, key, value):
        return hashlib.sha256((key + str(value)).encode()).hexdigest()

    