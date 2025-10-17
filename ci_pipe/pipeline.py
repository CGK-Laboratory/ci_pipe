import hashlib
import inspect

from ci_pipe.errors.defaults_after_step_error import DefaultsAfterStepsError
from ci_pipe.errors.output_key_not_found_error import OutputKeyNotFoundError
from ci_pipe.modules.isx_module import ISXModule
from ci_pipe.step import Step
from ci_pipe.trace.schema.branch import Branch
from ci_pipe.trace.trace_repository import TraceRepository
from ci_pipe.utils.config_defaults import ConfigDefaults
from external_dependencies.file_system.persistent_file_system import PersistentFileSystem


class CIPipe:
    RESUME_EXECUTION_ERROR_MESSAGE = "Cannot resume execution without the same trace file and output directory"

    @classmethod
    def with_videos_from_directory(cls, input, branch_name='Main Branch', outputs_directory='output',
                                   steps=None, file_system=PersistentFileSystem(), defaults=None, defaults_path=None,
                                   plotter=None, isx=None):
        files = file_system.listdir(input)
        inputs = cls._video_inputs_with_extension(files)
        return cls(
            inputs,
            branch_name=branch_name,
            outputs_directory=outputs_directory,
            steps=steps,
            file_system=file_system,
            defaults=defaults,
            defaults_path=defaults_path,
            plotter=plotter,
            isx=isx,
        )

    def __init__(self, inputs, branch_name='Main Branch', outputs_directory='output', steps=None,
                 file_system=PersistentFileSystem(), defaults=None, defaults_path=None, plotter=None, isx=None, validator=None):
        self._pipeline_inputs = self._inputs_with_ids(inputs)
        self._raw_pipeline_inputs = inputs
        self._steps = steps or []
        self._defaults = {}
        self._branch_name = branch_name
        self._outputs_directory = outputs_directory
        self._file_system = file_system
        self._trace_repository = TraceRepository(self._file_system, "trace.json",
                                                 validator)
        self._trace = self._trace_repository.load()
        self._plotter = plotter
        self._isx = isx
        self._load_combined_defaults(defaults, defaults_path)
        self._build_initial_trace()

    # Main protocol

    def output(self, key):
        for step in reversed(self._steps):
            if key in step.step_output():
                return step.step_output()[key]
        if key in self._pipeline_inputs:
            return self._pipeline_inputs[key]
        raise OutputKeyNotFoundError(key)

    def step(self, step_name, step_function, *args, **kwargs):
        self._populate_default_params(step_function, kwargs)
        new_step = Step(step_name, self.output, step_function, args, kwargs)
        self._steps.append(new_step)
        self._update_trace_if_trace_builder_provided()
        return self

    def info(self, step_number):
        self._plotter.get_step_info(self._trace_repository.load(), step_number, self._branch_name)

    def trace(self):
        self._plotter.get_all_trace_from_branch(self._trace_repository.load(), self._branch_name)

    def branch(self, branch_name):
        new_pipe = CIPipe(
            self._raw_pipeline_inputs.copy(),
            branch_name=branch_name,
            steps=self._steps.copy(),
            file_system=self._file_system,
            defaults=self._defaults.copy(),
            plotter=self._plotter,
            isx=self._isx, )
        return new_pipe

    def set_defaults(self, defaults_path=None, **defaults):
        if self._steps:
            raise DefaultsAfterStepsError()
        self._load_combined_defaults(defaults, defaults_path)
        self._build_initial_trace()
        return self

    def output_directory_for_next_step(self, next_step_name):  # TODO: analyze if this is the best place for this logic
        steps_count = len(self._steps)
        step_folder_name = f"{self._branch_name} - Step {steps_count + 1} - {next_step_name}"
        return self._file_system.join(self._outputs_directory, step_folder_name)

    def create_output_directory_for_next_step(self,
                                              next_step_name):  # TODO: analyze if this is the best place for this logic
        output_dir = self.output_directory_for_next_step(next_step_name)
        self._file_system.makedirs(output_dir, exist_ok=True)
        return output_dir

    def copy_file_to_output_directory(self, file_path,
                                      next_step_name):  # TODO: analyze if this is the best place for this logic
        output_dir = self.output_directory_for_next_step(next_step_name)
        new_file_path = self._file_system.copy2(file_path, output_dir)
        return new_file_path

    def associate_keys_by_id(self, key, key_to_associate):
        key_inputs = self.output(key)
        key_to_associate_inputs = self.output(key_to_associate)

        pairs = [
            (key_input['ids'], key_input['value'], key_to_associate_input['value'])
            for key_input in key_inputs
            for key_to_associate_input in key_to_associate_inputs
            if key_input['ids'] == key_to_associate_input['ids']
        ]

        return pairs

    def assert_trace_is_valid(self):
        return self._trace_repository.validate()

    # Modules

    @property
    def isx(self):
        return ISXModule(self._isx, self)

    # Private methods

    @classmethod
    def _video_inputs_with_extension(cls, files):
        inputs = {}

        for file in files:
            ext = file.split('.')[-1] if '.' in file else 'unknown'  # TODO: Throw error/handle if no extension
            key = f'videos-{ext}'
            inputs.setdefault(key, []).append(file)

        return inputs

    def _load_defaults(self, defaults):
        for defaults_key, defaults_value in defaults.items():
            self._defaults[defaults_key] = defaults_value

    def _load_combined_defaults(self, defaults, defaults_path):
        loaded_defaults = {}

        if defaults_path:
            file_defaults = ConfigDefaults.load_from_file(defaults_path, self._file_system)
            loaded_defaults.update(file_defaults)
        if defaults and isinstance(defaults, dict):
            loaded_defaults.update(defaults)

        self._load_defaults(loaded_defaults)

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
        if self._steps:
            return

        existing_branch = self._trace.branch_from(self._branch_name)
        if existing_branch and existing_branch.steps():
            return
        self._trace.set_pipeline(self._pipeline_inputs, self._defaults, self._outputs_directory)

        if existing_branch is None:
            self._trace.add_branch(Branch(self._branch_name, []))

        self._trace_repository.save(self._trace)

    def _update_trace_if_trace_builder_provided(self):
        if not self._trace:
            return

        steps_snapshot = []
        for idx, step in enumerate(self._steps, 1):
            steps_snapshot.append({
                "index": idx,
                "name": step.name(),
                "params": step.arguments(),
                "outputs": step.step_output()
            })

        branch = self._trace.branch_from(self._branch_name)
        if branch is None:
            branch = Branch(self._branch_name, steps_snapshot)
            self._trace.add_branch(branch)
        else:
            self._trace.add_steps(steps_snapshot, branch.name())

        self._trace_repository.save(self._trace)

    def _inputs_with_ids(self, inputs):
        inputs_with_ids = {}
        for key, values in inputs.items():
            for value in values:
                entry_id = self._hash_id(key, value)
                inputs_with_ids.setdefault(key, []).append({'ids': [entry_id], 'value': value})
        return inputs_with_ids

    def _hash_id(self, key, value):
        return hashlib.sha256((key + str(value)).encode()).hexdigest()
