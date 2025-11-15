import hashlib
import inspect

from ci_pipe.errors.defaults_after_step_error import DefaultsAfterStepsError
from ci_pipe.errors.output_key_not_found_error import OutputKeyNotFoundError
from ci_pipe.errors.resume_execution_error import ResumeExecutionError
from ci_pipe.modules.isx_module import ISXModule
from ci_pipe.step import Step
from ci_pipe.trace.schema.branch import Branch
from ci_pipe.trace.trace_repository import TraceRepository
from ci_pipe.utils.config_defaults import ConfigDefaults
from external_dependencies.file_system.persistent_file_system import PersistentFileSystem
from ci_pipe.plotter import Plotter


class CIPipe:
    @classmethod
    def with_videos_from_directory(cls, input, branch_name='Main Branch', outputs_directory='output',
                                   trace_path="trace.json", file_system=PersistentFileSystem(), defaults=None, defaults_path=None,
                                   isx=None):
        files = file_system.listdir(input)
        inputs = cls._video_inputs_with_extension(files)

        return cls(
            inputs,
            branch_name=branch_name,
            outputs_directory=outputs_directory,
            trace_path=trace_path,
            file_system=file_system,
            defaults=defaults,
            defaults_path=defaults_path,
            isx=isx,
        )

    def __init__(self, inputs, branch_name='Main Branch', outputs_directory='output', trace_path="trace.json", steps=None,
                 file_system=PersistentFileSystem(), defaults=None, defaults_path=None, isx=None,
                 validator=None):
        self._pipeline_inputs = self._inputs_with_ids(inputs)
        self._raw_pipeline_inputs = inputs
        self._steps = steps or []
        self._defaults = {}
        self._branch_name = branch_name
        self._outputs_directory = outputs_directory
        self._file_system = file_system
        self._trace_repository = TraceRepository(
            self._file_system, trace_path, validator)
        self._trace = self._trace_repository.load()
        self._plotter = Plotter()
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
        self._assert_pipeline_can_resume_execution()
        self._restore_previous_steps_from_trace_if_applicable()
        self._populate_default_params(step_function, kwargs)
        new_step = Step(step_name, self.output, step_function, args, kwargs)
        self._steps.append(new_step)
        self._update_trace_if_available()
        return self

    def info(self, step_number):
        self._plotter.get_step_info(self._trace_repository.load(), step_number, self._branch_name)

    def trace(self):
        self._plotter.get_all_trace_from_branch(self._trace_repository.load(), self._branch_name)

    def trace_as_json(self):
        return self._trace_repository.load().to_dict()

    def branch(self, branch_name):
        new_pipe = CIPipe(
            self._raw_pipeline_inputs.copy(),
            branch_name=branch_name,
            steps=self._steps.copy(),
            file_system=self._file_system,
            defaults=self._defaults.copy(),
            isx=self._isx,
        )

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

    def file_in_output_directory(self, file_name, next_step_name):  # TODO: analyze if this is the best place for this logic
        output_dir = self.output_directory_for_next_step(next_step_name)
        return self._file_system.join(output_dir, file_name)

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

    def _update_trace_if_available(self):
        if not self._trace:
            return

        branch = self._trace.branch_from(self._branch_name)
        if branch is None:
            branch = Branch(self._branch_name, self._steps)
            self._trace.add_branch(branch)

        amount_of_steps_in_branch = len(branch.steps())
        amount_of_steps_in_pipeline = len(self._steps)
        if amount_of_steps_in_branch < amount_of_steps_in_pipeline:
            self._trace.add_steps(self._steps[amount_of_steps_in_branch:], branch.name())

        self._trace_repository.save(self._trace)

    def _assert_pipeline_can_resume_execution(self):
        if not self._trace_repository.exists() or self._trace.has_empty_steps_for(self._branch_name):
            return

        if not self._is_same_trace_file() or not self._is_same_output_directory():
            raise ResumeExecutionError()

    def _can_pipeline_attempt_to_resume_execution(self):
        # Restore only if this in-memory pipeline has no steps
        # and the trace for this branch already contains steps.
        if self._steps:  # already have steps in memory
            return False
        branch = self._trace.branch_from(self._branch_name)
        return branch is not None and not self._trace.has_empty_steps_for(branch.name())


    def _is_same_trace_file(self):
        # With the current design this repo always points to the same file name ("trace.json"),
        # so "same file" reduces to "the file exists".
        return self._trace_repository.exists()


    def _is_same_output_directory(self):
        current_output_directory = self._trace.to_dict()['pipeline']['outputs_directory']
        return current_output_directory == self._outputs_directory

    def _restore_previous_steps_from_trace_if_applicable(self):
        if not self._can_pipeline_attempt_to_resume_execution():
            return

        trace_content = self.trace_as_json()
        steps = trace_content[self._branch_name]['steps']

        for step in steps:
            # build a Step in a non-executing way and preload outputs
            restored_steps = Step.restored_from_trace(
                name=step['name'],
                outputs=step['outputs'],
                params=step['params']
            )
            self._steps.append(restored_steps)



    def _inputs_with_ids(self, inputs):
        inputs_with_ids = {}
        for key, values in inputs.items():
            for value in values:
                entry_id = self._hash_id(key, value)
                inputs_with_ids.setdefault(key, []).append({'ids': [entry_id], 'value': value})
        return inputs_with_ids

    def _hash_id(self, key, value):
        return hashlib.sha256((key + str(value)).encode()).hexdigest()
