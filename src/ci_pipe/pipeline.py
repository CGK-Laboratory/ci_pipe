import hashlib
import inspect

from external_dependencies.file_system.persistent_file_system import PersistentFileSystem
from .errors.defaults_after_step_error import DefaultsAfterStepsError
from .errors.output_key_not_found_error import OutputKeyNotFoundError
from .errors.resume_execution_error import ResumeExecutionError
from .modules.caiman_module import CaimanModule
from .modules.isx_module import ISXModule
from .plotter import Plotter
from .step import Step
from .trace.schema.branch import Branch
from .trace.trace_repository import TraceRepository
from .utils.config_defaults import ConfigDefaults


class CIPipe:
    @classmethod
    def with_videos_from_directory(cls, input, branch_name='Main Branch', outputs_directory='output',
                                   trace_path="trace.json", file_system=PersistentFileSystem(), defaults=None,
                                   defaults_path=None,
                                   isx=None, caiman=None, auto_clean_up_enabled=True):
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
            caiman=caiman,
            auto_clean_up_enabled=auto_clean_up_enabled,
        )

    @classmethod
    def with_multiplane_videos_from_directory(
            cls,
            input_dir,
            *,
            group_name="recording-1",
            branch_name="Main Branch",
            outputs_directory="output",
            trace_path="trace.json",
            file_system=PersistentFileSystem(),
            defaults=None,
            defaults_path=None,
            isx=None,
            caiman=None,
            auto_clean_up_enabled=True,
    ):
        files = file_system.listdir(input_dir)
        inputs = cls._video_inputs_with_extension(files)

        pipeline = cls(
            inputs,
            branch_name=branch_name,
            outputs_directory=outputs_directory,
            trace_path=trace_path,
            file_system=file_system,
            defaults=defaults,
            defaults_path=defaults_path,
            isx=isx,
            caiman=caiman,
            auto_clean_up_enabled=auto_clean_up_enabled,
        )

        # NOTE: Overwriting of input ids, everything in that folder belongs to the same "original video"
        shared_id = pipeline._hash_id("multiplane", group_name)

        for key, entries in pipeline._pipeline_inputs.items():
            for entry in entries:
                entry["ids"] = [shared_id]

        pipeline._build_initial_trace()
        return pipeline

    def __init__(self, inputs, branch_name='Main Branch', outputs_directory='output', trace_path="trace.json",
                 steps=None,
                 file_system=PersistentFileSystem(), defaults=None, defaults_path=None, isx=None,
                 validator=None, caiman=None, auto_clean_up_enabled=True):
        self._pipeline_inputs = self._inputs_with_ids(inputs)
        self._raw_pipeline_inputs = inputs
        self._steps = steps or []
        self._defaults = {}
        self._branch_name = branch_name
        self._auto_clean_up_enabled = auto_clean_up_enabled
        self._outputs_directory = outputs_directory
        self._file_system = file_system
        self._trace_repository = TraceRepository(
            self._file_system, trace_path, validator)
        self._trace = self._trace_repository.load()
        self._plotter = Plotter()
        self._isx = isx
        self._caiman = caiman
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

    def values(self, key):
        outputs = self.output(key)
        return [entry['value'] for entry in outputs]

    def step(self, step_name, step_function, *args, **kwargs):
        self._assert_pipeline_can_resume_execution()
        self._restore_previous_steps_from_trace_if_applicable()
        self._populate_default_params(step_function, kwargs)
        new_step = Step(step_name, self.output, step_function, args, kwargs)
        self._steps.append(new_step)
        self._update_trace_if_available()
        self._try_clean_up_if_enabled()
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
            outputs_directory=self._outputs_directory,
            trace_path=self._trace_repository.trace_path(),
            branch_name=branch_name,
            auto_clean_up_enabled=self._auto_clean_up_enabled,
            steps=self._steps.copy(),
            file_system=self._file_system,
            defaults=self._defaults.copy(),
            isx=self._isx,
            caiman=self._caiman,
        )

        return new_pipe

    def file_system(self):
        return self._file_system

    def set_defaults(self, defaults_path=None, **defaults):
        if self._steps:
            raise DefaultsAfterStepsError()
        self._load_combined_defaults(defaults, defaults_path)
        self._build_initial_trace()
        return self

    def defaults(self):
        return self._defaults.copy()

    def output_directory_for_next_step(self, next_step_name):
        steps_count = len(self._steps)
        step_folder_name = f"{self._branch_name} - Step {steps_count + 1} - {next_step_name}"
        return self._file_system.join(self._outputs_directory, step_folder_name)

    def create_output_directory_for_next_step(self,
                                              next_step_name):
        output_dir = self.output_directory_for_next_step(next_step_name)
        self._file_system.makedirs(output_dir, exist_ok=True)
        return output_dir

    def copy_file_to_output_directory(self, file_path,
                                      next_step_name):
        output_dir = self.output_directory_for_next_step(next_step_name)
        new_file_path = self._file_system.copy2(file_path, output_dir)
        return new_file_path

    def file_in_output_directory(self, file_name, next_step_name):
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

    def clean_up_all(self):
        for key in self.all_keys():
            self.clean_up_key(key)
        return self

    def clean_up_key(self, key):
        old_values = self._old_values_for_key(key)
        old_values = self._exclude_values_used_in_other_branches(key, old_values)
        for old_value in old_values:
            if isinstance(old_value, str) and self._file_system.exists(old_value):
                self._file_system.remove(old_value)
        return self

    def all_keys(self):
        keys = set()
        for step in self._steps:
            for key in step.step_output().keys():
                keys.add(key)
        for key in self._pipeline_inputs.keys():
            keys.add(key)
        return list(keys)

    def assert_trace_is_valid(self):
        return self._trace_repository.validate()

    def make_output_file_path(
            self,
            in_file,
            out_dir,
            suffix,
            ext="tif"
    ):
        base = self._file_system.base_path(in_file)
        stem, _ = self._file_system.split_text(base)
        if suffix:
            stem = f"{stem}-{suffix}"
        new_filename = f"{stem}.{ext}"
        return self._file_system.join(out_dir, new_filename)

    def make_output_file_paths(
            self,
            in_files,
            out_dir,
            suffix,
            ext="tif"
    ):
        return [
            self.make_output_file_path(in_file, out_dir, suffix, ext)
            for in_file in in_files
        ]

    # Modules

    @property
    def isx(self):
        return ISXModule(self._isx, self)

    @property
    def caiman(self):
        return CaimanModule(self._caiman, self)

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

    def _try_clean_up_if_enabled(self):
        if self._auto_clean_up_enabled:
            self.clean_up_all()

    def _old_values_for_key(self, key):
        all_values = []
        for step in self._steps:
            step_output = step.step_output()
            if key in step_output:
                all_values.extend([entry['value'] for entry in step_output[key]])
        current_values = [entry['value'] for entry in self.output(key)]
        old_values = [value for value in all_values if value not in current_values]
        return old_values

    def _exclude_values_used_in_other_branches(self, key, values):
        branches = self._trace.branches()
        current_branch = self._trace.branch_from(self._branch_name)
        values_in_other_branches = set()

        for branch in branches:
            if branch.name() == current_branch.name():
                continue
            for step in branch.steps():
                step_output = step.step_output()
                if key in step_output:
                    values_in_other_branches.update(
                        entry['value'] for entry in step_output[key]
                    )

        filtered_values = [value for value in values if value not in values_in_other_branches]
        return filtered_values
