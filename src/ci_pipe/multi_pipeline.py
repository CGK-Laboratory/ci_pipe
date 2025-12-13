from .modules.multi_module_proxy import MultiModuleProxy
from external_dependencies.file_system.persistent_file_system import PersistentFileSystem
from .pipeline import CIPipe

class MultiCIPipe():

    @classmethod
    def from_pipelines(cls, pipelines_dict):
        multi_cipipe = cls.__new__(cls)
        multi_cipipe.init_with_pipelines(pipelines_dict)
        return multi_cipipe

    def __init__(self, inputs_directory, branch_name='Main Branch', outputs_directory='output', trace_path="trace.json", auto_clean_up_enabled=True,
                 file_system=PersistentFileSystem(), defaults=None, defaults_path=None, isx=None, caiman=None):
        self._pipelines = self._create_pipelines_from_inputs_directory(inputs_directory, branch_name, outputs_directory, trace_path, auto_clean_up_enabled,
                 file_system, defaults, defaults_path, isx, caiman)
        
    def init_with_pipelines(self, pipelines_dict):
        self._pipelines = pipelines_dict

    # Main protocol

    def pipeline(self, name):
        return self._pipelines.get(name)

    def values(self, key):
        values = []
        self.with_pipelines_do(lambda pipeline: values.extend([v for v in pipeline.values(key)]))
        return values

    def branch(self, branch_name):
        branched_pipelines = {}
        for name, pipeline in self._pipelines.items():
            branched_pipelines[name] = pipeline.branch(branch_name)
        return MultiCIPipe.from_pipelines(branched_pipelines)
    
    def set_defaults(self, **defaults):
        self.with_pipelines_do(lambda pipeline: pipeline.set_defaults(**defaults))
        return self
    
    def with_pipelines_do(self, action):
        for pipeline in self._pipelines.values():
            action(pipeline)

    # Modules

    @property
    def isx(self):
        return MultiModuleProxy(self, 'isx')

    @property
    def caiman(self):
        return MultiModuleProxy(self, 'caiman')
    
    # Private methods

    def _create_pipelines_from_inputs_directory(self, inputs_directory, branch_name, outputs_directory, trace_path, auto_clean_up_enabled,
                 file_system, defaults, defaults_path, isx, caiman):
        pipelines = {}
        for dir_entry in file_system.subdirs(inputs_directory):
            dir_path = file_system.join(inputs_directory, dir_entry)
            outputs_directory_for_pipeline = file_system.join(outputs_directory, dir_entry)
            if file_system.exists(dir_path):
                file_system.makedirs(outputs_directory_for_pipeline, exist_ok=True)
                pipeline = CIPipe.with_videos_from_directory(
                    dir_path,
                    branch_name=branch_name,
                    outputs_directory=outputs_directory_for_pipeline,
                    trace_path=file_system.join(outputs_directory_for_pipeline, trace_path),
                    file_system=file_system,
                    defaults=defaults,
                    defaults_path=defaults_path,
                    auto_clean_up_enabled=auto_clean_up_enabled,
                    isx=isx,
                    caiman=caiman
                )
                pipelines[dir_entry] = pipeline
        return pipelines