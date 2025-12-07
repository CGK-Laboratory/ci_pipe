from external_dependencies.caiman.mocked_cnmf_submodule import MockedCNMFModule


class MockedSourceExtractionModule:
    def __init__(self, file_system=None):
        self._file_system = file_system
        self.cnmf = MockedCNMFModule(file_system)