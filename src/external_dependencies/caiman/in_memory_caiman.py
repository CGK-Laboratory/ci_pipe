from external_dependencies.caiman.mocked_motion_correction_submodule import MockedMotionCorrectionSubModule
from external_dependencies.caiman.mocked_movie import MockedMovie
from external_dependencies.caiman.mocked_source_extraction import MockedSourceExtractionModule


class InMemoryCaiman:
    def __init__(self, file_system=None):
        self._file_system = file_system
        self.motion_correction = MockedMotionCorrectionSubModule(file_system)
        self.source_extraction = MockedSourceExtractionModule(file_system)

    def load(self, fname):
        return MockedMovie(fname, self._file_system)