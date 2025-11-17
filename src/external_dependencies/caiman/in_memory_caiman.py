from external_dependencies.caiman.mocked_motion_correction_submodule import MockMotionCorrectionModule
from external_dependencies.caiman.mocked_movie import MockedMovie


class InMemoryCaiman:
    def __init__(self, file_system=None):
        self._file_system = file_system
        self.motion_correction = MockMotionCorrectionModule(file_system)

    def load(self, fname):
        return MockedMovie(fname, self._file_system)