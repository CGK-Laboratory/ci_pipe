class MockedMotionCorrect:
    def __init__(self, filenames, file_system=None):
        self._file_system = file_system
        self._filenames = filenames

    def motion_correct(self,):
        for file_name in self._filenames:
            print("FILE: ", self._file_system.read(file_name))
            self._file_system.write(file_name, "")

class MockedCNMFParams:
    def __init__(self, params_dict, file_system=None):
        self._file_system = file_system
        self._params_dict = params_dict

    @property
    def motion(self):
        motion_correct_params = {}
        general_param_keys = ["fr", "decay_time", "dxy"]
        motion_correct_param_keys = ["strides", "overlaps", "max_shifts", "max_deviation_shift", "pw_rigid"]
        for key, value in self._params_dict.items():
            if key in general_param_keys:
                motion_correct_params[key] = value
            elif key in motion_correct_param_keys:
                motion_correct_params[key] = [value]
        return motion_correct_params

class InMemoryCaiman:
    def __init__(self, file_system=None):
        self._file_system = file_system

    def MotionCorrect(self, file_name):
        return MockedMotionCorrect(file_name, self._file_system)

    def CNMFParams(self, params_dict):
        return MockedCNMFParams(params_dict, self._file_system)
