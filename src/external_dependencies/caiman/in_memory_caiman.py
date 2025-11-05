class MockedMotionCorrect:
    def __init__(self, executed_steps, file_system=None, **motion_params):
        self._file_system = file_system
        self._executed_steps = executed_steps

    def motion_correct(self, **motion_kwargs):
        for step_data in self._executed_steps:
            output = step_data["value"]
            self._file_system.write(output, "")

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

    def MotionCorrect(self, executed_steps, **motion_kwargs):
        return MockedMotionCorrect(executed_steps, self._file_system, **motion_kwargs)

    def CNMFParams(self, params_dict):
        return MockedCNMFParams(params_dict, self._file_system)
