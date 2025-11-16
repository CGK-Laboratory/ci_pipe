class MockedMotionCorrect:
    def __init__(self, executed_steps, file_system=None, **motion_params):
        self._file_system = file_system
        self._executed_step = executed_steps

    def motion_correct(self, **motion_kwargs):
        output = self._executed_step["value"]
        print("OUTPUT: ", output)
        self._file_system.write(output, "")

    @property
    def mmap_file(self):
        return self._executed_step["value"]

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


    def make_output_file_path(
            self,
            in_file,
            out_dir,
            suffix,
            ext="tiff"
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
            ext="tiff"
    ):

        return [
            self.make_output_file_path(in_file, out_dir, suffix, ext)
            for in_file in in_files
        ]
