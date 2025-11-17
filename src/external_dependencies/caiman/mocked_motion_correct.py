class MockedMotionCorrect:
    def __init__(
            self,
            fname,
            strides=(48, 48),
            overlaps=(24, 24),
            max_shifts=(6, 6),
            max_deviation_rigid=3,
            pw_rigid=True,
            shifts_opencv=True,
            border_nan='copy',
            file_system=None):
        self._file_system = file_system
        self._fname = fname
        self._strides = strides
        self._overlaps = overlaps
        self._max_shifts = max_shifts
        self._max_deviation_rigid = max_deviation_rigid
        self._pw_rigid = pw_rigid
        self._shifts_opencv = shifts_opencv
        self._border_nan = border_nan

    def motion_correct(self, save_movie=True, **motion_kwargs):
        output = self._fname
        self._file_system.write(output, "")

    @property
    def motion_correction(self):
        return self

    @property
    def mmap_file(self):
        return [self._fname]
