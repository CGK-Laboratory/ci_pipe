from external_dependencies.caiman.mocked_motion_correct import MockedMotionCorrect


class MockedMotionCorrectionSubModule:
    def __init__(self, file_system):
        self._file_system = file_system

    def MotionCorrect(
            self,
            fname,
            strides=(48, 48),
            overlaps=(24, 24),
            max_shifts=(6, 6),
            max_deviation_rigid=3,
            pw_rigid=True,
            shifts_opencv=True,
            border_nan='copy'
    ):
        return MockedMotionCorrect(
            file_system=self._file_system,
            fname=fname,
            strides=strides,
            overlaps=overlaps,
            max_shifts=max_shifts,
            max_deviation_rigid=max_deviation_rigid,
            pw_rigid=pw_rigid,
            shifts_opencv=shifts_opencv,
            border_nan=border_nan
        )