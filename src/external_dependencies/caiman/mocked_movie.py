class MockedMovie:
    def __init__(self, fname, file_system):
        self._fname = fname
        self._file_system = file_system

    def save(self, output_path):
        self._file_system.write(output_path, "")
