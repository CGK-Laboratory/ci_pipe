import unittest
import yaml

from src.ci_pipe.utils.config_defaults import ConfigDefaults
from src.external_dependencies.file_system.in_memory_file_system import InMemoryFileSystem


class ConfigDefaultsTestCase(unittest.TestCase):

    def test_load_valid_yaml_file(self):
        # Given
        fs = InMemoryFileSystem()
        sample_data = {"key1": "value1", "key2": 42}
        yaml_content = yaml.safe_dump(sample_data)
        config_path = "config.yaml"
        fs.write(config_path, yaml_content)

        # When
        result = ConfigDefaults.load_from_file(config_path, file_system=fs)

        # Then
        self.assertEqual(result, sample_data)

    def test_loading_nonexistent_file_raises_error(self):
        # Given
        fs = InMemoryFileSystem()
        non_existent_path = "archivo_que_no_existe.yaml"

        # Then
        with self.assertRaises(FileNotFoundError):
            ConfigDefaults.load_from_file(non_existent_path, file_system=fs)


if __name__ == "__main__":
    unittest.main()
