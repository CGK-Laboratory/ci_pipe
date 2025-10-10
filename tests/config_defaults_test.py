import unittest
import tempfile
import os
import yaml

from ci_pipe.utils.config_defaults import ConfigDefaults


class ConfigDefaultsTestCase(unittest.TestCase):

    def test_load_valid_yaml_file(self):
        # Given
        sample_data = {"key1": "value1", "key2": 42}
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as tmp_file:
            yaml.safe_dump(sample_data, tmp_file)
            tmp_path = tmp_file.name

        # When
        result = ConfigDefaults.load_from_file(tmp_path)

        # Then
        self.assertEqual(result, sample_data)

        os.remove(tmp_path)

    def test_loading_nonexistent_file_raises_error(self):
        # Given
        non_existent_path = "archivo_que_no_existe.yaml"

        # Then
        with self.assertRaises(FileNotFoundError):
            ConfigDefaults.load_from_file(non_existent_path)


if __name__ == "__main__":
    unittest.main()
