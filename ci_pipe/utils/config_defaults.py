import yaml


class ConfigDefaults:
    """
    Responsible for loading default configuration values from a file.
    Currently supports YAML.
    """

    @staticmethod
    def load_from_file(path, file_system):
        """
        Loads defaults from a configuration file and returns them as a dictionary.

        Args:
            path (str): Path to the configuration file.

        Returns:
            dict: Parsed configuration data.

        Raises:
            FileNotFoundError: If the file does not exist.
            ValueError: If the file cannot be parsed or does not contain a dictionary.
        """
        if not file_system.exists(path):
            raise FileNotFoundError(f"Config file not found: {path}")

        try:
            content = file_system.read(path)
            data = yaml.safe_load(content) or {}
            if not isinstance(data, dict):
                raise ValueError(f"Config file {path} must contain a dictionary at the top level.")
            return data
        except yaml.YAMLError as e:
            raise ValueError(f"Error parsing YAML config: {e}")
