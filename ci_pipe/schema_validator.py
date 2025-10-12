from collections import defaultdict


class SchemaValidator:
    ROOT_PATH = "$"

    @classmethod
    def new_for(cls, schema):
        return cls(schema)

    def __init__(self, schema):
        self._schema = schema

    def validate(self, json_data):
        schema_keys_by_parent = self._collect_child_keys_by_parent(self._schema.as_dict())
        json_keys_by_parent = self._collect_child_keys_by_parent(json_data)

        for parent_path, schema_child_keys in schema_keys_by_parent.items():
            json_child_keys = json_keys_by_parent.get(parent_path, set())
            missing_keys = schema_child_keys - json_child_keys
            if missing_keys:
                raise ValueError(f"{parent_path}: missing keys {missing_keys}")

        schema_values_by_path = self._collect_values_by_path(self._schema.as_dict())
        json_values_by_path = self._collect_values_by_path(json_data)

        for path, schema_values in schema_values_by_path.items():
            json_values = json_values_by_path.get(path, set())
            missing_values = schema_values - json_values
            if missing_values:
                pretty = ", ".join(repr(v) for v in sorted(missing_values, key=lambda x: (type(x).__name__, str(x))))
                raise ValueError(f"{path}: missing values {pretty}")

        return True

    def _collect_child_keys_by_parent(self, dictionary):
        result = defaultdict(set)

        def walk(node, path):
            if isinstance(node, dict):
                if node:
                    result[path].update(node.keys())
                for k, v in node.items():
                    walk(v, f"{path}.{k}")
            elif isinstance(node, list):
                for item in node:
                    walk(item, path)

        walk(dictionary, self.ROOT_PATH)
        return result

    def _collect_values_by_path(self, dictionary):
        result = defaultdict(set)

        def is_primitive(x):
            return isinstance(x, (str, int, float, bool)) or x is None

        def walk(node, path):
            if is_primitive(node):
                result[path].add(node)
                return

            if isinstance(node, dict):
                for k, v in node.items():
                    walk(v, f"{path}.{k}")
                return

            if isinstance(node, list):
                for item in node:
                    walk(item, path)
                return

        walk(dictionary, self.ROOT_PATH)
        return result
