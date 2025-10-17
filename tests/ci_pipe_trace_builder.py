import json


class CIPipeTraceBuilder:
    def __init__(self):
        self._schema = {
            "pipeline": {
                "inputs": {},
                "defaults": {},
                "outputs_directory": ""
            },
        }

    def __repr__(self):
        return json.dumps(self._schema, indent=4)

    def as_dict(self):
        return self._schema

    def with_inputs(self, inputs_json):
        self._schema["pipeline"]["inputs"] = inputs_json
        return self

    def with_defaults(self, defaults_json):
        self._schema["pipeline"]["defaults"] = defaults_json
        return self

    def with_outputs_directory(self, outputs_directory):
        self._schema["pipeline"]["outputs_directory"] = outputs_directory
        return self

    def with_empty_branch(self, branch_name ="Main Branch"):
        self._schema[branch_name] = {"steps": []}
        return self

    def with_steps_in_branch(self, steps_json, branch_name="Main Branch"):
        self._schema[branch_name]["steps"].append(steps_json)
        return self
