class Pipeline:
    def __init__(self, inputs, defaults, outputs_directory):
        self.inputs = inputs
        self.defaults = defaults
        self.outputs_directory = outputs_directory

    @classmethod
    def from_dict(cls, data):
        return cls(
            data.get("inputs", {}),
            data.get("defaults", {}),
            data.get("outputs_directory")
        )

    def to_dict(self):
        return {
            "inputs": self.inputs,
            "defaults": self.defaults,
            "outputs_directory": self.outputs_directory
        }
