from typing import List

from ci_pipe.step import Step


class Branch:
    def __init__(self, name, steps):
        self._name = name
        self._steps = steps

    @classmethod
    def from_dict(cls, name, data):
        serialized_steps = data.get("steps", [])
        steps = [Step.from_dict(serialized_step) for serialized_step in serialized_steps]
        return cls(name, steps)

    def to_dict(self):
        return {
            "steps": [
                {
                    "index": index,
                    "name": step.name(),
                    "params": step.arguments(),
                    "outputs": step.step_output(),
                }
                for index, step in enumerate(self._steps, start=1)
            ]
        }

    def add_steps(self, steps: List[Step]):
        self._steps.extend(steps)

    def name(self):
        return self._name

    def steps(self):
        return self._steps
