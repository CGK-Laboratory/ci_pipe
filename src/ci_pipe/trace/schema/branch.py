from typing import List

from ...step import Step


class Branch:
    def __init__(self, name, steps):
        self._name = name
        self._steps = list(steps) if steps is not None else []

    @classmethod
    def from_dict(cls, name, data):
        serialized_steps = data.get("steps", [])
        return cls(name, list(serialized_steps))

    def to_dict(self):
        return {"steps": self._steps}

    def add_steps(self, steps: List[Step]):
        self._steps.extend(steps)

    def name(self):
        return self._name

    def steps(self):
        return self._steps
