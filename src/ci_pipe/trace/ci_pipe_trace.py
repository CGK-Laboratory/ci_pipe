from typing import List

from ci_pipe.step import Step
from ci_pipe.trace.schema.branch import Branch
from ci_pipe.trace.schema.pipeline import Pipeline


class CIPipeTrace:
    def __init__(self, pipeline=None, branches=None):
        self._pipeline = pipeline or Pipeline({}, {}, None)
        self._branches = branches or {}

    @classmethod
    def from_dict(cls, data):
        pipeline = Pipeline.from_dict(data.get("pipeline", {}))
        branches = {
            name: Branch.from_dict(name, payload)
            for name, payload in data.items() if name != "pipeline"
        }
        return cls(pipeline, branches)

    def to_dict(self):
        return {
            "pipeline": self._pipeline.to_dict(),
            **{name: branch.to_dict() for name, branch in self._branches.items()},
        }

    def set_pipeline(self, inputs, defaults, outputs_directory):
        self._pipeline = Pipeline(inputs, defaults or {}, outputs_directory)

    def add_branch(self, branch: Branch):
        self._branches[branch.name()] = branch

    def add_steps(self, steps, branch_name):
        if branch_name not in self._branches:
            self._branches[branch_name] = Branch(branch_name, [])
        self._branches[branch_name].add_steps(steps)

    def branch_from(self, branch_name) -> Branch:
        return self._branches.get(branch_name)
    
    def branches(self) -> List[Branch]:
        return list(self._branches.values())

    def steps_from(self, branch_name) -> List[Step]:
        branch = self.branch_from(branch_name)
        if branch:
            return branch.steps()
        return []

    def has_empty_steps_for(self, branch_name):
        steps = self.steps_from(branch_name)
        return len(steps) == 0