class Step:
    def __init__(self, step_name, look_up_function=None, step_function=None, args=None, kwargs=None, step_outputs=None):
        self._step_name = step_name
        self._step_function = step_function
        self._args = args if args is not None else []
        self._kwargs = kwargs if kwargs is not None else {}

        # TODO: Handle step_outputs more gracefully?
        if step_outputs is not None:
            self._step_outputs = step_outputs
        elif self._step_function is not None:
            self._step_outputs = self._step_function(look_up_function, *self._args, **self._kwargs)
        else:
            self._step_outputs = {}

    @classmethod
    def from_dict(cls, data):
        if data is None:
            return None
        return cls(
            data.get("name"),
            look_up_function=None,
            step_function=None,  # Don't execute the function during deserialization
            args=None,
            kwargs=data.get("params"),
            step_outputs=data.get("outputs"),
        )

    @classmethod
    def restored_from_trace(cls, name, outputs, params):
        obj = cls.__new__(cls)
        obj._name = name
        obj._output_lookup = None
        obj._fn = None
        obj._args = ()
        obj._kwargs = params or {}
        obj._step_outputs = outputs  # Preload, do not execute
        return obj

    def step_output(self):
        return self._step_outputs

    def name(self):
        return self._step_name

    def output(self):
        return self._step_outputs

    def arguments(self):
        return self._kwargs

    def to_dict(self):
        return {
            "name": self.name(),
            "outputs": self.output(),
            "params": self.arguments()
        }
