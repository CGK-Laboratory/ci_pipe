class Step:
    def __init__(self, step_name, look_up_function=None, step_function=None, args=None, kwargs=None):
        self._step_name = step_name
        self._step_function = step_function
        self._args = args if args is not None else []
        self._kwargs = kwargs if kwargs is not None else {}

        self._step_outputs = self._step_function(look_up_function, *self._args, **self._kwargs)

    def step_output(self):
        return self._step_outputs

    def name(self):
        return self._step_name

    def output(self):
        return self._step_outputs
    
    def arguments(self):
        return self._kwargs