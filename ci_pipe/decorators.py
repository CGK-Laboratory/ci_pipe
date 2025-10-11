import functools
import inspect
from inspect import Parameter, Signature


def step(step_name):
	"""
	Method decorator for pipeline steps without requiring a nested function.

	Usage:
		@step("My Step Name")
		def my_method(self, inputs, *, opt=1):

	"""

	def decorator(method):
		@functools.wraps(method)
		def wrapper(self, *call_args, **call_kwargs):
			if not hasattr(self, "_ci_pipe") or self._ci_pipe is None:
				raise RuntimeError("Decorator @step requires 'self._ci_pipe' to be set.")

			bound_method = method.__get__(self, type(self))

			def step_function(inputs, *s_args, **s_kwargs):
				return bound_method(inputs, *s_args, **s_kwargs)

			orig_sig = inspect.signature(method)
			new_params = []
			for name, p in orig_sig.parameters.items():
				if name == 'self':
					continue
				if name == 'inputs':
					new_params.append(Parameter('inputs', kind=Parameter.POSITIONAL_OR_KEYWORD))
				else:
					default = p.default if p.default is not inspect._empty else inspect._empty
					new_params.append(Parameter(name, kind=Parameter.KEYWORD_ONLY, default=default))
			step_function.__signature__ = Signature(new_params)

			return self._ci_pipe.step(step_name, step_function, *call_args, **call_kwargs)

		return wrapper

	return decorator
