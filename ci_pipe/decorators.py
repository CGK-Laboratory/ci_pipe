import functools


def step(step_name):
	"""
	Method decorator for pipeline steps without requiring a nested function.

	Usage:
		@step("My Step Name")
		def my_method(self):
			return {...} # step output

	The decorator registers a step function that will call the method with
	the inputs resolver at execution time, and returns the pipeline for chaining.
	"""

	def decorator(method):
		@functools.wraps(method)
		def wrapper(self, *args, **kwargs):
			if not hasattr(self, "_ci_pipe") or self._ci_pipe is None:
				raise RuntimeError("Decorator @step requires 'self._ci_pipe' to be set.")

			def step_function(inputs):
				return method(self, inputs, *args, **kwargs)

			return self._ci_pipe.step(step_name, step_function)

		return wrapper

	return decorator
