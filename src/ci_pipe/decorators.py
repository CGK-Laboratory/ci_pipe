import functools
import inspect
from inspect import Parameter, Signature
from typing import TypeVar, Callable, Protocol, Any, TYPE_CHECKING

try:
    from typing import ParamSpec
except ImportError:
    from typing_extensions import ParamSpec

if TYPE_CHECKING:
    from .pipeline import CIPipe

T_Pipeline = TypeVar("T_Pipeline", bound="CIPipe")
P = ParamSpec("P")

class PipelineBound(Protocol[T_Pipeline]):
    _ci_pipe: T_Pipeline


def step(step_name: str) -> Callable[[Callable[..., dict[str, list]]], Callable[..., "CIPipe"]]:
	"""
	Method decorator for pipeline steps without requiring a nested function.

	Usage:
		@step("My Step Name")
		def my_method(self, inputs, *, opt=1):

	"""

	def decorator(method: Callable[..., dict[str, list]]) -> Callable[..., "CIPipe"]:
		@functools.wraps(method)
		def wrapper(self: PipelineBound["CIPipe"], *call_args: Any, **call_kwargs: Any) -> "CIPipe":
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

		wrapper.__signature__ = inspect.signature(method)
		wrapper.__doc__ = method.__doc__
		wrapper.__annotations__ = method.__annotations__.copy()
		wrapper.__annotations__["return"] = "CIPipe"

		return wrapper

	return decorator
