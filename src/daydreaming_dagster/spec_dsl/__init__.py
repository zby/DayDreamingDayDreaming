"""Tuple-capable experiment DSL package."""

from .compiler import compile_design
from .errors import SpecDslError, SpecDslErrorCode
from .loader import load_spec
from .models import AxisSpec, ExperimentSpec, ReplicateSpec

__all__ = [
    "AxisSpec",
    "ExperimentSpec",
    "ReplicateSpec",
    "compile_design",
    "load_spec",
    "SpecDslError",
    "SpecDslErrorCode",
]
