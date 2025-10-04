"""Tuple-capable experiment DSL package."""

from .compiler import compile_design
from .errors import SpecDslError, SpecDslErrorCode
from .loader import load_spec, parse_spec_mapping
from .models import AxisSpec, ExperimentSpec

__all__ = [
    "AxisSpec",
    "ExperimentSpec",
    "compile_design",
    "load_spec",
    "parse_spec_mapping",
    "SpecDslError",
    "SpecDslErrorCode",
]
