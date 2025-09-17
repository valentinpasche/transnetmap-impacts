# -*- coding: utf-8 -*-
"""
Time utilities for selecting, validating, and registering time calculation functions.

This module provides small helpers to work with the registry defined in
``transnetmap.analysis.time_functions``. It lets you:
    
- import a registered time function by name (`import_time_function`),
- validate that a function matches the expected signature (`validate_time_function`),
- register a new function into the registry (`register_time_function`).

<span style="font-size: small;">*Module renamed from ``transnetmap.utils.time`` to ``transnetmap.utils.time_tools``
to avoid collisions with the Python standard library ``time`` module.*</span>

Notes
-----
* Expected function signature (strict): ``(distance, v_max, acceleration, deceleration) -> float``.
"""

from __future__ import annotations

from typing import Callable, Protocol, get_type_hints
import inspect

from transnetmap.analysis.time_functions import TIME_FUNCTION_REGISTERY

__all__ = [
    "TimeFunction",
    "import_time_function",
    "validate_time_function",
    "register_time_function",
]


# -----------------------------------------------------------------------------
# Protocol (expected callable signature)
# -----------------------------------------------------------------------------
class TimeFunction(Protocol):
    """Protocol for time functions computing a travel time."""

    def __call__(
        self,
        *,
        distance: float,
        v_max: float,
        acceleration: float,
        deceleration: float,
    ) -> float:
        """
        Compute travel time.

        Parameters
        ----------
        distance : float
            Length of the link in meters.
        v_max : float
            Maximum speed in km/h.
        acceleration : float
            Acceleration in m/s².
        deceleration : float
            Deceleration in m/s².

        Returns
        -------
        float
            Computed travel time.
        """
        ...


# -----------------------------------------------------------------------------
# Import helper
# -----------------------------------------------------------------------------
def import_time_function(function_name: str) -> TimeFunction:
    """
    Dynamically import a time calculation function from the time function registry.

    Parameters
    ----------
    function_name : str
        The name of the time function to import.

    Returns
    -------
    TimeFunction
        The time calculation function.

    Raises
    ------
    ValueError
        If the function is not found in the registry.
    """
    if function_name not in TIME_FUNCTION_REGISTERY:
        raise ValueError(
            f"The time function '{function_name}' is not registered. "
            f"Available functions: {', '.join(TIME_FUNCTION_REGISTERY.keys())}."
        )
    fn = TIME_FUNCTION_REGISTERY[function_name]

    # Validate the function (for additional safety)
    validate_time_function(fn)

    return fn  # type: ignore[return-value]  # validated at runtime


# -----------------------------------------------------------------------------
# Validator
# -----------------------------------------------------------------------------
def validate_time_function(fn: Callable[..., float]) -> None:
    """
    Validate a time calculation function to ensure it matches the expected signature.

    Parameters
    ----------
    fn : callable
        The function to validate.

    Raises
    ------
    TypeError
        If the function is not callable, or does not match the expected format.
    """
    # Ensure the provided object is callable
    if not callable(fn):
        raise TypeError(f"The provided object '{fn}' is not a callable function.")

    # Check the function signature (names & order)
    signature = inspect.signature(fn)
    params = signature.parameters

    required_params = ["distance", "v_max", "acceleration", "deceleration"]
    if list(params.keys()) != required_params:
        raise TypeError(
            f"Function '{fn.__name__}' must have exactly these parameters: "
            f"{', '.join(required_params)}. "
            f"Found: {', '.join(params.keys())}."
        )

    # Resolve annotations (handles 'from __future__ import annotations')
    if signature.return_annotation is inspect.Signature.empty:
        raise TypeError(
            f"Function '{fn.__name__}' must have a return annotation 'float'."
        )

    hints = get_type_hints(fn, globalns=getattr(fn, "__globals__", {}), include_extras=False)
    ret = hints.get("return")

    if ret is not float:
        raise TypeError(
            f"Function '{fn.__name__}' must return a float. Found: {ret!r}."
        )

    return None



# -----------------------------------------------------------------------------
# Registry helper
# -----------------------------------------------------------------------------
def register_time_function(fn: Callable[..., float], name: str) -> None:
    """
    Register a new time calculation function into the registry.

    Parameters
    ----------
    fn : callable
        The time function to register.
    name : str
        The name to assign to the function in the registry.

    Raises
    ------
    ValueError
        If the function name is already registered.
    TypeError
        If the function does not match the expected format.
    """
    if name in TIME_FUNCTION_REGISTERY:
        raise ValueError(f"The function name '{name}' is already registered.")

    # Validate the function's signature
    validate_time_function(fn)

    # Add the function to the registry
    TIME_FUNCTION_REGISTERY[name] = fn  # type: ignore[index]
    print(f"Function '{name}' registered successfully.")

    return None


# -----------------------------------------------------------------------------
# Manual test (no side effects at import)
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    """
    Main block for testing time utility functions.

    This section tests the dynamic import and validation of a time calculation
    function from the registry. The example function 'suarm' must already be
    registered in the ``time_function_registry`` for this test to succeed.

    Test steps
    ----------
    1. Dynamically import the 'suarm' function.
    2. Call the function with example values for distance, v_max, acceleration and deceleration.
    3. Print the calculated travel time.

    Expected Output
    ---------------
    - "Function imported successfully!"
    - "Calculated travel time: <value> min"
    """
    try:
        # Import a registered time function
        time_func = import_time_function("suarm")
        print("Function imported successfully!")

        # Test the function with example parameters
        travel_time = time_func(distance=10000, v_max=120, acceleration=1.5, deceleration=2)
        print(f"Calculated travel time: {travel_time} min")

    except ValueError as e:
        print(f"Error: {e}")
