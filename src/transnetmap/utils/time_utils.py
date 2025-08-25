# -*- coding: utf-8 -*-

"""
Module: time_utils

This module contains utility functions for working with time calculation functions.
It supports dynamic import and validation of functions from the time_functions module.
"""

from transnetmap.analysis.time_functions import time_function_registry


def import_time_function(function_name: str):
    """
    Dynamically import a time calculation function from the time function registry.

    Parameters
    ----------
    function_name : str
        The name of the time function to import.

    Returns
    -------
    callable
        The time calculation function.

    Raises
    ------
    ValueError
        If the function is not found in the registry.
    """
    if function_name not in time_function_registry:
        raise ValueError(
            f"The time function '{function_name}' is not registered. "
            f"Available functions: {', '.join(time_function_registry.keys())}."
        )
    fn = time_function_registry[function_name]
    
    # Validate the function (for additional safety)
    validate_time_function(fn)
    
    return fn


def validate_time_function(fn):
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
    import inspect

    # Ensure the provided object is callable
    if not callable(fn):
        raise TypeError(f"The provided object '{fn}' is not a callable function.")

    # Check the function signature
    signature = inspect.signature(fn)
    params = signature.parameters

    # Check that the function has exactly 4 parameters
    required_params = ['distance', 'v_max', 'acceleration', 'deceleration']
    if list(params.keys()) != required_params:
        raise TypeError(
            f"Function '{fn.__name__}' must have exactly these parameters: "
            f"{', '.join(required_params)}. "
            f"Found: {', '.join(params.keys())}."
        )

    # Check that the return annotation is float
    if signature.return_annotation is not float:
        raise TypeError(
            f"Function '{fn.__name__}' must return a float. "
            f"Found: {signature.return_annotation}."
        )


def register_time_function(fn, name: str):
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
    if name in time_function_registry:
        raise ValueError(f"The function name '{name}' is already registered.")
    
    # Validate the function's signature
    validate_time_function(fn)
    
    # Add the function to the registry
    time_function_registry[name] = fn
    print(f"Function '{name}' registered successfully.")


if __name__ == "__main__":
    """
    Main block for testing time utility functions.

    This section tests the dynamic import and validation of a time calculation
    function from the registry. The example function 'suarm' must already be
    registered in the `time_function_registry` for this test to succeed.

    Test steps:
    1. Dynamically import the 'suarm' function.
    2. Call the function with example values for distance, v_max, and acceleration.
    3. Print the calculated travel time.

    Expected Output:
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
