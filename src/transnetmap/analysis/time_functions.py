# -*- coding: utf-8 -*-

"""
Module: time_functions

This module contains time calculation functions for network analysis. It serves as a repository for predefined 
and user-defined travel time calculation functions. Each function calculates the travel time for a link 
based on its length and other parameters, such as speed and acceleration.

Features:
---------
1. Predefined functions for common travel time calculations (e.g., SUARM).
2. Support for custom user-defined functions, which can be dynamically registered and validated.
3. Centralized registry (`time_function_registry`) to manage available functions.

Guidelines:
-----------
- Define your function to calculate travel time based on **4 required parameters**:
    1. `distance` : float (Length of the link in meters).
    2. `v_max` : float (Maximum speed in km/h).
    3. `acceleration` : float (Acceleration in m/s²).
    4. `deceleration` : float (Deceleration in m/s²).
- You must define all 4 parameters in your function's signature, even if some are unused.
- The output travel time must be in **minutes** (float).
- Ensure your function is deterministic and well-documented for consistency and reproducibility.

Function Format:
----------------
All functions must conform to the following signature:
    def function_name(distance: float, v_max: float, acceleration: float, deceleration: float) -> float:
        - `distance` : The length of the link [meters].
        - `v_max` : Maximum speed [km/h].
        - `acceleration` : Acceleration [m/s²].
        - `deceleration` : Deceleration [m/s²].
        - Returns a float representing the travel time [minutes].

Example:
--------
Define your function:
    def my_time_function(distance: float, v_max: float, acceleration: float, deceleration: float) -> float:
        return distance / v_max / 60  # Example logic for calculating travel time

Register your function:
    from transnetmap.utils.time_utils import register_time_function
    register_time_function(my_time_function, "my_time_function")

Test the function dynamically:
    from transnetmap.utils.time_utils import import_time_function
    time_function = import_time_function("my_time_function")
    travel_time = time_function(distance=1000, v_max=50, acceleration=1.0, deceleration=1.0)  # Result: 20.0 minutes

Registry:
---------
The module includes a central `time_function_registry` dictionary, which holds all registered functions.
    time_function_registry = {
        "suarm": suarm,
        "my_custom_time_function": my_custom_time_function,
        ...
    }

Notes:
------
- The `time_function_registry` ensures that all functions are accessible dynamically for import and use.
- Users should ensure their functions are deterministic and properly tested before adding them to the registry.
- This module supports extensibility for custom user needs while maintaining compatibility with the network analysis framework.
"""

# Import necessary modules
import numpy as np


# Example custom time function
def my_custom_time_function(distance: float, v_max: float, acceleration: float, deceleration: float) -> float:
    """
    Example custom time function for testing.

    Parameters
    ----------
    distance : float
        Length of the section [m].
    v_max : float
        Maximum speed, constant speed [km/h].
    acceleration : float
        Average constant acceleration [m/s²].
    deceleration : float
        Average constant deceleration [m/s²].

    Returns
    -------
    float
        Total travel time [min].
    """
    # Simple calculation using only distance and speed
    travel_time = distance / (v_max / 3.6) / 60  # Convert speed to m/s and calculate time in minutes
    return round(travel_time, 1)  # Round to 1 decimal place    


def suarm(distance: float, v_max: int, acceleration: float, deceleration: float) -> float:
    """
    Calculates travel time based on SUARM (Symmetrical Uniformly Accelerated Rectilinear Motion).
    
    This function computes the travel time for a link based on the given distance, 
    maximum speed, acceleration and deceleration.

    Parameters
    ----------
    distance : float
        Length of the section [m].
    v_max : int
        Maximum speed, constant speed [km/h].
    acceleration : float
        Average constant acceleration [m/s²].
    deceleration : float
        Average constant deceleration [m/s²].


    Returns
    -------
    float
        Total travel time [min].
    
    Notes
    -----
    - The input distance should be in meters.
    - Deceleration can be positive or negative (the absolute value is used).
    - The output travel time is rounded to 1 decimal place and returned in minutes.
    """
    v = v_max / 3.6  # [m/s]
    a = acceleration  # [m/s²]
    d = np.absolute(deceleration)  # [m/s²]
    ta_max = v / a  # [s]
    td_max = v / d  # [s]
    d_ad_max = (ta_max + td_max) * (v / 2)  # [m]

    if distance > d_ad_max:
        t_vc = (distance - d_ad_max) / v  # [s]
        total_time = t_vc + ta_max + td_max  # [s]
    else:
        td = np.sqrt(2 * distance / ((d**2 / a) + d))  # [s]
        ta = td * (d / a)  # [s]
        total_time = ta + td  # [s]

    return round(total_time / 60, 1)  # [min]


# Add your custom functions here and register them in the registry below.
time_function_registry = {
    "my_custom_time_function": my_custom_time_function, # Example function in the registry
    "suarm": suarm
}

