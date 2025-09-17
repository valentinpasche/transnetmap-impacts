# -*- coding: utf-8 -*-
"""
Time calculation functions for network analysis.

This module serves as a repository of predefined and user-defined **travel time** functions.

Each function computes travel time for a link based on its length and kinematic parameters.

Notes
-----
- All time functions must have the signature

    (distance, v_max, acceleration, deceleration) -> float

  where:
      
  - ``distance`` is in meters,
  - ``v_max`` is the maximum speed in km/h,
  - ``acceleration`` and ``deceleration`` are in m/s²,
  - The return value is the travel time in **minutes**.

Module attributes
-----------------
TIME_FUNCTION_REGISTERY : dict[str, TimeFunction]

    Dictionary mapping function names to callables available for import.

Examples
--------
Define a function and register it:

>>> def my_time_function(
            distance: float,
            v_max: float,
            acceleration: float,
            deceleration: float
    ) -> float:
        time = distance / (v_max / 3.6) / 60
        return time  # minutes

>>> from transnetmap.utils.time_tools import register_time_function`, import_time_function

>>> register_time_function(my_time_function, "my_time_function")

Use the newly recorded function: 

>>> fn = import_time_function("my_time_function")

>>> fn(distance=1000, v_max=50, acceleration=1.0, deceleration=1.0)  # 1/3 minute
    
Features:

- Predefined travel-time models (e.g. *SUARM* → `suarm`).
- Support for custom, user-defined functions.
- Central registry ``TIME_FUNCTION_REGISTERY`` to list available functions.

Guidelines:

- Signature (strict): ``(distance, v_max, acceleration, deceleration) -> float``.
- Units: ``distance`` in meters; ``v_max`` in km/h; ``acceleration``/``deceleration`` in m/s².
- Return value: **minutes** (recommended rounding: 1 decimal).
- Functions should be pure (no side effects, no I/O).
- Use `transnetmap.utils.time_tools.validate_time_function` to check a function’s signature.
- Use `transnetmap.utils.time_tools.register_time_function` to register a function.
- Retrieve by name with `transnetmap.utils.time_tools.import_time_function`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Dict

import numpy as np

if TYPE_CHECKING:  # noqa: F401
    from transnetmap.utils.time_tools import TimeFunction
    
__all__ = ["my_custom_time_function", "suarm", "TIME_FUNCTION_REGISTERY"]


# -----------------------------------------------------------------------------
# Example custom time function
# -----------------------------------------------------------------------------
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


# -----------------------------------------------------------------------------
# SUARM (Symmetrical Uniformly Accelerated Rectilinear Motion)
# -----------------------------------------------------------------------------
def suarm(distance: float, v_max: float, acceleration: float, deceleration: float) -> float:
    """
    Calculate travel time based on SUARM (Symmetrical Uniformly Accelerated Rectilinear Motion).

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


# -----------------------------------------------------------------------------
# Registry
# -----------------------------------------------------------------------------
TIME_FUNCTION_REGISTERY: Dict[str, TimeFunction] = {
    "my_custom_time_function": my_custom_time_function,  # Example function in the registry
    "suarm": suarm,
}
"""Registry of time functions keyed by name."""