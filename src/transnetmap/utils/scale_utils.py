import numpy as np
from typing import List, Tuple


def compute_boolean_scale_type(data: np.ndarray) -> Tuple[List[float], float, float]:
    """
    Computes a 4-bin threshold scale for the `transport_type` analysis type (with-NTS vs extend-NTS).
    Used for discrete boolean values defined in `dct_type`.

    Logic:
    - Builds bins around the expected values (e.g. 6 and 7)
    - Creates [val - 0.5, val + 0.5] around each value
    - Merges and sorts the edges
    - Adds an artificial bin before if the values are consecutive (e.g. [6, 7] → add 4.5)

    Parameters
    ----------
    data : np.ndarray
        Numpy array containing only the numeric codes for transport types.
        Must match the values in dct_type['with-NTS'] and dct_type['extend-NTS'].

    Returns
    -------
    Tuple[List[float], float, float]
        - 4-bin threshold scale (for Folium)
        - vmin (for Branca)
        - vmax (for Branca)

    Raises
    ------
    ValueError
        If the dataset contains values outside the expected transport type pair.
    """
    from transnetmap.utils.dct import dct_type

    expected_vals = {dct_type['with-NTS'], dct_type['extend-NTS']}
    unique_vals = set(np.unique(data))

    if unique_vals != expected_vals:
        raise ValueError(f"⚠️ Expected only values {expected_vals} for `transport_type`, got: {unique_vals}")

    min_val, max_val = float(min(expected_vals)), float(max(expected_vals))

    # Step 1: build half-open bins [val - 0.5, val + 0.5] around each value
    edges = set()
    for val in expected_vals:
        edges.add(val - 0.5)
        edges.add(val + 0.5)

    scale = sorted(edges)

    # Step 2: if there are only 3 bins (2 classes), add a fake bin before for folium
    if len(scale) == 3:
        scale.insert(0, min_val - 1.5)  # → now 4 bins (3 color classes)

    # Step 3: safety check
    if len(scale) != 4:
        print(f"⚠️ WARNING: scale for `transport_type` does not contain exactly 4 thresholds: {scale}")

    return scale, min_val, max_val


def compute_discrete_scale_changes(data: np.ndarray) -> Tuple[List[float], float, float]:
    """
    Computes a discrete scale for the `changes` analysis type (natural integers, always ≥ 0).
    Automatically ensures proper bin rendering even for small ranges (e.g. only 1 or 2 values).

    Logic:
    - Creates bins centered around each integer value using ±0.5
    - Adds extra bins to avoid errors when too few categories are present

    Parameters
    ----------
    data : np.ndarray
        Array of integers (np.int8) representing number of route changes.

    Returns
    -------
    Tuple[List[float], float, float]
        - Discrete bin edges (List of float)
        - vmin (for Branca)
        - vmax (for Branca)

    Raises
    ------
    ValueError
        If data contains negative values.
    """    
    unique_values = np.unique(data)
    
    min_val = float(min(unique_values))
    max_val = float(max(unique_values))
    
    if min_val < 0:
        raise ValueError("⚠️ No valid data found. Check dataset integrity.")
    
    dynamic_scale = np.arange(min_val - 0.5, max_val + 1.5, 1).tolist()
    
    # Force full bin rendering, len(dynamic_scale) is always >= 2
    match len(dynamic_scale):
        case 2:
            dynamic_scale.append(max_val + 1.5)
            dynamic_scale.append(max_val + 2.5)
        case 3:
            dynamic_scale.append(max_val + 1.5)
        case _:
            pass
    
    return dynamic_scale, min_val, max_val


def compute_jenks_dynamic_scale(data: np.ndarray, bins: int) -> Tuple[List[float], float, float]:
    """
    Computes a dynamic scale using Jenks Natural Breaks for continuous data.

    Behavior:
    - Falls back to `linspace` if too few unique values are present
    - Applies adaptive rounding to produce readable thresholds
    - Ensures that 0 is included if the range crosses zero

    Rounding logic:
    - Range < 1   → round to nearest 0.01
    - Range < 10  → round to nearest 0.1
    - Range < 100 → round to nearest 1
    - Range ≥ 100 → round to nearest 5

    Parameters
    ----------
    data : np.ndarray
        Flattened array of continuous values.
    bins : int
        Number of bins to compute. Must be ≥ 4.

    Returns
    -------
    Tuple[List[float], float, float]
        - Rounded threshold scale (List of float)
        - vmin (for Branca)
        - vmax (for Branca)
    """
    min_val = float(data.min())
    max_val = float(data.max())
    unique_values = np.unique(data)

    # Fallback to linspace if not enough distinct values
    if unique_values.size < bins:
        natural_breaks = np.linspace(min_val, max_val, bins)
    else:
        from jenkspy import JenksNaturalBreaks
        jnb = JenksNaturalBreaks(bins)
        jnb.fit(data)
        natural_breaks = np.array(jnb.breaks_)

    # Determine adaptive rounding factor
    range_val = max_val - min_val
    if range_val < 1:
        round_factor = 0.01
    elif range_val < 10:
        round_factor = 0.1
    elif range_val < 100:
        round_factor = 1
    else:
        round_factor = 5

    # Apply rounding
    rounded_breaks = np.round(natural_breaks / round_factor) * round_factor

    # Update of minimum and maximum values
    min_val = np.floor(natural_breaks[0] / round_factor) * round_factor
    max_val = np.ceil(natural_breaks[-1] / round_factor) * round_factor
    rounded_breaks[0] = min_val
    rounded_breaks[-1] = max_val

    # Ensure 0 is included
    if rounded_breaks[0] < 0 < rounded_breaks[-1]:
        rounded_breaks = np.insert(rounded_breaks, np.searchsorted(rounded_breaks, 0), 0)

    dynamic_scale = sorted(set(rounded_breaks.tolist()))
    
    return dynamic_scale, min_val, max_val


def adjust_bins_for_folium(scale: List[float], margin: float = 1.0) -> List[float]:
    """
    Adjusts bin edges to ensure all values are included by `folium.Choropleth`.

    Folium excludes values equal to the first or last bin edge.
    This function expands the range slightly to prevent this issue.

    Parameters
    ----------
    scale : List[float]
        Original list of bin edges (must be sorted and have at least 3 values).
    margin : float, optional
        Margin to subtract from the minimum and add to the maximum. Default is 1.0.

    Returns
    -------
    List[float]
        Adjusted bin list compatible with Folium (middle edges unchanged).

    Raises
    ------
    ValueError
        If the input scale is too short to define bins (length < 3).
    """
    if len(scale) < 3:
        raise ValueError("Scale must contain at least 3 values to build bins.")

    bins = scale[1:-1]
    bins.insert(0, scale[0] - margin)
    bins.append(scale[-1] + margin)

    return bins


def validate_user_defined_scale(scale_config: dict, analysis_type: str, data: np.ndarray
) -> Tuple[List[float], float, float]:
    """
    Validates and returns a user-defined scale if present.
    Also ensures it is suitable for the current dataset and for Folium/Branca usage.

    Parameters
    ----------
    scale_config : dict
        Dictionary containing the `scale`, `fill_color`, etc. for the analysis type.
    analysis_type : str
        The analysis type (e.g., 'time', 'EP', etc.)
    data : np.ndarray
        Flattened array of data values used for scale validation.

    Returns
    -------
    Tuple[List[float], float, float]
        - The validated scale.
        - The vmin (used by Branca to define the start of the legend).
        - The vmax (used by Branca to define the end of the legend).

    Raises
    ------
    ValueError
        - If the scale is missing or not a list of at least 4 values.
        - If the scale contains non-numeric values.
        - If the scale is not strictly increasing.

    Notes
    -----
    - The scale must contain at least 4 numeric values to be compatible with Folium's Choropleth.
    - The scale must be strictly increasing (i.e., no flat or reversed segments).
    - The min/max of the data are checked against the scale to detect if values fall outside the range.
      This doesn't raise an error, but issues a warning for clarity.
    """
    scale = scale_config.get("scale", None)

    if not isinstance(scale, list) or len(scale) < 4:
        raise ValueError(f"⚠️ User-defined scale for `{analysis_type}` must be a list of at least 4 numeric values.")

    if not all(isinstance(x, (int, float)) for x in scale):
        raise ValueError(f"⚠️ Invalid scale values for `{analysis_type}`. Must be numeric: {scale}")

    if any(scale[i] > scale[i + 1] for i in range(len(scale) - 1)):
        raise ValueError(f"⚠️ Scale values for `{analysis_type}` must be monotonically increasing: {scale}")

    scale = [float(x) for x in scale]
    vmin = scale[0]
    vmax = scale[-1]

    data_min = float(np.min(data))
    data_max = float(np.max(data))

    if data_min < vmin or data_max > vmax:
        print(f"⚠️ Data for `{analysis_type}` is outside user-defined scale range [{vmin}, {vmax}]. "
              f"Values will be clamped on the map (Folium) and legend (Branca).")

    return scale, vmin, vmax

