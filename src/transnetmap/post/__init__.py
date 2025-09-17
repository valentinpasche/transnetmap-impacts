# -*- coding: utf-8 -*-
"""
Post-processing subpackage: interactive map visualization and impact presentation.

This subpackage re-exports the main user-facing class:

- class `HeatMap` – build Folium maps from per-zone partial networks, with validated
  layers (time, length, changes, type, impacts), thresholds, legends, and popups.

The base class `Results` (which prepares per-zone results) is available in
`transnetmap.post.results`, but is intentionally not re-exported here to keep the
public API focused on interactive visualization.
"""

from __future__ import annotations

from .heatmap import HeatMap
# Advanced (not re-exported): from .results import Results  # import explicitly if needed

__all__ = ["HeatMap"]