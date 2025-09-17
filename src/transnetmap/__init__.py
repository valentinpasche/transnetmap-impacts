# -*- coding: utf-8 -*-
"""
`transnetmap` — build, analyze, and visualize multimodal transport networks.

This top-level package exposes three user-facing subpackages:

- `transnetmap.pre`       – input preparation and data structuring
- `transnetmap.analysis`  – edge list building and path optimization
- `transnetmap.post`      – map visualization and impact presentation
"""

from __future__ import annotations

__all__ = ["pre", "analysis", "post", "__version__"]

# Optional version placeholder; replace at build time if needed
__version__ = "1.1.0"