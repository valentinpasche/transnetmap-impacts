# -*- coding: utf-8 -*-
"""
Analysis subpackage : edge list construction and path optimization.

For most users, the class `Graph` is the entry point to run Dijkstra and
produce optimization tables. The lower-level class `EdgeList` remains
available in `transnetmap.analysis.edgelist` for advanced workflows,
but is intentionally not re-exported here.
"""

from __future__ import annotations

from .graph import Graph
# Advanced (not re-exported): from .edgelist import EdgeList  # import explicitly if needed

__all__ = ["Graph"]