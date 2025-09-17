# -*- coding: utf-8 -*-
"""
Pre-processing subpackage: input preparation and data structuring.

This subpackage re-exports user-facing classes so they can be imported directly:

- class `NPTM` – prepare and validate inputs (zones, IMT/PT) for downstream steps
- classes `Network`, `Stations`, `Links` – build and inspect network geometry
- classes `PVS_TravelTime`, `PVS_Impacts` – manage physical value sets (time, impacts)
"""

from __future__ import annotations

from .nptm import NPTM
from .network import Network
from .network_child import Stations, Links
from .pvs import PVS_TravelTime, PVS_Impacts

__all__ = [
    "NPTM",
    "Network",
    "Stations",
    "Links",
    "PVS_TravelTime",
    "PVS_Impacts",
]