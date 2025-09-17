# transnetmap.pre
Pre-processing subpackage — input preparation and data structuring.

---

## transnetmap.pre.nptm
NPTM (National Passenger Traffic Model) definition.

| Classes | Summary |
|:---|:---|
| [`NPTM`](./nptm/NPTM.md) | Setup raw data and interactions with PostgreSQL/PostGIS. |

---

## transnetmap.pre.network_child
Child network entities Stations and Links.

| Classes | Summary |
|:---|:---|
| [`Stations`](./network_child/Stations.md) | Setup and validation raw data, visualizations and interactions with PostgreSQL/PostGIS. |
| [`Links`](./network_child/Links.md) | Setup and validation raw data and interactions with PostgreSQL. |

---

## transnetmap.pre.network
Network builder/visualizer for stations + links.

| Classes | Summary |
|:---|:---|
| [`Network`](./network/Network.md) | Aggregation of Stations and Links objects, visualizations and interactions with PostgreSQL/PostGIS. |

---

## transnetmap.pre.pvs
Physical Value Sets (PVS) management for travel time and impacts.

| Classes | Summary |
|:---|:---|
| [`PVS_TravelTime`](./pvs/PVS_TravelTime.md) | Setup and validation raw data and interactions with PostgreSQL. |
| [`PVS_Impacts`](./pvs/PVS_Impacts.md) | Setup and validation raw data and interactions with PostgreSQL. |




<!---
| Name | Summary |
|:---|:---|
| [`nptm`](nptm.md) | NPTM (National Passenger Traffic Model) definition. |
| [`network_child`](network_child.md) | Child network entities Stations and Links. |
| [`network`](network.md) | Network builder/visualizer for stations + links. |
| [`pvs`](pvs.md) | Physical Value Sets (PVS) management for travel time and impacts. |

      - transnetmap.pre:
          - api/pre/index.md
          - transnetmap.pre.nptm:
              - api/pre/nptm.md
              - NPTM: api/pre/nptm/NPTM.md
          - transnetmap.pre.network_child:
              - api/pre/network_child.md
              - Stations: api/pre/network_child/Stations.md
              - Links: api/pre/network_child/Links.md
          - transnetmap.pre.network:
             - api/pre/network.md
             - Network: api/pre/network/Network.md
          - transnetmap.pre.pvs:
             - api/pre/pvs.md
             - PVS_TravelTime: api/pre/pvs/PVS_TravelTime.md
             - PVS_Impacts: api/pre/pvs/PVS_Impacts.md


--->