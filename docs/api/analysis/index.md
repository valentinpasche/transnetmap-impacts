# transnetmap.analysis

Analysis core subpackage — edge list building and path optimisation.

---

## transnetmap.analysis.graph
Graph-based optimisation using Dijkstra over the edge list.  

Class, *`Graph`*, is used to configure, construct, and analyze the directed analysis graph.  
It is above the [`transnetmap.analysis.edgelist.EdgeList`](./edgelist/EdgeList.md) class.

| Classes | Summary |
|:---|:---|
| [`Graph`](./graph/Graph.md) | Graph class for network optimization using the Dijkstra algorithm. |

---

## transnetmap.analysis.time_functions
Time calculation functions for network analysis.  

This module serves as a repository of predefined and user-defined **travel time** functions.

| Modules | Summary |
|:---|:---|
| [`time_functions`](./time_functions.md) | Management of time calculation functions for network analysis. |

---

## transnetmap.analysis.edgelist
Edge list construction and I/O for network analysis.  
  
Class, *`EdgeList`*, is **not intended for direct use**,  
but serves as a **parent class** for configuring the analysis graph used by the [`Graph`](./graph/Graph.md) class.

| Classes | Summary |
|:---|:---|
| [`EdgeList`](./edgelist/EdgeList.md) | Construct and manage the list of edges required for routing and optimisation tasks. |



<!---
| Name | Summary |
|:---|:---|
| [`graph`](graph.md) | Graph-based optimisation using Dijkstra over the edge list. |
| [`edgelist`](edgelist.md) | Edge list construction and I/O for network analysis. |
| [`time_functions`](time_functions.md) | Management of time calculation functions for network analysis. |
--->