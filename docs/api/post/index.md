# transnetmap.post

Post-processing subpackage — interactive map visualization and impact presentation.

---

## transnetmap.post.heatmap
Interactive heatmap generation over a transport network.  

Class, *`HeatMap`*, is used to configure, create, and display heat maps  
It is above the [`transnetmap.post.results.Results`](./results/Results.md) class.

  - Loads a partial network per-zone produced by the *`Results`* class.
  - Prepares **choropleth layers** (time, length, changes, transport type, impacts),
  - Builds **popups**, computes **thresholds** (continuous or discrete), and renders an interactive **Folium** map.
  
The main method method is method `HeatMap.generate_map()`.

| Classes | Summary |
|:---|:---|
| [`HeatMap`](./heatmap/HeatMap.md) | Interactive heatmap generation over a transport network. |

---

## transnetmap.post.results
Post-processing utilities to compute and manage environmental/energy/financial impacts.  
  
Class, *`Results`*, is **not intended for direct use**,  
but serves as a **parent class** to processing the optimisation results used by the [`HeatMap`](./heatmap/HeatMap.md) class.

  - Managing **impact calculations** for network analysis.
  - Handling **partial networks** and their respective impact computations.
  - Providing **database utilities** for reading and writing results.

| Classes | Summary |
|:---|:---|
| [`Results`](./results/Results.md) | Utilities to compute and manage optimisation results. |

