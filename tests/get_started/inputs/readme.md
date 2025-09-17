# Data Sources

---

## NPTM Sources : *Swiss Confederation*

Data from the Swiss National Passenger Traffic Model (NPTM),  
provided by the Swiss Confederation via the Federal Office of Spatial Development (ARE), according to 2017 status.  
Data relating to travel times and distances are dated 20 April 2022.  

### Links :

- [Model description, fr](https://www.are.admin.ch/are/fr/home/mobilite/bases-et-donnees/modelisation-des-transports/mntp.html)
- [Open source database](https://zenodo.org/records/13589099)

### Files :

Formatted extracts from Swiss traffic model data (NPTM).
- `simplify_zones.parquet`
- `imt_time.arrow`, `imt_length.arrow`
- `pt_time.arrow`, `pt_length.arrow`

---

## PVS Impacts Sources : *mobitool* and *HEIA-FR*

### For *IMT* and *PT* :

Data from the *Facteurs mobitool v3.0* database provided by the mobitool Association, as of 04/25/2025.  

### For *NTS* :

Data from first-order estimates made by Florian Davone form HEIA-FR (HES-SO) as part of the HES-SO GRIPIT project.

### Links :

- [Mobitool home page](https://www.mobitool.ch)
- [Open source database](https://www.mobitool.ch/fr/outils/facteurs-mobitool-v3-0-25.html)

### Files :

- `physical_values_impacts_CO2_2.csv`
- `physical_values_impacts_EP_2.csv`

---
