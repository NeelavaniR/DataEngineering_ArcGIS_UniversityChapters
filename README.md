# Project Overview
Azure Medallion Data Product

This project implements Azure medallion pipeline that ingests university chapter data from a public API (ArcGIS REST Services Directory) and publishes it as a
data product others can consume.

- Bronze:  Raw land — API payload as received (or near-as-received), plus ingest metadata
- Silver:  Cleaned, typed, deduped, flattened to chapter grain
- Gold	:  Published data product — consumer facing

The solution includes schema creation, data ingestion, transformation, validation, DQ checks and curated datasets for analytics.'


# Architecture
```text
Source Data
    |
    v
  Bronze (Raw land — API payload as received)
    |
    | Cleaning / Standardization
    v
  Silver (Cleaned, typed, deduped, flattened to chapter grain)
    |
    | 
    v
   Gold (Published data product — consumer facing)
 ```  
# Repository Structure
This is particularly useful for an evaluator.

```
├── DataEngineering_ArcGIS_UniversityChapters/src
│   ├── NB_UniversityChapters_DDL.ipynb
│   ├── NB_UniversityChapters_Orchestration
│   ├── NB_UniversityChapters_Inbound_To_Bronze
│   ├── NB_UniversityChapters_Bronze_To_Silver
│ 	├──	NB_UniversityChapters_Silver_To_Gold
````

# Setup Instruction
````
  Step 1: Run the DDL Notebook # NB_UniversityChapters_DDL
			    We have DDL for silver and bronze table
	Step 2: Run the NB # NB_UniversityChapters_Orchestration with below parameter
			    Object Name --> university_chapters ; SourceName --> ArcGIS
````

# Notebook Details

The DDL notebook is responsible for initializing the data product environment.It creates the required database objects.
  
- NB_UniversityChapters_Orchestration --> Execute the child Notebooks to load the data into different layers
- NB_UniversityChapters_Inbound_To_Bronze --> Ingest the API payload with specified state into bronze table
- NB_UniversityChapters_Bronze_To_Silver	--> Data Quality Checks and Quarantine bad records
- NB_UniversityChapters_Silver_To_Gold    --> Load into Data product Layer

# Data Flow
- Ingestion 		- Bronze	--> Source data is ingested from API into bronze layers
-	Transformation - Silver 	--> Handling DQ checks and deduplication
-	Data Product 	- Gold 		--> Published data product — consumer facing




