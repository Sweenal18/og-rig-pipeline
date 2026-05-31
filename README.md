# O&G Rig Operations Intelligence Platform — ETL Pipeline

A production-grade data engineering pipeline built on **Databricks** and **Delta Lake** that ingests Oil & Gas rig operational data from a REST API, transforms it through a multi-layer Lambda architecture, and delivers aggregated KPIs to a **Power BI** dashboard.

Built as a portfolio project targeting the **BI Engineer role at Helmerich & Payne (H&P)**.

---

## Architecture

```
REST API (8 endpoints)
        │
        ▼
┌─────────────────┐
│   workspace.    │  Truncate + reload every run
│    staging      │  Exact mirror of API response
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   workspace.    │  Append only — permanent audit trail
│      raw        │  Never truncated
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   workspace.    │  15 dims (SCD Type 2) + 5 facts
│     bronze      │  Incremental watermark load
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   workspace.    │  Full rebuild every run
│     silver      │  Enriched, validated, derived metrics
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   workspace.    │  Incremental MERGE by month
│      gold       │  Aggregated KPIs — Power BI ready
└─────────────────┘
```

---

## Data Source

A custom-built FastAPI application simulating an O&G operations platform hosted on Render.

**Base URL:** `https://og-rig-api.onrender.com`

| Endpoint | Records | Description |
|---|---|---|
| `/master/regions` | 5 | Basin and state metadata |
| `/master/rigs` | 20 | Rig master data |
| `/master/crew` | 50 | Crew member master data |
| `/scada/rig-readings` | 17,620 | Daily drilling KPIs per rig |
| `/scada/equipment-events` | 88,100 | Equipment failure and SLA events |
| `/sap-pm/maintenance` | 8,672 | Maintenance work orders |
| `/erp/crew` | 35,240 | Daily crew hours by shift |
| `/hse/incidents` | 1,465 | Safety and operational incidents |

**Total: 151,097 records** across 5 operational domains.

API source code: [og-rig-api](https://github.com/Sweenal18/og-rig-api)

---

## Pipeline Notebooks

| Notebook | Type | Description |
|---|---|---|
| `Notebook_0b_Staging_Ingestion` | Python | Fetches all 8 API endpoints with pagination, truncates and reloads staging tables |
| `Notebook_0a_Dim_Generator` | Python | Builds 15 bronze dimensions — SCD Type 2, `-1` unknown members, `row_number()` SKs |
| `Notebook_0c_Bronze_Raw_Load` | Python | Resolves surrogate keys, loads 5 bronze fact tables incrementally, appends raw archive |
| `Notebook_1_SQL_Silver_Load` | SQL | Full rebuild of 5 silver tables — joins dims, derives metrics, applies quality flags |
| `Notebook_2_SQL_Gold_Load` | SQL | Incremental MERGE into 5 gold KPI tables by month watermark |

---

## Schema Design

### Dimensions (workspace.bronze) — 15 tables

All dims follow **SCD Type 2** with `valid_from`, `valid_to`, `is_current` columns.
Every dim includes a `-1` unknown member row.
Surrogate keys follow the `[entity]_id_sk` naming convention.

| Dim Table | Source | Natural Key |
|---|---|---|
| dim_date | Generated (2023–2035) | date_id (YYYYMMDD) |
| dim_rig | `/master/rigs` API | rig_id ("RIG-001") |
| dim_region | `/master/regions` API | region_name |
| dim_crew_member | `/master/crew` API | crew_id ("CREW-001") |
| dim_equipment | staging distinct values | equipment_type |
| dim_well | staging distinct values | well_name |
| dim_well_type | staging distinct values | well_type |
| dim_status | staging distinct values | status_name |
| dim_shift | staging distinct values | shift_name |
| dim_downtime_reason | staging distinct values | downtime_reason |
| dim_cost_category | staging distinct values | cost_category |
| dim_failure_type | staging distinct values | failure_type |
| dim_maintenance_type | staging distinct values | maintenance_type |
| dim_contractor | staging distinct values | contractor_name |
| dim_incident_type | staging distinct values | incident_type |

### Facts (workspace.bronze) — 5 tables

All facts store surrogate keys, natural/business keys for traceability, and numeric measures only. Unresolvable FK joins default to `-1`.

| Fact Table | Grain | Rows |
|---|---|---|
| fact_rig_daily_ops | rig_id + recorded_at | 17,620 |
| fact_equipment_events | event_id | 88,100 |
| fact_maintenance_log | maintenance_id | 8,672 |
| fact_crew_hours | rig_id + crew_id + recorded_at | 35,240 |
| fact_incident_log | incident_id | 1,465 |

### Silver (workspace.silver) — 5 tables

Full rebuild every run. Joins facts to dims, resolves human-readable attributes, adds derived metrics and data quality flags.

| Silver Table | Source | Key additions |
|---|---|---|
| silver_rig_ops | fact_rig_daily_ops + 8 dims | uptime_pct, npt_pct, drilling_hours_clean |
| silver_equipment_events | fact_equipment_events + 5 dims | downtime_to_resolution_pct, is_sla_breach |
| silver_maintenance_log | fact_maintenance_log + 6 dims | cost_per_hour, cost_per_technician, is_preventive |
| silver_crew_hours | fact_crew_hours + 5 dims | total_hours, has_overtime, is_npt_crew |
| silver_incident_log | fact_incident_log + 7 dims | severity_rank, caused_downtime, is_serious_incident |

### Gold (workspace.gold) — 5 tables

Incremental MERGE by month. Power BI connects directly to these tables.

| Gold Table | Grain | Rows |
|---|---|---|
| rig_performance_summary | rig_id × month_year | 580 |
| equipment_reliability | equipment_type × region_name × month_year | 725 |
| sla_compliance | equipment_type × region_name × month_year | 725 |
| maintenance_summary | rig_id × month_year | 580 |
| regional_benchmarks | region_name × month_year | 145 |

`regional_benchmarks` includes a weighted composite score: **40% uptime + 20% safety + 20% SLA + 20% availability**.

---

## Databricks Job

**Job name:** `OG_Rig_Pipeline`  
**Schedule:** Daily  
**Compute:** Serverless  
**Total runtime:** ~25 minutes

| Task | Notebook | Depends On |
|---|---|---|
| staging_ingestion | Notebook_0b_Staging_Ingestion | — |
| dim_generator | Notebook_0a_Dim_Generator | staging_ingestion |
| bronze_raw_load | Notebook_0c_Bronze_Raw_Load | dim_generator |
| silver | Notebook_1_SQL_Silver_Load | bronze_raw_load |
| gold_kpis | Notebook_2_SQL_Gold_Load | silver |

---

## Tech Stack

| Tool | Purpose |
|---|---|
| Databricks Free Edition | Compute and orchestration |
| Delta Lake | Storage format for all layers |
| PySpark | Bronze ingestion and dim generation |
| Spark SQL | Silver and gold transformations |
| Power BI | Dashboard and reporting |
| Python / FastAPI | Simulated API data source |
| GitHub | Version control |

---

## Key Design Decisions

**SCD Type 2 on all dims** — `valid_from`, `valid_to`, `is_current` on all 15 dimensions to track master data changes over time.

**`-1` unknown member** — Every dim has a `-1` row representing unknown/not applicable. Unresolvable fact FK joins default to `-1` rather than NULL, following enterprise DW conventions.

**`[entity]_id_sk` naming** — Surrogate keys follow a consistent naming convention distinguishing warehouse-generated keys from source system business IDs.

**Incremental watermark on facts** — Bronze facts and gold tables use `MAX(date_id)` and `MAX(loaded_at)` watermarks respectively — only new data is processed on each run.

**Raw archive layer** — `workspace.raw` is an append-only permanent archive of all staging data. Since staging is truncated on every run, raw preserves the full unmodified API history for audit and recovery.

**Natural keys in facts** — All fact tables store the original business IDs (e.g. `rig_id`, `crew_id`, `event_id`) alongside surrogate keys for traceability without requiring dim joins.

---

## Repository Structure

```
og-rig-pipeline/
├── Notebook_0a_Dim_Generator.ipynb
├── Notebook_0b_Staging_Ingestion.ipynb
├── Notebook_0c_Bronze_Raw_Load.ipynb
├── Notebook_1_SQL_Silver_Load.ipynb
├── Notebook_2_SQL_Gold_Load.ipynb
└── README.md
```
