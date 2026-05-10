# 🚴 Ridemate Mobility — Real-Time Transit Demand Forecasting for Smarter Cities

> **Streaming CitiBike & subway data through Kafka → PySpark → ML models to predict where bikes and riders will be before they get there.**

---

## 📌 Table of Contents

- [Project Overview](#project-overview)
- [Business Problem](#business-problem)
- [Solution Architecture](#solution-architecture)
- [Pipeline Breakdown](#pipeline-breakdown)
- [Datasets](#datasets)
- [Tech Stack](#tech-stack)
- [Notebooks](#notebooks)
- [ML Models & Results](#ml-models--results)
- [Geospatial Analysis](#geospatial-analysis)
- [Business Impact](#business-impact)
- [Repository Structure](#repository-structure)
- [Getting Started](#getting-started)
- [Team](#team)

---

## Project Overview

**Client**: Ridemate Mobility — a smart-city startup based in New York City focused on improving urban commuting using data and AI.

Ridemate partners with NYC DOT, CitiBike operators, and transit agencies to help city planners make smarter, faster decisions. This project delivers a **real-time, end-to-end data pipeline** that ingests live transit feeds, processes them at scale with PySpark, and forecasts demand at CitiBike stations and MTA subway turnstiles across the city.

---

## Business Problem

CitiBike stations and subway platforms across NYC suffer from **unpredictable demand surges**:

- 🚲 Some stations run out of bikes entirely during morning rush hour
- 🅿️ Other docks sit full for hours, blocking return trips
- 🚇 Subway turnstile traffic spikes unpredictably around events, weather changes, and time-of-day patterns
- 🏗️ Nearby construction and local events compound these imbalances in ways operators can't anticipate

**The core challenge**: *"How can we accurately forecast city transit demand and respond proactively — in real time?"*

---

## Solution Architecture

```
┌─────────────────────────────────────────────────────────┐
│                     DATA SOURCES                        │
│  CitiBike CSVs │ MTA Turnstile │ Safety Events │ NYC Construction │
└──────────────────────┬──────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────┐
│              KAFKA PRODUCERS (GCP VM)                   │
│   citibike.py │ subway.py │ events.py                   │
│   Stream records in real-time from CSV sources          │
└──────────────────────┬──────────────────────────────────┘
                       │  Kafka Topics
                       ▼
┌─────────────────────────────────────────────────────────┐
│              KAFKA CONSUMERS (GCP VM)                   │
│   Write streamed messages → .jsonl files                │
│   citibike_consumer_to_file.py                          │
│   subway_consumer_to_file.py                            │
│   events_consumer_to_file.py                            │
└──────────────────────┬──────────────────────────────────┘
                       │  .jsonl files
                       ▼
┌─────────────────────────────────────────────────────────┐
│           PYSPARK ETL & ANALYSIS (Google Colab)         │
│   Load → Clean → Feature Engineer → EDA → Model        │
└──────────────────────┬──────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────┐
│          ML FORECASTING + GEOSPATIAL MAPPING            │
│   Linear Regression │ Random Forest │ GBT Regressor     │
│   Folium heatmaps of high-demand station clusters       │
└─────────────────────────────────────────────────────────┘
```

---

## Pipeline Breakdown

### 1. Kafka Producers (GCP VM)
Three producers stream records from CSV datasets in real-time:
- `citibike.py` — streams CitiBike trip records
- `subway.py` — streams MTA turnstile entries/exits
- `events.py` — streams NYC safety/event data

### 2. Kafka Consumers (GCP VM)
Each consumer writes streamed Kafka messages to local `.jsonl` files:
- `citibike_consumer_to_file.py`
- `subway_consumer_to_file.py`
- `events_consumer_to_file.py`

> **Note on Architecture Decision**: Direct Colab ↔ Kafka streaming was attempted but faced JVM stream timeout and connectivity issues within the Colab environment. The adopted approach — consumer writes to `.jsonl` → upload to Colab — is more reliable and still preserves the real-time ingestion integrity on the GCP side.

### 3. PySpark ETL in Google Colab
- Load `.jsonl` files into PySpark DataFrames
- Schema inference and validation
- Data cleaning, null handling, type casting
- Feature engineering: hour-of-day, day-of-week, lag features, rolling averages
- Joins across CitiBike, subway, events, and construction datasets

### 4. ML Modeling
Demand forecasting trained on engineered features, evaluated with RMSE and R²:
- **Linear Regression** (baseline)
- **Random Forest Regressor**
- **Gradient Boosted Trees (GBT) Regressor**

### 5. Geospatial Visualization
Folium-powered interactive heatmaps showing:
- High-demand CitiBike stations
- Station clusters near active events
- Proximity-based demand correlation

---

## Datasets

| Dataset | File | Description |
|---|---|---|
| CitiBike Trip Data | `nyccitibike.csv` | Station-level bike trip records including start/end station, duration, user type |
| CitiBike Stream | `citibike_stream.jsonl` | Real-time streamed trip records via Kafka (203 MB) |
| MTA Subway Turnstile | `MTA_Subway_Turnstile_Usage_Data__2022.csv` | Turnstile entry/exit counts by station and time |
| NYC Local Events | `nyclocalevents.csv` | Local event data used for demand correlation |
| NYC Safety Events | `Safety_Events.csv` | Safety incidents affecting transit patterns |

> ⚠️ Large files (`citibike_stream.jsonl` ~203 MB, MTA CSV ~25 MB) are tracked via **Git LFS**. See [Getting Started](#getting-started) for setup instructions.

---

## Tech Stack

| Layer | Technology |
|---|---|
| **Streaming** | Apache Kafka |
| **Cloud Infrastructure** | Google Cloud Platform (GCP VM) |
| **Big Data Processing** | Apache Spark / PySpark 3.5 |
| **Notebook Environment** | Google Colab |
| **ML Library** | Spark MLlib |
| **Data Manipulation** | Pandas, PySpark DataFrames |
| **Visualization** | Matplotlib, Seaborn, Folium |
| **SQL / Hive** | Spark SQL, HiveContext |
| **Graph Analytics** | GraphFrames |
| **Language** | Python 3.10 |

---

## Notebooks

### 🔬 Main Project Notebook
| File | Description |
|---|---|
| `Pyspark_Final_Group_Project_KafkaSpark.ipynb` | **Core project notebook** — full pipeline from data loading through ETL, EDA, ML modeling, and geospatial mapping |

### 📚 Supporting / Lab Notebooks
| File | Description |
|---|---|
| `Spark_RDD.ipynb` | RDD operations, transformations, and actions |
| `Spark_SQL.ipynb` | Spark SQL queries and DataFrame API |
| `Spark_SQL_Hive_JSON.ipynb` | HiveContext, Hive tables, and JSON data querying |
| `Spark_GraphFrames.ipynb` | Graph analytics using GraphFrames |
| `SparkStreaming.ipynb` | Structured Streaming with CSV data (Employee Attrition demo) |
| `SparkStreaming_json.ipynb` | Structured Streaming with JSON data (Doctor dataset demo) |

---

## ML Models & Results

Features used for training:
- `start_station_id`, `start_hour`, `day_of_week`
- Lag-1 demand, 3-period rolling average
- Event proximity flag, construction nearby flag
- Subway entry count at nearest station

| Model | Notes |
|---|---|
| Linear Regression | Baseline; fast, interpretable |
| Random Forest | Better captures non-linear demand patterns |
| GBT Regressor | Best overall performance on holdout set |

---

## Geospatial Analysis

Interactive maps (Folium) were generated to visualize:
- 📍 CitiBike stations colored by hourly demand volume
- 🔴 High-demand clusters near event venues
- 🔗 Station-event proximity within configurable radius (via `geopy.distance.geodesic`)

---

## Business Impact

| Impact Area | Outcome |
|---|---|
| **Dock Shortage Prevention** | Forecasts allow operators to rebalance bikes proactively, not reactively |
| **Commuter Experience** | Riders reliably find bikes at high-demand stations during peak hours |
| **Smarter Resource Allocation** | City planners can prioritize rebalancing trucks and staff |
| **Event-Driven Preparedness** | Demand spikes around events are anticipated, not discovered after the fact |
| **Scalability** | Kafka + PySpark architecture scales horizontally as the city's data grows |

---

## Repository Structure

```
ridemate-mobility/
│
├── 📓 notebooks/
│   ├── Pyspark_Final_Group_Project_KafkaSpark.ipynb   ← Main project
│   ├── Spark_RDD.ipynb
│   ├── Spark_SQL.ipynb
│   ├── Spark_SQL_Hive_JSON.ipynb
│   ├── Spark_GraphFrames.ipynb
│   ├── SparkStreaming.ipynb
│   └── SparkStreaming_json.ipynb
│
├── 📊 datasets/
│   ├── nyccitibike.csv
│   ├── nyclocalevents.csv
│   └── citibike_stream.jsonl          ← Git LFS (203 MB)
│
├── 📄 docs/
│   ├── FINAL_PROJECT_REPORT-Kafka-Spark.docx
│   └── Project_Proposal_Ridemate_Mobility.docx
│
└── README.md
```

---

## Getting Started

### Prerequisites
- Python 3.10+
- Java 8 or 11 (required for Spark)
- Git LFS (for large dataset files)

### 1. Clone the repo

```bash
git lfs install
git clone https://github.com/YOUR_USERNAME/ridemate-mobility.git
cd ridemate-mobility
```

### 2. Install Python dependencies

```bash
pip install pyspark pandas matplotlib seaborn folium geopy
```

### 3. Run the main notebook

Open `notebooks/Pyspark_Final_Group_Project_KafkaSpark.ipynb` in Google Colab.

Upload the required `.jsonl` files when prompted (or mount your Google Drive).

### Kafka Setup (GCP VM)

To replicate the real-time streaming pipeline:

1. Provision a GCP VM with Kafka installed
2. Run the producer scripts to stream data:
   ```bash
   python citibike.py
   python subway.py
   python events.py
   ```
3. Run consumers to write `.jsonl` output:
   ```bash
   python citibike_consumer_to_file.py
   python subway_consumer_to_file.py
   python events_consumer_to_file.py
   ```
4. Download the `.jsonl` files and upload to Colab

---

## Team

**Ridemate Mobility Analytics Team** — Group Final Project

*Big Data Technologies with PySpark, Kafka, and Google Cloud*

---

> Built with 🚀 Apache Kafka + ⚡ PySpark + 🗺️ Folium for smarter, data-driven cities.
