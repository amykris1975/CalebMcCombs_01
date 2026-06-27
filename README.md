# Project Harmony: Python Ingestion Scripts

> **Automated data ingestion pipeline for agricultural and geospatial telemetry, feeding the Digital Twin architecture with real-time and historical data.**

[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![BigQuery](https://img.shields.io/badge/BigQuery-Connected-green.svg)](https://cloud.google.com/bigquery)
[![TDA Traceability](https://img.shields.io/badge/TDA-Compliant-orange.svg)]()
[![UN SDG 13](https://img.shields.io/badge/UN%20SDG-13-brightgreen.svg)]()

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Repository Structure](#repository-structure)
- [Prerequisites](#prerequisites)
- [Setup and Installation](#setup-and-installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Deployment and CI/CD](#deployment-and-cicd)
- [Compliance and Standards](#compliance-and-standards)
- [Testing](#testing)
- [API Reference](#api-reference)
- [Contributing](#contributing)

---

## Overview

Project Harmony is a production-grade Python ingestion pipeline designed to automate the collection, processing, validation, and storage of agricultural and geospatial telemetry data. The pipeline serves as the data foundation for a **Digital Twin** architecture, bridging raw sensor outputs and external APIs to a central BigQuery data warehouse.

### Data Sources

| Source | Type | Resolution | Protocol |
|--------|------|------------|----------|
| **Sentinel-2** | Satellite multispectral | 10-20m | Sentinel Hub API |
| **Landsat-8/9** | Satellite multispectral | 30m | USGS M2M API |
| **DJI P4 Multispectral** | Drone imagery | 5cm | GCS / Local files |
| **MicaSense RedEdge** | Drone imagery | 5cm | GCS / Local files |
| **Sentek TEROS** | Soil NPK/pH/EC | Point | LoRaWAN / MQTT |
| **Custom IoT** | Soil sensors | Point | NB-IoT / Cellular |

### Key Features

- **Real-time ingestion** via MQTT for IoT telemetry streams
- **Batch processing** for satellite imagery and drone flights
- **Edge caching** with store-and-forward reliability for intermittent connectivity
- **Radiometric calibration** for drone multispectral sensors
- **Vegetation index computation** (NDVI, EVI, NDRE, MSAVI, OSAVI, PRI, LCI)
- **Biomass density estimation** via hemp-calibrated regression models
- **Agronomic validation** against state and federal hemp cultivation thresholds
- **Sensor drift detection** using statistical z-score analysis
- **Full traceability** with SHA-256 hashes for every reading (TDA compliance)
- **Priority-based flushing** from edge cache (critical alerts first)
- **Automatic retry** with exponential backoff for failed transmissions
- **Credential rotation** with Google Secret Manager integration

---

## Architecture

```
                    +-------------------------+
                    |   External Data Sources  |
                    +-----------+-------------+
                                |
            +-------------------+-------------------+
            |                   |                   |
    +-------v-------+  +--------v--------+  +------v-------+
    | Sentinel Hub  |  |  Drone GCS      |  | IoT Sensors  |
    | (Satellite)   |  |  (Multispec)    |  | (LoRa/MQTT)  |
    +-------+-------+  +--------+--------+  +------+-------+
            |                   |                   |
    +-------v-------+  +--------v--------+  +------v-------+
    | satellite_    |  | drone_          |  | soil_        |
    | _biomass.py   |  | _multispectral  |  | _sensors.py  |
    |               |  | .py             |  |              |
    +-------+-------+  +--------+--------+  +------+-------+
            |                   |                   |
            +-------------------+-------------------+
                                |
                    +-----------v-------------+
                    |    edge_cache.py        |
                    |  (SQLite Buffer/Cache)  |
                    +-----------+-------------+
                                |
                    +-----------v-------------+
                    |    db_connect.py        |
                    |   (BigQuery Connector)  |
                    +-----------+-------------+
                                |
                    +-----------v-------------+
                    |    BigQuery Warehouse    |
                    |  project_harmony dataset |
                    +-------------------------+
```

### Data Flow

1. **Ingestion Layer**: Raw data is collected from satellites (Sentinel-2/Landsat), drones (DJI/MicaSense), and IoT soil sensors via MQTT/REST APIs.
2. **Processing Layer**: Data undergoes radiometric calibration, vegetation index computation, biomass estimation, and agronomic validation.
3. **Edge Cache Layer**: Processed data is buffered in a local SQLite database with priority-based queueing for intermittent connectivity scenarios.
4. **Persistence Layer**: Validated data is streamed to BigQuery with full traceability hashes and partitioned by timestamp for efficient querying.

---

## Repository Structure

```
.
├── src/
│   ├── ingestion/
│   │   ├── satellite_biomass.py      # Satellite imagery → biomass density
│   │   ├── drone_multispectral.py    # Drone multispectral → vegetation indices
│   │   ├── soil_sensors.py           # IoT sensors → NPK/pH/EC readings
│   │   └── edge_cache.py             # Local cache/buffer manager
│   └── utils/
│       ├── db_connect.py             # BigQuery connection & batch inserts
│       └── auth.py                   # Credential & token lifecycle manager
├── tests/
│   ├── conftest.py                   # Shared pytest fixtures
│   ├── test_satellite_biomass.py     # Satellite pipeline tests
│   ├── test_soil_sensors.py          # Soil sensor pipeline tests
│   ├── test_edge_cache.py            # Edge cache manager tests
│   ├── test_auth.py                  # Authentication tests
│   └── test_db_connect.py            # BigQuery connector tests
├── requirements.txt                  # Python dependencies
├── .env.example                      # Environment variable template
├── .gitignore                        # Git ignore patterns
└── README.md                         # This file
```

---

## Prerequisites

- **Python 3.9+**
- **Git**
- **Google Cloud SDK** (for BigQuery / GCS / Secret Manager)
- **Sentinel Hub account** (for satellite imagery)
- **MQTT broker** (for IoT telemetry, optional)

---

## Setup and Installation

### 1. Clone the Repository

```bash
git clone https://github.com/amykris1975/CalebMcCombs_01.git
cd CalebMcCombs_01
```

### 2. Create a Virtual Environment

```bash
python3 -m venv venv
source venv/bin/activate  # Linux/Mac
# or
venv\Scripts\activate     # Windows
```

### 3. Install Dependencies

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

### 4. Configure Environment Variables

```bash
cp .env.example .env
# Edit .env with your actual credentials
```

See [Configuration](#configuration) for details on each variable.

---

## Configuration

### Required Environment Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `GOOGLE_CLOUD_PROJECT` | GCP project ID | `project-harmony-prod` |
| `GOOGLE_APPLICATION_CREDENTIALS` | Path to GCP service account JSON | `/path/to/sa.json` |
| `SENTINEL_CLIENT_ID` | Sentinel Hub OAuth client ID | `your-client-id` |
| `SENTINEL_CLIENT_SECRET` | Sentinel Hub OAuth client secret | `your-client-secret` |
| `SENSOR_API_KEY` | Sensor gateway API key | `your-api-key` |
| `MQTT_USERNAME` | MQTT broker username | `harmony` |
| `MQTT_PASSWORD` | MQTT broker password | `secure-password` |

### Optional Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `BIGQUERY_DATASET` | `project_harmony` | BigQuery dataset name |
| `BIGQUERY_LOCATION` | `US` | BigQuery dataset location |
| `HARMONY_DRONE_BUCKET` | `harmony-drone-telemetry` | GCS bucket for drone imagery |
| `EDGE_CACHE_DB_PATH` | `~/.harmony/edge_cache.db` | Local SQLite cache path |
| `EDGE_CACHE_MAX_SIZE_MB` | `500` | Maximum cache size in MB |
| `EDGE_FLUSH_INTERVAL_SEC` | `300` | Cache flush interval in seconds |
| `PIPELINE_LOG_LEVEL` | `INFO` | Logging level |

---

## Usage

### Satellite Biomass Ingestion

```bash
# Ingest Sentinel-2 data for a field
python -m src.ingestion.satellite_biomass \
    --field-id FIELD-001 \
    --min-lon -105.0 --min-lat 40.0 \
    --max-lon -104.9 --max-lat 40.1 \
    --days-back 7 \
    --source sentinel2
```

### Drone Multispectral Ingestion

```bash
# Ingest a drone flight
python -m src.ingestion.drone_multispectral \
    --flight-id FLIGHT-2024-06-15 \
    --field-id FIELD-001 \
    --images gs://harmony-drone-telemetry/flight1/*.tif \
    --sensor dji_p4
```

### Soil Sensor Ingestion

```bash
# Start MQTT listener for real-time telemetry
python -m src.ingestion.soil_sensors mqtt \
    --broker mqtt.example.com \
    --port 8883

# Poll gateway API
python -m src.ingestion.soil_sensors poll \
    --gateway-url https://sensor-gateway.example.com \
    --field-id FIELD-001

# Ingest batch file (JSON or CSV)
python -m src.ingestion.soil_sensors batch \
    --file data/soil_readings.json \
    --field-id FIELD-001
```

### Edge Cache Management

```bash
# Run flush scheduler (background)
python -m src.ingestion.edge_cache scheduler

# Manual flush
python -m src.ingestion.edge_cache flush

# Store a payload manually
python -m src.ingestion.edge_cache store \
    --type soil \
    --payload '{"ph": 6.5, "sensor_id": "S1"}' \
    --priority soil_critical

# View cache statistics
python -m src.ingestion.edge_cache stats
```

---

## Deployment and CI/CD

This repository is configured for automated deployment pipelines. The ingestion scripts can be deployed as:

- **Cloud Run jobs** for scheduled satellite ingestion
- **Cloud Functions** for event-driven drone processing
- **GCE/Compute Engine VMs** for long-running MQTT listeners
- **Kubernetes CronJobs** for periodic batch processing

### Docker Deployment

```dockerfile
FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/
COPY .env .

ENTRYPOINT ["python", "-m", "src.ingestion.soil_sensors"]
CMD ["mqtt", "--broker", "mqtt.example.com"]
```

### Terraform Infrastructure

```hcl
resource "google_bigquery_dataset" "harmony" {
  dataset_id = "project_harmony"
  location   = "US"
}

resource "google_cloud_run_v2_job" "satellite_ingestion" {
  name     = "harmony-satellite-ingestion"
  location = "us-central1"

  template {
    template {
      containers {
        image = "gcr.io/${var.project_id}/harmony-satellite:latest"
        env {
          name  = "GOOGLE_CLOUD_PROJECT"
          value = var.project_id
        }
      }
      service_account = google_service_account.harmony.email
    }
  }
}
```

---

## Compliance and Standards

### TDA Traceability

Every telemetry reading is assigned a SHA-256 traceability hash computed from its core fields (reading ID, sensor ID, timestamp, and measurement values). This ensures:

- **Data integrity**: Any tampering with historical data invalidates the hash chain
- **Audit readiness**: Regulators can verify the authenticity of any reading
- **Chain of custody**: Complete provenance from sensor to data warehouse

### UN Sustainable Development Goal 13

The pipeline supports **SDG 13: Climate Action** through:

- Efficient resource utilization via precision agriculture data
- Carbon sequestration monitoring via biomass density tracking
- Reduced chemical runoff via optimized NPK application
- Water conservation via soil moisture monitoring

### Hot Crop Compliance

The validation pipeline enforces hemp-specific regulatory thresholds:

| Parameter | Acceptable Range | Optimal Range |
|-----------|-----------------|---------------|
| pH | 5.5 - 8.5 | 6.0 - 7.5 |
| Nitrogen | 10 - 200 mg/kg | 50 - 150 mg/kg |
| Phosphorus | 5 - 100 mg/kg | 15 - 60 mg/kg |
| Potassium | 20 - 400 mg/kg | 80 - 250 mg/kg |

Readings outside acceptable ranges are rejected; readings in the suboptimal range are flagged for agronomic review.

---

## Testing

### Run All Tests

```bash
pytest tests/ -v
```

### Run with Coverage

```bash
pytest tests/ --cov=src --cov-report=html --cov-report=term
```

### Run Specific Test Modules

```bash
pytest tests/test_satellite_biomass.py -v
pytest tests/test_soil_sensors.py -v
pytest tests/test_edge_cache.py -v
pytest tests/test_auth.py -v
pytest tests/test_db_connect.py -v
```

### Run Integration Tests

```bash
pytest tests/ -m integration -v
```

### Code Quality Checks

```bash
# Formatting
black src/ tests/

# Linting
flake8 src/ tests/

# Type checking
mypy src/
```

---

## API Reference

### `SatelliteBiomassIngestor`

| Method | Description |
|--------|-------------|
| `run_ingestion(field_id, bbox, start_date, end_date)` | Execute full satellite ingestion pipeline |
| `_compute_ndvi(nir, red)` | Compute Normalized Difference Vegetation Index |
| `_compute_evi(nir, red, blue)` | Compute Enhanced Vegetation Index |
| `_estimate_biomass(ndvi, evi)` | Estimate biomass density (kg/ha) |

### `SoilSensorIngestor`

| Method | Description |
|--------|-------------|
| `start_mqtt_listener(broker_host, broker_port)` | Start real-time MQTT subscriber |
| `poll_gateway_api(gateway_url, field_id)` | Poll REST API for new readings |
| `ingest_batch_file(file_path, field_id)` | Ingest JSON/CSV batch file |
| `_validate_reading(reading)` | Full validation pipeline |
| `_detect_drift(sensor_id, field, value)` | Statistical drift detection |

### `EdgeCacheManager`

| Method | Description |
|--------|-------------|
| `store(data_type, payload, priority_tier)` | Store payload in local cache |
| `store_batch(data_type, payloads, priority_tier)` | Store multiple payloads |
| `flush_all()` | Flush all pending entries to BigQuery |
| `flush_priority(tier)` | Flush entries by priority tier |
| `get_stats()` | Return cache statistics |

### `BigQueryConnector`

| Method | Description |
|--------|-------------|
| `insert_rows(table_id, rows)` | Insert rows with batching and retry |
| `query(sql, params, timeout_sec)` | Execute parameterized query |
| `table_exists(table_id)` | Check if table exists |
| `get_table_schema(table_id)` | Fetch table schema |

### `CredentialManager`

| Method | Description |
|--------|-------------|
| `get_satellite_credentials()` | Retrieve Sentinel Hub OAuth credentials |
| `get_sensor_api_key()` | Retrieve sensor gateway API key |
| `get_mqtt_credentials()` | Retrieve MQTT broker credentials |
| `get_token(token_key, refresh_callback)` | Retrieve cached or refreshed token |
| `invalidate_cache(key)` | Invalidate credential cache |

---

## Contributing

We welcome contributions! Please follow these guidelines:

1. **Fork** the repository and create a feature branch
2. **Write tests** for any new functionality
3. **Run the test suite** before submitting: `pytest tests/ -v`
4. **Format code** with Black: `black src/ tests/`
5. **Lint** with flake8: `flake8 src/ tests/`
6. **Type check** with mypy: `mypy src/`
7. **Submit a pull request** with a clear description of changes

### Code Standards

- Follow PEP 8 style guidelines
- Use type hints for all function signatures
- Document all public methods with docstrings
- Maintain test coverage above 80%
- Include traceability hashes for all data classes

### Commit Message Format

```
type(scope): subject

body

footer
```

Types: `feat`, `fix`, `docs`, `style`, `refactor`, `test`, `chore`

Example:
```
feat(soil): add support for Decagon GS3 sensors

Implements parsing for Decagon GS3 volumetric water content
and electrical conductivity measurements.

Closes #42
```

---

## License

Copyright 2024 Project Harmony Contributors. All rights reserved.

This project is proprietary software. Unauthorized copying, distribution, or use is strictly prohibited.

---

## Contact

For questions, issues, or contributions, please contact the Project Harmony team.

**Repository**: [https://github.com/amykris1975/CalebMcCombs_01](https://github.com/amykris1975/CalebMcCombs_01)
