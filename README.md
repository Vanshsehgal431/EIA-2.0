# 🚀 EIA 2.0 — Cloud Data Engineering Pipeline

## 📌 Overview

EIA 2.0 is an end-to-end cloud-native data engineering pipeline that ingests energy and weather data from external APIs, processes it using an incremental ELT architecture, and stores analytics-ready data in AWS.

The project focuses on building a **production-like pipeline** with reliability, scalability, and observability in mind.

---

## 🧠 Architecture

```
EIA API + Weather API
        ↓
AWS S3 (Bronze Layer - Raw JSON)
        ↓
Transform + Validation (Python)
        ↓
Amazon RDS (PostgreSQL)
        ↓
Fact Table (Analytics Layer)
```

---

## ⚙️ Features

### 🔹 Data Ingestion

* Fetches data from EIA and Open-Meteo APIs
* Handles pagination and API limits

### 🔹 Incremental Processing

* Watermark-based incremental loading
* Late-arriving data handling (2-day buffer)

### 🔹 Data Transformation

* Structured transformation using Pandas
* Schema standardization and normalization

### 🔹 Data Validation

* Schema validation
* Null checks
* Range checks
* Duplicate detection

### 🔹 Cloud Storage

* Raw data stored in **AWS S3 (Bronze Layer)**
* Structured data stored in **Amazon RDS (PostgreSQL)**

### 🔹 Reliability

* Retry mechanism with exponential backoff
* Error handling for API failures

### 🔹 Observability

* Pipeline audit table
* Tracks:

  * run_id
  * status
  * runtime
  * rows processed

### 🔹 Data Modeling

* SQL-based fact table combining energy and weather data

---

## 🛠 Tech Stack

* **Language:** Python
* **Libraries:** Pandas, Requests, Psycopg2, Boto3
* **Database:** PostgreSQL (Amazon RDS)
* **Storage:** AWS S3
* **Infrastructure:** Docker (initial setup)
* **Orchestration:** (Planned) Apache Airflow

---

## 📂 Project Structure

```
pipeline/
│
├── extract/        # API ingestion
├── transform/      # Data transformation
├── validation/     # Data quality checks
├── load/           # Load into RDS
├── storage/        # S3 bronze layer
├── utils/          # Logger, API utils
├── run_pipeline.py # Main pipeline runner
```

---

## ▶️ How to Run

### 1. Clone the repository

```
git clone <your-repo-link>
cd EIA-2.0
```

### 2. Setup environment

```
pip install -r requirements.txt
```

### 3. Configure environment variables

Create a `.env` file:

```
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
AWS_REGION=your_region
S3_BUCKET=your_bucket

DB_HOST=your_rds_endpoint
DB_PORT=5432
DB_NAME=postgres
DB_USER=postgres
DB_PASSWORD=your_password
```

### 4. Run pipeline

```
python -m pipeline.run_pipeline
```

---

## 📊 Example Output

* Raw JSON stored in:

```
s3://<bucket>/bronze/eia/
s3://<bucket>/bronze/weather/
```

* Structured tables in RDS:

```
energy_consumption
weather_daily
fact_energy_weather
pipeline_runs
```

---

## 🔥 Key Learnings

* Building incremental data pipelines
* Handling late-arriving data
* Designing reliable ETL systems
* Integrating AWS services (S3, RDS)
* Implementing data quality and observability

---

## 🚀 Future Improvements

* Add Apache Airflow for orchestration
* Implement real-time streaming pipeline (AWS Kinesis)
* Add data quality monitoring dashboard
* Deploy pipeline fully on AWS EC2

---

## 🤝 Author

**Vansh**
Aspiring Data Engineer 🚀

---
