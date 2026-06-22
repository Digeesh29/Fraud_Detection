# Real-Time Graph-Based Financial Fraud Detection Pipeline

## Problem Statement

Traditional fraud detection systems operate on **batch processing** — transactions are analyzed in bulk at the end of the day. By the time fraud is detected, the money is already gone.

This project addresses that gap by building a **real-time fraud detection pipeline** that analyzes every transaction the moment it occurs — before it completes.

---

## Solution Overview

Instead of looking at transactions one by one, this system models the **entire transaction network as a graph** — accounts as nodes, transactions as edges. This makes complex fraud patterns like money cycling between accounts or coordinated fraud rings immediately visible, patterns that are completely invisible to standard row-by-row SQL queries.

---

## System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        LAYER 1 — DATA GENERATION                │
│                                                                  │
│   PaySim Dataset  +  Custom Synthetic Generator                  │
│   (labeled fraud data)   (continuous real-time stream sim)       │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                     LAYER 2 — STREAMING (KAFKA)                  │
│                                                                  │
│   Producer → Kafka Topic → Consumer Group                        │
│   Partitioned for parallel processing & fault tolerance          │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                   LAYER 3 — PROCESSING (PYTHON)                  │
│                                                                  │
│   Read from Kafka → Preprocess → Feature Engineering             │
│   → Apply ML Model → Output Prediction                           │
└──────────────┬──────────────────────────────┬───────────────────┘
               │                              │
               ▼                              ▼
┌──────────────────────────┐   ┌──────────────────────────────────┐
│  LAYER 4A — GRAPH STORE  │   │     LAYER 4B — STRUCTURED STORE  │
│                          │   │                                   │
│  Neo4j                   │   │  Sqlite                           │
│  · Accounts as nodes     │   │  · Transaction event log          │
│  · Transactions as edges │   │  · Audit trail                    │
│  · Cycle detection       │   │  · Operational queries            │
│  · Community detection   │   │                                   │
└──────────────┬───────────┘   └──────────────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────────────────────────────┐
│                   LAYER 5 — ANALYTICS & ALERTING                 │
│                                                                  │
│   Graph Queries → Fraud Pattern Detection → Alert System         │
│   Dashboard (Neo4j Browser)                                      │
└─────────────────────────────────────────────────────────────────┘
```

---

## Fraud Detection Techniques

| Technique | What It Catches |
|---|---|
| **Cycle Detection** | Money bouncing between accounts in a loop — classic money laundering |
| **Community Detection** | Tightly connected account clusters — coordinated fraud rings |
| **Velocity Monitoring** | Single account making hundreds of transfers in seconds |
| **Graph Centrality** | Accounts acting as hubs in suspicious transaction networks |

---

## Tech Stack

| Layer | Technology | Why |
|---|---|---|
| Data Generation | Python | Custom synthetic transaction generator |
| Streaming | Apache Kafka | High-throughput, fault-tolerant real-time streaming |
| Processing | Python | ETL, feature engineering, ML inference |
| Graph Storage | Neo4j | Relationship-first queries — cycles and clusters |
| Structured Storage | **SQLite** (dev) / **PostgreSQL** (prod) | SQLite for development & testing; PostgreSQL for production audit logging & operational data |
| Visualization | Neo4j Browser | Real-time graph exploration |

---

## Dataset Strategy

Real banking transaction data is never publicly available due to privacy regulations. This project uses a **hybrid approach**:

**PaySim Dataset**
- Synthetic mobile money transaction dataset
- Contains `isFraud` labels — used for supervised model training
- Simulates realistic transaction behavior

**Custom Synthetic Generator**
- Generates continuous transaction streams in real time
- Injects controlled fraud scenarios: cycles, fraud rings, high-frequency bursts
- Enables true real-time simulation without waiting for batch data

---

## ML Design

### Unlabeled Streaming Data (Unsupervised)
- **Isolation Forest** — anomaly detection without requiring labels
- Ideal for production where fraud labels aren't immediately available

### Labeled Training Data (Supervised)
- **Random Forest** — robust baseline classifier
- **XGBoost** — high-performance gradient boosting

### Evaluation Metrics
| Metric | Why It Matters |
|---|---|
| Precision | How many flagged transactions are actually fraud |
| Recall | How many actual frauds did we catch |
| F1 Score | Balance between precision and recall |
| ROC-AUC | Overall model discrimination ability |

### Delayed Labeling Strategy
Since real-time fraud labels aren't immediately available:
1. Start with pre-trained model (trained on PaySim)
2. Apply rule-based validation in parallel
3. Feed confirmed fraud cases back as training data (feedback loop)

---

## Scalability Design

- **Kafka partitioning** distributes load across multiple brokers
- **Consumer groups** enable parallel ML inference across multiple workers
- **Lightweight models** chosen deliberately for low-latency inference
- System designed to handle high-throughput streams (tested design for 1GB+ data loads)

---

## Installation

### Prerequisites

Before installing, ensure you have the following installed on your system:

- **Python 3.9+** — [Download](https://www.python.org/downloads/)
- **Docker & Docker Compose** — [Install Guide](https://docs.docker.com/get-docker/)
- **Git** — [Download](https://git-scm.com/)

### 1. Clone the Repository

```bash
git clone https://github.com/yourusername/fraud-detection-pipeline.git
cd fraud-detection-pipeline
```
### 2. Install Python Dependencies

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

**Key Dependencies:**
- `kafka-python` — Kafka consumer/producer
- `neo4j` — Neo4j driver
- `scikit-learn` — Machine learning models
- `xgboost` — Gradient boosting
- `pandas` — Data manipulation
- `numpy` — Numerical computing
- `psycopg2-binary` — PostgreSQL adapter (for production)

### 3. Start Docker Services

Start Kafka, Neo4j, and PostgreSQL containers:

```bash
docker-compose up -d
```

Verify all services are running:

```bash
docker-compose ps
```

**Expected Output:**
```
CONTAINER ID   IMAGE                              STATUS
xxxxx          confluentinc/cp-kafka:7.6.0        Up (healthy)
xxxxx          neo4j:5.18-community               Up (healthy)
xxxxx          postgres:16-alpine                 Up (healthy)
```

### 4. Configure Environment Variables

Create a `.env` file in the project root:

```bash
# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=transactions

# Neo4j Configuration
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=fraudpassword

# PostgreSQL Configuration (for production)
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=fraud_db
POSTGRES_USER=fraud_user
POSTGRES_PASSWORD=fraudpassword

# SQLite Configuration (for development)
DB_PATH=data/fraud_pipeline.db
```

### 5. Initialize Databases

**Neo4j** — Create indexes for faster queries:

```bash
# Access Neo4j Browser at http://localhost:7474
# Username: neo4j
# Password: fraudpassword
# Run the queries in graph/fraud_queries.py
```

**PostgreSQL** — Schema is auto-initialized from `storage/init.sql`

**SQLite** — Database is created automatically on first run

### 8. Download & Prepare Data

Download the PaySim dataset:

```bash
# Create data directory
mkdir -p data

# Download PaySim CSV from Kaggle:
# https://www.kaggle.com/datasets/ealaxi/paysim1
# Place it as: data/paysim.csv
```

### 8. Run the Pipeline

**Start the Kafka Producer** (generates transactions):

```bash
python kafka/producer.py
```

**In a new terminal, start the Kafka Consumer** (processes transactions):

```bash
python kafka/consumer.py
```

**Monitor in Neo4j Browser:**

```
http://localhost:7474
```

**View SQLite Data:**

```bash
python -c "import sqlite3; conn = sqlite3.connect('data/fraud_pipeline.db'); \
cursor = conn.cursor(); cursor.execute('SELECT * FROM transactions LIMIT 5'); \
print(cursor.fetchall())"
```

### 9. (Optional) Train ML Models

Train on PaySim dataset:

```bash
python ml/train.py 
```

Evaluate model performance:

```bash
python ml/evaluate.py
```

### 9. Run Analytics Dashboard

Start the Flask analytics dashboard:

```bash
python analytics/dashboard.py
```

Access at: `http://localhost:5000`

---

## Troubleshooting

| Issue | Solution |
|---|---|
| Kafka not connecting | Ensure Docker services are running: `docker-compose ps` |
| Neo4j authentication failed | Check credentials in `.env` match docker-compose.yaml |
| SQLite locked error | Close other database connections: `fuser -k data/fraud_pipeline.db` |
| Models not found | Run `python ml/train.py` to generate pre-trained models |
| Missing PaySim data | Download from [Kaggle](https://www.kaggle.com/datasets/ealaxi/paysim1) and place in `data/` |

### Reset Everything

If you need to start fresh:

```bash
# Stop and remove containers
docker-compose down -v

# Clean Python cache
rm -rf __pycache__ .pytest_cache

# Remove generated data
rm -rf data/fraud_pipeline.db data/fraud_alerts.jsonl

# Restart
docker-compose up -d
```

---

## Project Structure (Planned)

```
fraud-detection-pipeline/
│
├── generator/
│   ├── paysim_loader.py          # Load and stream PaySim data
│   └── synthetic_generator.py   # Custom real-time transaction generator
│
├── kafka/
│   ├── producer.py               # Kafka producer
│   └── consumer.py               # Kafka consumer
│
├── processing/
│   ├── preprocessor.py           # Data cleaning and transformation
│   └── feature_engineering.py   # Feature extraction for ML
│
├── ml/
│   ├── train.py                  # Model training on PaySim
│   ├── inference.py              # Real-time prediction
│   └── evaluate.py               # Metrics evaluation
│
├── graph/
│   ├── neo4j_writer.py           # Write transactions to Neo4j
│   └── fraud_queries.py          # Cypher queries for pattern detection
│
├── storage/
│   ├── sqlite_writer.py          # Write to SQLite (development)
│   └── postgres_writer.py        # Write to PostgreSQL (production — prepared)
│
├── analytics/
│   ├── dashboard.py              # Real-time fraud analytics dashboard
│   ├── scheduler.py              # Periodic fraud pattern scanning
│   ├── alert_manager.py          # Alert management and notifications
│   └── templates/
│       └── dashboard.html        # Web UI for fraud monitoring
│
├── docs/
│   └── architecture.md           # Detailed architecture notes
│
└── README.md
```

---

## Current Status

| Component | Status |
|---|---|
| System Architecture | ✅ Complete |
| Dataset Strategy | ✅ Complete |
| ML Design | ✅ Complete |
| Scalability Design | ✅ Complete |
| Implementation | ✅ Complete |

---

## References

- [PaySim Dataset — Kaggle](https://www.kaggle.com/datasets/ealaxi/paysim1)
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Neo4j Graph Data Science](https://neo4j.com/docs/graph-data-science/)
- Base Paper: *Graph-based Anomaly Detection and Description: A Survey*
