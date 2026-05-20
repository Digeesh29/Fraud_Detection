# Real-Time Graph-Based Financial Fraud Detection Pipeline

> **Status: In Progress** — Architecture complete, implementation ongoing.

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
│  Neo4j                   │   │  PostgreSQL                       │
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
| Structured Storage | PostgreSQL | Operational data and audit logging |
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
│   └── postgres_writer.py        # Write to PostgreSQL
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
| Implementation | 🔄 In Progress |

---

## References

- [PaySim Dataset — Kaggle](https://www.kaggle.com/datasets/ealaxi/paysim1)
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Neo4j Graph Data Science](https://neo4j.com/docs/graph-data-science/)
- Base Paper: *Graph-based Anomaly Detection and Description: A Survey*
