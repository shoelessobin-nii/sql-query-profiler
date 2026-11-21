# sql-query-profiler

An adaptive, self-learning, and self-healing SQL query profiler daemon designed to monitor query execution across multiple database engines through modular adapters.

## Overview & Key Features
- **Universal Database Adapters (`src/adapters.py`):** Connects to engines such as SQLite, Oracle, PostgreSQL, and generic SQL targets via pluggable adapters and environment-variable-driven configuration.
- **Rolling Baselines & Drift Detection (`src/history_store.py`):** Persists SQLite telemetry to track rolling P50/P95/MAD latency baselines, execution plan hash drift, and row-count anomalies.
- **Adaptive & Self-Healing Daemon (`src/profiler_daemon.py`, `query_profiler.py`):** Adjusts polling intervals and backoff behavior dynamically, handles connection recovery and target isolation, and triggers self-healing hooks when anomalies occur.

## Project Structure
```text
sql-query-profiler/
├── src/
│   ├── adapters.py
│   ├── history_store.py
│   ├── profiler_daemon.py
│   └── query_profiler.py
├── tests/
│   ├── test_phase1_phase2.py
│   └── test_phase3.py
├── requirements.txt
├── pyproject.toml
├── .gitignore
├── LICENSE
└── README.md