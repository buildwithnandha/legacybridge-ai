# LegacyBridge AI

> AI-powered data migration reconciliation and autonomous root cause analysis platform.

LegacyBridge AI moves enterprise data from legacy systems to modern infrastructure using **Apache Airflow + PySpark**, automatically validates every aspect of the migration, and uses a **multi-step Claude AI agent** to investigate failures, identify root causes, and generate a full PDF incident report — in under 60 seconds.

---

## The Problem It Solves

In enterprise data migrations, failures are silent and expensive.

A CDC trigger skips 847 rows. A type coercion corrupts 12,000 timestamps. A missing column causes downstream joins to silently return wrong results. Nobody notices until production breaks — and by then, finding the cause takes days of manual investigation across multiple systems.

LegacyBridge AI automates the entire investigation loop:

```
Legacy DB (DB2)
     ↓  PySpark ETL
Modern DB (PostgreSQL)
     ↓  Reconciliation Engine
Schema Drift + Row Mismatches + CDC Gaps detected
     ↓  Claude AI Agent (multi-step tool calls)
Root cause identified with confidence score
     ↓  PDF Incident Report
Full audit trail — ready to share with leadership
```

---

## Demo

> Demo runs against mock data only. See Enterprise AI Governance section below.

```
Health Score:    10/100 — Critical
Tables Analyzed: 5
Issues Found:    8 (3 Critical, 3 Warning, 2 Informational)
Analysis Time:   28 seconds
```

---

## System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      LegacyBridge AI                           │
│                                                                 │
│  ┌────────────┐   ┌─────────────┐   ┌────────────────────┐    │
│  │  Layer 1   │   │  Layer 2    │   │     Layer 3        │    │
│  │  ETL       │──▶│  Recon      │──▶│  RCA Agent         │    │
│  │  Pipeline  │   │  Engine     │   │  (Claude AI)       │    │
│  │            │   │             │   │                    │    │
│  │  Airflow   │   │  Schema     │   │  6 Tool Calls      │    │
│  │  PySpark   │   │  Row Diff   │   │  Chain-of-Thought  │    │
│  │  PostgreSQL│   │  CDC Check  │   │  PDF Report        │    │
│  └────────────┘   └─────────────┘   └────────────────────┘    │
│                                              │                 │
│                                              ▼                 │
│                              ┌───────────────────────────┐    │
│                              │      Layer 4              │    │
│                              │  React Dashboard          │    │
│                              │  Split View — Agent       │    │
│                              │  reasoning streams live   │    │
│                              └───────────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
```

---

## How It Works

### Layer 1 — ETL Pipeline (Airflow + PySpark)

A 5-task Airflow DAG orchestrates the full migration:

```
Task 1: extract_from_source   →  PySpark reads legacy source DB
Task 2: transform_data        →  Type mapping, TZ normalization, null handling
Task 3: load_to_target        →  Upsert to modern PostgreSQL
Task 4: run_reconciliation    →  Triggers recon engine automatically
Task 5: run_rca_agent         →  Claude agent fires only if issues found
```

### Layer 2 — Reconciliation Engine

Four engines run in parallel after every pipeline execution:

| Engine | What It Checks |
|---|---|
| Schema Differ | Missing columns, type mismatches, nullability changes |
| Row Reconciler | Row counts, checksum comparison per table |
| CDC Analyzer | Event counts, gap rate, missed event patterns |
| Sample Differ | Sample rows where source value != target value |

Every run produces a **Migration Health Score (0–100)**:
- Start at 100
- Critical issue: -15 points each
- Warning issue: -5 points each
- Row mismatch > 1%: -10 points per table
- CDC gap rate > 2%: -10 points per table

### Layer 3 — Claude AI RCA Agent

The agent investigates each table using 6 tool calls autonomously:

```
get_schema_diff(table)         →  Detects column and type issues
get_row_recon(table)           →  Compares source vs target counts
get_cdc_events(table)          →  Analyzes CDC event gaps
get_sample_diff(table, column) →  Samples actual mismatched rows
get_pipeline_logs(dag, run)    →  Reads Airflow task execution logs
classify_root_cause(evidence)  →  Final RCA with confidence score
```

Root cause classifications:
- `CDC_SCHEMA_DRIFT` — Column missing from CDC payload
- `CDC_TRIGGER_GAP` — CDC trigger skipping certain update patterns
- `TYPE_COERCION` — Data type conversion causing value loss
- `TZ_MISMATCH` — Timezone offset causing timestamp drift
- `SOFT_DELETE_MISMATCH` — Delete model differs between systems
- `NULL_EMPTY_MISMATCH` — Empty string vs NULL handling
- `HEALTHY` — No issues found

### Layer 4 — React Dashboard

Split-view UI — agent reasoning streams live on the left while the incident report builds on the right in real time.

---

## Tech Stack

### Backend
| Technology | Purpose |
|---|---|
| Python 3.11+ | Core backend language |
| FastAPI | REST API + SSE streaming |
| Apache Airflow 2.8+ | Pipeline orchestration |
| Apache PySpark 3.5+ | Distributed ETL |
| Anthropic SDK | Claude AI agent |
| SQLAlchemy 2.x | Database ORM |
| ReportLab | PDF generation |
| PostgreSQL 15 | Source + target databases |
| Docker | Containerization |

### Frontend
| Technology | Purpose |
|---|---|
| React 18 | UI framework |
| Tailwind CSS | Styling |
| Vite | Build tool |
| EventSource API | SSE streaming for live agent panel |

---

## Data Model

Five supply chain tables with intentional drift injected for demo:

| Table | Issues Injected | Severity |
|---|---|---|
| vendor | Missing column, type mismatch, CDC trigger gap | Critical |
| inventory | Type coercion (DECIMAL→FLOAT), TZ offset | Warning |
| purchase_order | None — clean baseline | Healthy |
| inventory_transaction | Soft delete mismatch | Critical |
| supplier_contract | Null vs empty string | Warning |

---

## Quick Start

### Prerequisites

Before running, confirm you have:

- [ ] Docker Desktop installed
- [ ] Python 3.11+ installed
- [ ] Node 18+ installed
- [ ] Java 11+ installed (required for PySpark)
- [ ] Anthropic API key (platform.anthropic.com)
- [ ] 8GB+ RAM available (16GB recommended)
- [ ] 10GB+ free disk space

### Setup

**1. Clone the repo**
```bash
git clone https://github.com/yourusername/legacybridge-ai.git
cd legacybridge-ai
```

**2. Configure environment**
```bash
cp .env.example .env
# Edit .env and add your ANTHROPIC_API_KEY
```

**3. Start all services**
```bash
docker compose up -d
```

**4. Wait for services to be ready (~2 minutes)**
```bash
docker compose ps
# All services should show "healthy"
```

**5. Open the dashboard**
```
http://localhost:3000
```

**6. Open Airflow**
```
http://localhost:8080
Username: airflow
Password: airflow
```

### Running Your First Reconciliation

**Option A — From the dashboard**
```
Click "Run Analysis" in the dashboard
Watch the agent reason through each table live
PDF report appears automatically when complete
```

**Option B — From Airflow**
```
Open Airflow at localhost:8080
Trigger the legacy_migration_pipeline DAG
Reconciliation and RCA run automatically after ETL
```

**Option C — From the API**
```bash
curl -X POST http://localhost:8000/api/recon/run
```

---

## Project Structure

```
legacybridge-ai/
│
├── docker-compose.yml
├── .env.example
├── README.md
│
├── airflow/
│   ├── dags/
│   │   └── legacy_migration_pipeline.py
│   ├── plugins/
│   │   └── legacybridge_operator.py
│   └── logs/
│
├── spark/
│   ├── jobs/
│   │   ├── extract_source.py
│   │   ├── transform_data.py
│   │   └── load_target.py
│   ├── schemas/
│   │   ├── source_schema.py
│   │   └── target_schema.py
│   └── utils/
│       ├── schema_mapper.py
│       └── spark_session.py
│
├── backend/
│   ├── main.py
│   ├── config.py
│   ├── requirements.txt
│   ├── data/
│   │   ├── source_seed.sql
│   │   ├── target_seed.sql
│   │   ├── cdc_events.json
│   │   └── demo_result.json
│   ├── engines/
│   │   ├── schema_differ.py
│   │   ├── row_reconciler.py
│   │   ├── cdc_analyzer.py
│   │   └── sample_differ.py
│   ├── agents/
│   │   ├── rca_agent.py
│   │   ├── tools.py
│   │   └── prompts.py
│   ├── reports/
│   │   └── pdf_generator.py
│   └── routers/
│       ├── recon.py
│       ├── pipeline.py
│       └── health.py
│
└── frontend/
    ├── package.json
    ├── vite.config.js
    └── src/
        ├── App.jsx
        └── components/
            ├── Header.jsx
            ├── StatusBar.jsx
            ├── PipelineStatus.jsx
            ├── agent/
            │   ├── AgentPanel.jsx
            │   ├── ThinkingStep.jsx
            │   ├── ToolCall.jsx
            │   └── ToolResult.jsx
            └── report/
                ├── ReportPanel.jsx
                ├── ExecutiveSummary.jsx
                ├── SchemaDriftTable.jsx
                ├── RowReconTable.jsx
                ├── CdcAnalysis.jsx
                └── RcaFinding.jsx
```

---

## Configuration

```env
# Anthropic
ANTHROPIC_API_KEY=your_key_here

# Demo Mode — set true for public deployment (zero API cost)
# set false for real runs during development
DEMO_MODE=true

# Source DB — simulates legacy DB2
SOURCE_DB_HOST=localhost
SOURCE_DB_PORT=5433
SOURCE_DB_NAME=legacybridge_source
SOURCE_DB_USER=postgres
SOURCE_DB_PASSWORD=postgres

# Target DB — modern PostgreSQL
TARGET_DB_HOST=localhost
TARGET_DB_PORT=5434
TARGET_DB_NAME=legacybridge_target
TARGET_DB_USER=postgres
TARGET_DB_PASSWORD=postgres

# Airflow
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@airflow-db/airflow

# Cost protection
MAX_TOKENS_PER_RUN=10000
MAX_RUNS_PER_DAY=20
```

---

## Docker Services

| Service | Port | RAM | Purpose |
|---|---|---|---|
| db-source | 5433 | 256MB | Source PostgreSQL — simulates DB2 |
| db-target | 5434 | 256MB | Modern PostgreSQL target |
| airflow-db | 5435 | 256MB | Airflow metadata database |
| airflow-webserver | 8080 | 512MB | Airflow UI |
| airflow-scheduler | — | 256MB | DAG scheduler |
| spark-master | 7077 | 1GB | PySpark master node |
| spark-worker | — | 1.5GB | PySpark worker |
| backend | 8000 | 256MB | FastAPI application |
| frontend | 3000 | 128MB | React dashboard |
| **Total** | | **~4.5GB** | Requires 8GB+ RAM |

---

## API Reference

| Method | Endpoint | Description |
|---|---|---|
| POST | /api/recon/run | Trigger full reconciliation run |
| GET | /api/recon/{run_id}/stream | SSE stream of live agent reasoning |
| GET | /api/recon/{run_id}/report | Structured JSON report |
| GET | /api/recon/{run_id}/pdf | Download PDF incident report |
| GET | /api/recon/history | Past reconciliation runs |
| GET | /api/pipeline/status | Current Airflow DAG status |
| GET | /api/health | System health check |

---

## ⚠️ Enterprise AI Governance

LegacyBridge AI is built as a demonstration of AI-powered data reconciliation and root cause analysis. Before deploying in a production enterprise environment, the following AI governance requirements must be addressed:

### Data Privacy & Residency
- Anonymize all sensitive columns before sending context to any external LLM API — send schema metadata, counts, and patterns only — never raw data values
- Verify compliance with GDPR, CCPA, or applicable regional data regulations for your jurisdiction
- Review your organization's data residency policy — this demo sends context to Anthropic's API which may not align with all enterprise data policies
- Consider using Anthropic's Enterprise tier with a Data Processing Agreement for production use

### AI Accountability & Explainability
- All agent reasoning steps are logged and fully auditable by design — every tool call, decision, and classification is visible in the dashboard and PDF report
- Human approval must gate any automated remediation actions before production deployment — LegacyBridge AI investigates and recommends, never remediates automatically
- Pin the Claude model version in your configuration for reproducible, auditable results across runs
- Maintain a log of all RCA outputs for compliance audit trails

### Access Controls
- Implement role-based access control (RBAC) before enterprise deployment — control who can trigger reconciliation runs and who can view results
- Integrate with your organization's SSO and identity provider
- Maintain a full audit log of all user actions including who triggered each run and when
- Apply least-privilege principles to database connection credentials

### Compliance Frameworks To Review
- **EU AI Act** — assess risk classification for automated decision support in data infrastructure
- **NIST AI RMF** — apply risk management controls across govern, map, measure, and manage functions
- **ISO 42001** — consider formal AI management system certification for production deployment
- **SOC 2 Type II** — required if handling customer or partner data in your pipeline

> **Important:** This project uses mock data exclusively for all demonstrations. The demo dataset contains no real personal information, business data, or proprietary content. Never connect LegacyBridge AI to real production databases without proper governance controls, security review, and organizational approval in place.

---

## Nice To Have

These features would take LegacyBridge AI from a demonstration to a production-grade enterprise platform. Not in scope for v1 — but the architecture is designed to support all of them.

### 🔧 Pipeline & Connectivity

| Feature | Why It Matters |
|---|---|
| Real DB2 JDBC connector | Connect to actual legacy systems — not just PostgreSQL simulation |
| Live Kafka stream integration | Replace JSON flat files with true real-time CDC event streaming |
| Scheduled automated reconciliation | Proactively catch drift on a schedule — not just after manual pipeline runs |
| Multi-database support | Oracle, MySQL, SQL Server — not just DB2 and PostgreSQL |

### 🤖 AI & Intelligence

| Feature | Why It Matters |
|---|---|
| Self-healing — auto-generate backfill SQL | Agent generates the fix SQL — engineer reviews and applies with one click |
| Pattern learning across runs | Agent learns from past RCA findings and improves classification accuracy over time |
| Multiple LLM support | Choose between Claude, GPT-4, Gemini, or a local Llama model for air-gapped environments |
| Confidence threshold configuration | Let teams tune the minimum confidence level before an alert fires — reduce noise |

### 🏢 Enterprise Readiness

| Feature | Why It Matters |
|---|---|
| Role-based access control (RBAC) | Control who can trigger runs, view reports, and access sensitive findings |
| SSO and identity provider integration | SAML, OAuth, LDAP — required for enterprise login policies |
| Slack and Teams alert integration | Push critical findings to engineering channels — nobody checks dashboards proactively |
| Historical trend analysis | Track migration health score over time — spot tables that degrade regularly |
| Multi-tenant support | Multiple teams and projects under one platform with isolated data |
| Audit log export | Export full run history for compliance and security reviews |

---

## License

MIT License — free to use, modify, and distribute.

---

*Built to demonstrate what happens when enterprise data engineering meets autonomous AI reasoning.*
