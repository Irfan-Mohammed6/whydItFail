**WhyDidItFail**

Intelligent Data Pipeline Failure Analyst

*Product Requirements Document  •  v1.0*

|**Author**|Irfan Mohammed|
| :- | :- |
|**Status**|Draft|
|**Version**|1\.0|
|**Last Updated**|16 May 2026|
|**Stack**|Python • PostgreSQL • DuckDB • Airflow • Ollama • FAISS • FastAPI|

# **1. Overview**
Data pipelines fail. Stack traces are cryptic. Engineers waste hours asking the same question every time: why did this break, and did it happen before?

WhyDidItFail is an intelligent observability layer that sits on top of Apache Airflow. It automatically captures failure context when a DAG task fails, stores and embeds a history of failures, and uses a locally-hosted LLM to diagnose root causes and suggest fixes — grounding every answer in similar past failures.

The project is built entirely on open-source tooling and runs locally with zero API costs.

# **2. Problem Statement**
## **2.1 The pain**
- Airflow surfaces error messages but rarely explains root cause
- Engineers manually grep logs and cross-reference past incidents
- The same failures recur — there is no structured memory of what broke and why
- No tooling connects current failures to historical ones automatically
## **2.2 Who feels it**
Any data or backend engineer maintaining Airflow pipelines — which includes the target persona for this portfolio project: mid-level data engineers at AI-first startups.

# **3. Goals & Non-Goals**
## **3.1 Goals**
- Automatically capture Airflow task failures with full context (DAG, task, timestamp, error, stack trace)
- Store a searchable, queryable history of failures in PostgreSQL
- Embed failure summaries and enable semantic similarity search via FAISS
- Use Ollama (Llama 3.1:8B) to generate root-cause diagnosis grounded in past similar failures
- Aggregate failure analytics into DuckDB for trend queries and reporting
- Expose diagnosis results via a FastAPI endpoint
- Orchestrate all recurring jobs with Airflow DAGs
## **3.2 Non-Goals**
- Auto-remediation or self-healing pipelines (out of scope for v1)
- Support for non-Airflow orchestrators
- A production-ready UI dashboard
- Cloud deployment or Kubernetes orchestration

# **4. Feature Requirements**
## **4.1 Failure capture**
- on\_failure\_callback hook fires on any Airflow task failure
- Collector captures: DAG ID, task ID, execution date, exception message, full stack trace, attempt number
- Error is classified into a category (schema error, connection timeout, data validation, unknown) using regex + LLM fallback
- Raw event written to PostgreSQL failures table within 5 seconds of failure
## **4.2 Semantic memory**
- Nightly Airflow DAG generates embeddings for new failure records using sentence-transformers (all-MiniLM-L6-v2)
- Embeddings stored in a persistent FAISS index on disk
- At diagnosis time, top-3 most similar past failures retrieved via cosine similarity
## **4.3 LLM diagnosis**
- Ollama serves Llama 3.1:8B locally
- RAG prompt includes: current failure context + top-3 similar past failures
- LLM returns structured JSON: { root\_cause, confidence, suggested\_fix, similar\_incident\_ids }
- Diagnosis stored back to PostgreSQL and linked to the failure record
## **4.4 Analytics (DuckDB)**
- Nightly Airflow DAG reads from PostgreSQL and loads aggregated data into DuckDB
- DuckDB tables: failure\_trends (by DAG, by week), top\_error\_categories, mean\_time\_to\_diagnose
- Queryable via DuckDB CLI or Python for ad-hoc analysis and reporting
## **4.5 API**
- GET /failures — paginated list of recent failures with diagnosis
- GET /failures/{id} — full detail for a single failure including similar incidents
- POST /diagnose — trigger on-demand diagnosis for a given failure ID
- GET /analytics/trends — return aggregated DuckDB trend data as JSON

# **5. Data Model (PostgreSQL)**
Core tables:

- pipeline\_failures — raw event per task failure (id, dag\_id, task\_id, error\_message, stack\_trace, error\_category, execution\_date, created\_at)
- failure\_diagnoses — LLM output linked to a failure (id, failure\_id, root\_cause, suggested\_fix, confidence, similar\_failure\_ids, created\_at)
- embedding\_index\_metadata — tracks which failure IDs have been embedded and indexed

# **6. Technology Stack**

|**Component**|**Technology**|**Role**|
| :- | :- | :- |
|Failure capture|Python + Airflow callbacks|Hooks into on\_failure\_callback, enriches and writes to Postgres|
|Raw storage|PostgreSQL|Stores every failure event, diagnosis, and embedding metadata|
|Orchestration|Apache Airflow|Schedules nightly embedding, aggregation, and cleanup DAGs|
|Vector search|FAISS + sentence-transformers|Embeds failure summaries, enables semantic similarity retrieval|
|LLM inference|Ollama (Llama 3.1:8B)|Local open-source model, no API cost, runs on-device|
|Analytics|DuckDB|In-process analytical queries over aggregated failure data|
|API layer|FastAPI|Async REST endpoints for failure queries and on-demand diagnosis|

# **7. Build Timeline**
## **Days 1–2 — Foundation**
- Set up Airflow locally (Docker Compose or pip install)
- Create PostgreSQL schema and run migrations
- Implement on\_failure\_callback and write first failure record
## **Days 3–4 — Enrichment & Embedding**
- Error classification logic (regex + category enum)
- sentence-transformers integration and FAISS index builder
- Nightly embedding DAG skeleton
## **Days 5–6 — LLM Diagnosis**
- Ollama setup with Llama 3.1:8B
- RAG prompt template and diagnosis chain
- Similarity retrieval from FAISS, diagnosis written back to Postgres
## **Days 7–8 — DuckDB & API**
- DuckDB aggregation DAG (Postgres → DuckDB)
- FastAPI endpoints with async SQLAlchemy
- End-to-end test: trigger a failure, get a diagnosis via API
## **Days 9–10 — Polish**
- README with architecture diagram and demo GIF
- Seed script with realistic fake failure history
- Code cleanup, type hints, docstrings

# **8. Success Criteria**
- A real Airflow task failure triggers capture, embedding, and diagnosis end-to-end within 60 seconds
- Diagnosis JSON is grounded in at least one similar past failure when history exists
- DuckDB trend queries return results in under 1 second on 10,000 synthetic failure records
- All five FastAPI endpoints return correct responses under load
- Project is fully reproducible from README with a single setup script
