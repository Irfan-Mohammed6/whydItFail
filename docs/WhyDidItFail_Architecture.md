**WhyDidItFail**

Architecture Document

*Technical design reference  •  v1.0*

|**Author**|Irfan Mohammed|
| :- | :- |
|**Version**|1\.0|
|**Last Updated**|16 May 2026|
|**Stack**|Python • PostgreSQL • DuckDB • Airflow • Ollama • FAISS • FastAPI|

# **1. System Overview**
WhyDidItFail is composed of four loosely coupled subsystems that communicate through shared storage (PostgreSQL) and a disk-persisted FAISS index. All components run locally with no cloud dependencies.

**Subsystems:** Failure Ingestion → Semantic Memory → LLM Diagnosis → Analytics & API

# **2. Data Flow**
## **2.1 Failure ingestion path**
- Airflow task fails → on\_failure\_callback fires synchronously
- Python collector extracts: dag\_id, task\_id, exception, stack\_trace, execution\_date
- Error classifier runs regex patterns against exception message to assign a category enum
- If no regex match, a lightweight Ollama call classifies the error (fallback only)
- Enriched record inserted into PostgreSQL pipeline\_failures table
- Total latency target: under 5 seconds from failure to database write
## **2.2 Nightly embedding DAG**
- Airflow DAG runs at 02:00 daily
- Queries PostgreSQL for failure records not yet in embedding\_index\_metadata
- sentence-transformers (all-MiniLM-L6-v2) generates 384-dimensional embeddings for each
- FAISS IndexFlatIP index updated on disk with new vectors
- embedding\_index\_metadata updated with processed IDs
## **2.3 Diagnosis path**
- Triggered by POST /diagnose or automatically after ingestion (configurable)
- Failure text (error + stack trace summary) embedded with same sentence-transformers model
- FAISS top-k=3 similarity search retrieves most similar past failure IDs
- Past failure records fetched from PostgreSQL by ID
- RAG prompt assembled: system context + current failure + 3 similar past failures
- Ollama Llama 3.1:8B generates structured JSON response
- Diagnosis parsed and written to failure\_diagnoses table, linked by failure\_id
## **2.4 Analytics aggregation DAG**
- Airflow DAG runs at 03:00 daily (after embedding DAG)
- Reads from PostgreSQL using pandas or SQLAlchemy
- Computes: failure counts by DAG/week, error category breakdown, mean time to diagnose
- Writes aggregated DataFrames to DuckDB tables via duckdb Python connector
- DuckDB file lives on local disk, queryable by FastAPI and directly via CLI

# **3. Component Design**
## **3.1 PostgreSQL schema**
### **pipeline\_failures**
Primary event store. One row per Airflow task failure.

- id: UUID primary key
- dag\_id: VARCHAR(255)
- task\_id: VARCHAR(255)
- error\_message: TEXT — first line of exception
- stack\_trace: TEXT — full traceback
- error\_category: ENUM (schema\_error, connection\_timeout, data\_validation, file\_not\_found, unknown)
- attempt\_number: INTEGER
- execution\_date: TIMESTAMPTZ
- created\_at: TIMESTAMPTZ DEFAULT NOW()
### **failure\_diagnoses**
One-to-one with pipeline\_failures once diagnosed.

- id: UUID primary key
- failure\_id: UUID REFERENCES pipeline\_failures(id)
- root\_cause: TEXT
- suggested\_fix: TEXT
- confidence: FLOAT (0–1, LLM self-reported)
- similar\_failure\_ids: UUID[] (PostgreSQL array)
- raw\_llm\_response: JSONB — full LLM output for debugging
- created\_at: TIMESTAMPTZ DEFAULT NOW()
### **embedding\_index\_metadata**
Tracks what has been embedded to avoid re-processing.

- failure\_id: UUID PRIMARY KEY
- faiss\_index\_position: INTEGER — position in the FAISS flat index
- embedded\_at: TIMESTAMPTZ
## **3.2 FAISS index**
- Index type: IndexFlatIP (inner product, vectors pre-normalized for cosine similarity)
- Dimension: 384 (all-MiniLM-L6-v2 output dimension)
- Persisted to disk at data/faiss\_index/failures.index
- ID map file at data/faiss\_index/id\_map.json maps FAISS position → failure UUID
- Index rebuilt nightly — not incrementally updated (safe for this scale)
## **3.3 Ollama + LLM**
- Model: llama3.1:8b served locally via Ollama
- Ollama runs as a background service on localhost:11434
- Python client uses httpx async POST to /api/generate
- Prompt template enforces JSON-only output with schema validation via Pydantic
- Retry logic: 3 attempts with exponential backoff on Ollama timeout
- Max tokens: 512 (sufficient for structured diagnosis JSON)
## **3.4 DuckDB**
- Single DuckDB file at data/analytics.duckdb
- Tables: failure\_trends, error\_category\_summary, diagnosis\_latency\_stats
- Populated nightly by Airflow aggregation DAG via duckdb Python connector
- FastAPI reads DuckDB directly using duckdb.connect() in read-only mode
- No server process needed — DuckDB is embedded in the Python process
## **3.5 FastAPI**
- Async application using asyncpg for PostgreSQL and duckdb for analytics
- Pydantic models for all request/response schemas
- Endpoints: GET /failures, GET /failures/{id}, POST /diagnose, GET /analytics/trends, GET /health
- Background task queue for async diagnosis (avoids blocking the request thread on LLM call)

# **4. Project Structure**
Recommended folder layout:

|**Path**|**Purpose**|
| :- | :- |
|whydiditfail/|Project root|
|├── airflow/dags/|Airflow DAG definitions|
|├── collector/|Failure capture + error classification|
|├── embeddings/|Embedding generation + FAISS index management|
|├── diagnosis/|RAG prompt builder + Ollama client|
|├── analytics/|DuckDB aggregation logic|
|├── api/|FastAPI app, routes, Pydantic schemas|
|├── db/|PostgreSQL migrations and connection pool|
|├── data/|FAISS index files + DuckDB file (gitignored)|
|├── scripts/|Seed script, setup helpers|
|└── tests/|Unit + integration tests|

# **5. Key Design Decisions**
## **PostgreSQL as raw store, DuckDB as analytics layer**
PostgreSQL handles transactional writes (failure events, diagnosis records) where ACID guarantees matter. DuckDB handles analytical aggregation queries where column-oriented scan performance matters. The two are kept separate and synced nightly — this mirrors the classic OLTP/OLAP separation pattern used in production data stacks.
## **FAISS over a managed vector database**
For the scale of this project (thousands of failures, not millions), FAISS running locally is simpler and faster than standing up Chroma or Weaviate. It also avoids an extra service dependency. If the project were to scale, migrating the embedding store to Chroma or pgvector would be a clean swap since the retrieval interface is abstracted.
## **Local LLM via Ollama**
Ollama with Llama 3.1:8B runs entirely on-device, incurring no API cost and requiring no network dependency at diagnosis time. The tradeoff is speed (8B models are slower than GPT-4o) and reasoning quality on complex stack traces. Llama 3.1:8B is sufficient for structured JSON extraction and pattern matching against past failures, which is the core task.
## **Airflow for all scheduled work**
Rather than using cron directly, all scheduled jobs (embedding, aggregation, cleanup) are implemented as Airflow DAGs. This means the project demonstrates real Airflow skills (DAG definition, task dependencies, XCom, scheduling) rather than treating it as a black box orchestrator.

# **6. Local Setup**
## **Prerequisites**
- Python 3.11+
- PostgreSQL 15+ running locally
- Ollama installed and llama3.1:8b pulled (ollama pull llama3.1:8b)
- Apache Airflow 2.7+ (pip install apache-airflow)
## **Environment variables**
- POSTGRES\_DSN — PostgreSQL connection string
- OLLAMA\_BASE\_URL — defaults to http://localhost:11434
- FAISS\_INDEX\_PATH — path to index directory
- DUCKDB\_PATH — path to analytics.duckdb file
## **First run**
- python db/migrate.py — creates PostgreSQL tables
- python scripts/seed.py — inserts 500 synthetic failure records
- python embeddings/build\_index.py — builds initial FAISS index
- airflow standalone — starts Airflow with local executor
- uvicorn api.main:app --reload — starts FastAPI
