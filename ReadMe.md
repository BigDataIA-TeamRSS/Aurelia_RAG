# Project AURELIA — Automated Financial Concept Note Generator (RAG, Cloud-Hosted)

AURELIA is a production-grade, cloud-hosted microservice that generates standardized concept notes for financial topics using Retrieval-Augmented Generation (RAG). It prioritizes content retrieved from the Financial Toolbox User’s Guide (fintbx.pdf) and gracefully falls back to Wikipedia when the concept isn’t present in the PDF corpus.
The stack includes: FastAPI (service), Streamlit (UI), Pinecone/ChromaDB (vector store), Cloud Composer / MWAA (managed Airflow), PostgreSQL (cache), and managed hosting (e.g., Cloud Run).

## ✨ Key Features

- PDF-first RAG: Parses and chunks fintbx.pdf with multiple splitter strategies; stores embeddings + metadata.

- Dual vector-DB support: Use Pinecone or ChromaDB via a common retrieval interface.

- FastAPI service: Endpoints for /healthz, /, /query, /seed, /concept/{name}.

- Fallback to Wikipedia: When no PDF context is found, the system standardizes results via the same schema.

- Concept DB seeding: Managed Airflow DAGs to seed/refresh structured concept notes.

- Streamlit frontend: Cloud-hosted UI to search, view, and generate concept notes.

- Cloud-only deployment: No local runtimes; all services run on managed cloud.

## System Architecture
![WhatsApp Image 2025-10-17 at 8 05 10 PM](https://github.com/user-attachments/assets/19de8dfd-a97a-44ce-abb2-c8ce575a74ac)

## 🧵 Data & Pipelines

### Lab 1 — PDF Corpus Construction

- Raw data i.e PDF is fetched from gcp cloud bucket.

- Parse fintbx.pdf preserving reading order, figure/table captions, equations, and code. Tool Used - PyMuPDF open SOurce library.

- Experiment with multiple chunkers: Tool Used : Langchain Text Splitter library. Storage - Stored in GCP bucket and ChromaDB 

  - RecursiveCharacterTextSplitter

  - Header/section-aware splitter

  - Code-aware splitter

- Choose final chunk size and overlap i.e size=1000 based on retrieval quality (e.g., precision@k, MRR, recall@k) and latency. ( Stored in GCP bucket and ChromaDB )

- Write embeddings + metadata (section title, page no.). Tool - Stored in ChromaDB inside collections folder called fintbx_concepts.

### Lab 2 — Managed Orchestration

Tool - Built using GCP composer.
- DAG: fintbx_ingest_dag
Pull fintbx.pdf from bucket → parse → chunk experiments → embed → vector DB write.

- DAG: concept_seed_dag
Generate/refresh concept JSON via instructor client → upsert to Postgres cache.

### Lab 3 — FastAPI RAG Service 

Tool - Deployed on GCP cloud Build, connected to this Github repository.

- Implements retrieval pipeline + Wikipedia fallback.

- Provides /query & /seed + health endpoints.

- /seed:
   - This DAG triggers with predefined list of financial concepts
   - Send query to /seed endpoint with the concept list
   - For every concept it will query in vector store for relevant chunks from fintbx.pdf
   - If relevant content found then Generate structured ConceptNote using instructor + LLM and store in database(postgre in gcp)
   - If no relevant content found the output as the concept is not relevent

  - /query:
    - Querying the data using /query using Streamlit
    - Check database for cached data i.e concept note is present 
    - Incase of data is in cached (postgre) return the query immediately as final answer to user.
    - Considering if not cached query vector store for relevant chunks from fintbx.pdf using RAG architecture and embedding stored to retrive the matched answer to the           query
      If content found then it will generate note from fintbx.pdf
    - If there is no data found then we will check if concept is relevant or not
    - For relevant finacial topic data is fected from Wikipedia and generate answer as final ouput to user
    - Store generated note in database(postgre) as cached
   
  

### Lab 4 — Streamlit (Cloud)

- Hosted on Cloud Run/App Engine/EB.

- Talks to API, displays notes & references.

### Lab 5 — Evaluation & Benchmarking
Following is documented in codelab.
-  Performance: cached vs new generation latency; vector search latency; token costs.
   - Compared common query retrival time for fresh and cached query , calculated it using python function predefined logic and displayed on stremlit UI.
   - Calculated cost per query.
-  Compare Pinecone vs ChromaDB for both quality and latency. Compared which vectore storage is better.

  


## ☁️ Cloud Deployment (GCP Reference)
### 1) Storage & Databases

- Create GCS bucket for artifacts (raw PDF, parsed JSON, embeddings):
    - fintxb_pdf - GCP Bucket has raw data stored that is pdf 
    - fintxb_chunks - Has chunks, embedding , plain text and json format of parsed , chunked and embedded pdf.
    - aurelia_cncept/seed_concet - Has concept pre-defined and seeded.
    - /dag/ - stored all ython files necessary to run the DAG.

- Cloud SQL (Postgres): create an instance + database aurelia_db and set PG_* envs.

### 2) Managed Airflow (Cloud Composer)

- Create a Composer environment and upload dags/:

  - fintbx_ingest_dag.py:
  Parses fintbx.pdf → multiple chunking strategies → embeds → writes vector DB.

  - concept_seed_dag.py:
  Seeds standardized concept notes using the instructor client and caches to Postgres.

- Set connections/variables for:

  - Bucket path(s)

  - Vector backend choice & index/collection

  - Embedding model

  - Postgres URI

### 3) FastAPI on Cloud Run

- Containerize service:
```bash
gcloud builds submit --tag gcr.io/<project-id>/aurelia-api
gcloud run deploy aurelia-api \
  --image gcr.io/<project-id>/aurelia-api \
  --region us-central1 \
  --allow-unauthenticated \
  --set-env-vars "..."   # PG_*, VECTOR_*, GCS_BUCKET, OPENAI_API_KEY, etc.
```

- Verify health:

  - GET https://<cloud-run-url>/healthz

### 4) Streamlit on Cloud Run

- Containerize UI:
```bash
gcloud builds submit --tag gcr.io/<project-id>/aurelia-ui ./streamlit
gcloud run deploy aurelia-ui \
  --image gcr.io/<project-id>/aurelia-ui \
  --region us-central1 \
  --allow-unauthenticated \
  --set-env-vars "API_BASE_URL=https://<cloud-run-api-url>"
```
## 🔌 FastAPI Endpoints
### GET / (root)

Quick hello + version—used for smoke tests.

### GET /healthz

Returns 200 OK if API can reach database and vector store.

### POST /query

#### Body:
```bash
{ "concept": "Sharpe Ratio", "top_k": 5 }
```

#### Behavior:

1. Check Postgres cache; if hit → return cached note (with citation metadata).

2. Else query vector store (PDF-first) for top_k chunks → synthesize structured note.

3. If no PDF context → Wikipedia fallback → synthesize → clearly mark source: wikipedia.

4. Save to cache and return.

#### Response (schema excerpt):
```bash
{
  "concept": "Sharpe Ratio",
  "summary": "...",
  "formula": "...",
  "assumptions": ["..."],
  "use_cases": ["..."],
  "examples": ["..."],
  "references": [{"pdf_section": "6.2", "page": 143}],
  "source": "pdf"   // or "wikipedia"
}
```
### POST /seed

#### Body:
```bash
{ "concepts": ["Duration", "Black-Scholes", "Sharpe Ratio"] }
```

Runs standardized generation for the list, respecting PDF-first logic, and writes to Postgres (idempotent upsert).
Typically triggered by Airflow; can be called on-demand for refreshes.

### GET /concept/{name}

Retrieve a single concept from the cache if present (no regeneration).

## 🖥️ Streamlit UI

- Search for a concept, view cached results instantly, or trigger generation via the API.

- Clear badges indicate PDF vs Wikipedia source.

- Includes a small latency & token usage footer for transparency.

## Codelab & Demo
[codelab](https://codelabs-preview.appspot.com/?file_id=1qAdsxU0h5gxf8uIa31Kf0t2GxHBP56iIXbiIyN-kHIQ#0)
[demo](https://drive.google.com/file/d/1301E82ATM7ArIvbOMyI-z7C-8mWxRWVt/view?usp=drive_link)

## 👥 Team Contributions

| Member | Contribution | Percentage |
|--------|-------------|------------|
| **Somil Shah** | DAG, Wikipedia fallback, Postgres cache | 33.3% |
| **Riya Kapadnis** | FastAPI and Streamlit | 33.3% |
| **Siddhi Dhamale** | Parsing, chunking experiments, embeddings, DAG | 33.3% |


## 📋 Attestation

**WE ATTEST THAT WE HAVEN'T USED ANY OTHER STUDENTS' WORK IN OUR ASSIGNMENT AND ABIDE BY THE POLICIES LISTED IN THE STUDENT HANDBOOK.**
