import os
import json
import time
from typing import List, Optional, Literal, Dict, Any
from dataclasses import dataclass

# GCP Storage
from google.cloud import storage
from google.oauth2 import service_account

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from dotenv import load_dotenv

import requests
from contextlib import asynccontextmanager

# ChromaDB Cloud client
import chromadb

# OpenAI (LLM for final answer)
from openai import OpenAI

# Wikipedia fallback
import wikipedia

# ---------- boot ----------
load_dotenv(override=True)

APP_NAME = "aurelia-fastapi"
PDF_TOP_K = int(os.getenv("PDF_TOP_K", "5"))
USE_WIKI = os.getenv("WIKI_FALLBACK", "1") == "1"

# vector store config - CHROMADB CLOUD
CHROMADB_API_KEY = os.getenv("CHROMADB_API_KEY", "ck-BrgcfXPxcyK22Lir6SGqqiVQsEaU5EE8mDbQ71omUUg8")
CHROMA_TENANT_ID = os.getenv("CHROMA_TENANT_ID", "default_tenant")
CHROMA_DATABASE = os.getenv("CHROMA_DATABASE", "AURELIA_CHROMA_DB")
CHROMA_COLLECTION = os.getenv("CHROMA_COLLECTION", "fintbx_concepts")

# GCP Storage config
GCP_BUCKET_NAME = os.getenv("GCP_BUCKET_NAME", "aurelia-concepts")
GCP_SEED_FOLDER = os.getenv("GCP_SEED_FOLDER", "seed_concept/out/")
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# For Cloud Run, use default credentials or service account from environment
GCP_CREDENTIALS_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
if not GCP_CREDENTIALS_PATH:
    # Try local service account file for development
    GCP_CREDENTIALS_PATH = os.path.join(BASE_DIR, "service-account.json")
    if not os.path.exists(GCP_CREDENTIALS_PATH):
        GCP_CREDENTIALS_PATH = None

print(f"📁 Base directory: {BASE_DIR}")
print(f"🔑 GCP credentials: {'Default' if not GCP_CREDENTIALS_PATH else GCP_CREDENTIALS_PATH}")

# postgres config (using Cloud SQL connector like DAG)
# Connection details are hardcoded to match DAG configuration

# llm config
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
ANSWER_MODEL = os.getenv("ANSWER_MODEL", "gpt-4o-mini")

# ---------- GCP Storage Client ----------
def get_gcs_client():
    """Initialize GCP Storage client"""
    try:
        if GCP_CREDENTIALS_PATH and os.path.exists(GCP_CREDENTIALS_PATH):
            print(f"🔑 Using service account file: {GCP_CREDENTIALS_PATH}")
            credentials = service_account.Credentials.from_service_account_file(
                GCP_CREDENTIALS_PATH
            )
            return storage.Client(credentials=credentials)
        else:
            # Use default credentials (for Cloud Run and other GCP environments)
            print("🔑 Using default GCP credentials")
            return storage.Client()
    except Exception as e:
        print(f"❌ GCP client initialization failed: {e}")
        return None

def fetch_concept_from_gcs(concept_name: str) -> Optional[Dict[str, Any]]:
    """
    Fetch a concept summary from GCS bucket
    Expected file: gs://aurelia-concepts/seed_concept/out/concept_seed_summary.json
    """
    try:
        client = get_gcs_client()
        if not client:
            print("❌ GCS client not available")
            return None
        
        bucket = client.bucket(GCP_BUCKET_NAME)
        
        # Try to find the concept file
        # Normalize concept name for filename
        normalized_name = concept_name.lower().replace(" ", "_").replace("-", "_")
        
        # Try different filename patterns
        possible_filenames = [
            f"{GCP_SEED_FOLDER}concept_seed_summary.json",
            f"{GCP_SEED_FOLDER}{normalized_name}.json",
            f"seed_concept/out/concept_seed_summary.json",
        ]
        
        for blob_name in possible_filenames:
            blob = bucket.blob(blob_name)
            if blob.exists():
                content = blob.download_as_text()
                data = json.loads(content)
                print(f"✅ Loaded concept data from GCS: {blob_name}")
                return data
        
        print(f"⚠️  No concept file found in GCS for: {concept_name}")
        return None
        
    except Exception as e:
        print(f"❌ Error fetching from GCS: {e}")
        return None

def list_all_concepts_from_gcs() -> List[str]:
    """
    List all available concepts in the GCS bucket
    """
    try:
        client = get_gcs_client()
        if not client:
            return []
        
        bucket = client.bucket(GCP_BUCKET_NAME)
        blobs = bucket.list_blobs(prefix=GCP_SEED_FOLDER)
        
        concepts = []
        for blob in blobs:
            if blob.name.endswith('.json'):
                # Extract concept name from filename
                filename = blob.name.split('/')[-1]
                concept_name = filename.replace('.json', '').replace('_', ' ').title()
                concepts.append(concept_name)
        
        return concepts
        
    except Exception as e:
        print(f"❌ Error listing concepts from GCS: {e}")
        return []

# ---------- db (Cloud SQL connector like DAG) ----------
def get_pg_conn():
    """Connect to Cloud SQL using connector (like DAG)"""
    try:
        from google.cloud.sql.connector import Connector
        
        # Use same connection details as DAG
        instance_connection_name = "aurelia-475916:us-central1:cache-db"
        db_user = "postgres"
        db_pass = "postGre@123"
        db_name = "rag_DB"
        
        print(f"🔧 Connecting to Cloud SQL: {instance_connection_name}")
        
        connector = Connector()
        conn = connector.connect(
            instance_connection_name,
            "pg8000",
            user=db_user,
            password=db_pass,
            db=db_name,
        )
        print(f"✅ Cloud SQL connection successful")
        return conn
        
    except ImportError as e:
        print(f"⚠️  Cloud SQL connector not available: {e}. Skipping PostgreSQL cache.")
        return None
    except Exception as e:
        print(f"❌ Cloud SQL connection failed: {e}")
        return None

def ensure_cache_table():
    try:
        conn = get_pg_conn()
        if conn is None:
            print("⚠️  No database connection, skipping cache table creation")
            return
            
        try:
            with conn.cursor() as cur:
                cur.execute("""
                CREATE TABLE IF NOT EXISTS rag_cache (
                  query TEXT PRIMARY KEY,
                  collection_name TEXT,
                  gen_model TEXT,
                  embed_model TEXT,
                  answer_text TEXT,
                  answer_json JSONB,
                  sources_json JSONB,
                  retrieval_source TEXT,
                  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                """)
            print("✅ Cache table ready")
        finally:
            conn.close()
    except Exception as e:
        print(f"⚠️  Cache table creation skipped: {e}")

# ---------- vector store (ChromaDB Cloud) ----------
def get_chroma_collection():
    """Connect to ChromaDB Cloud"""
    if not CHROMADB_API_KEY:
        print("⚠️  CHROMADB_API_KEY not provided, ChromaDB unavailable")
        return None
    
    try:
        # Create CloudClient
        client = chromadb.CloudClient(
            api_key=CHROMADB_API_KEY,
            tenant=CHROMA_TENANT_ID,
            database=CHROMA_DATABASE
        )
        
        # Get or create collection
        collection = client.get_or_create_collection(
            name=CHROMA_COLLECTION,
            metadata={"hnsw:space": "cosine", "description": "Financial concepts from PDFs"}
        )
        
        print(f"✅ Connected to ChromaDB Cloud collection: {CHROMA_COLLECTION}")
        return collection
        
    except Exception as e:
        print(f"❌ Failed to connect to ChromaDB Cloud: {e}")
        return None

@dataclass
class RetrievedChunk:
    content: str
    page: Optional[int] = None
    section: Optional[str] = None
    source: Literal["pdf", "gcs"] = "pdf"
    score: Optional[float] = None

def search_gcs_concepts(query: str) -> Optional[Dict[str, Any]]:
    """Search for concept data in GCS bucket - simplified version"""
    try:
        return fetch_concept_from_gcs(query)
    except Exception as e:
        print(f"❌ Error searching GCS concepts: {e}")
        return None
    
def search_pdf_chunks(query: str, k: int = PDF_TOP_K) -> List[RetrievedChunk]:
    """Search ChromaDB Cloud for relevant chunks"""
    try:
        col = get_chroma_collection()
        if col is None:
            print("⚠️  ChromaDB not available, returning empty results")
            return []
            
        res = col.query(
            query_texts=[query], 
            n_results=k, 
            include=["documents", "metadatas", "distances"]
        )
        chunks: List[RetrievedChunk] = []
        if not res or not res.get("documents"):
            return chunks
        distances = res.get("distances", [[]])
        for doc, meta, dist in zip(res["documents"][0], res["metadatas"][0], distances[0] if distances else [None] * len(res["documents"][0])):
            chunks.append(
                RetrievedChunk(
                    content=doc,
                    page=meta.get("page"),
                    section=meta.get("section"),
                    score=(1.0 - dist) if isinstance(dist, (int,float)) else None
                )
            )
        return chunks
    except Exception as e:
        print(f"❌ Error searching ChromaDB: {e}")
        return []

# ---------- wikipedia fallback ----------
def fetch_wikipedia_summary(topic: str, sentences: int = 5) -> Optional[Dict[str, Any]]:
    """Uses Wikipedia library for fallback"""
    try:
        summary = wikipedia.summary(topic, sentences=sentences)
        if not summary:
            return None
        return {
            "title": topic,
            "summary": summary,
            "source": "Wikipedia"
        }
    except Exception as e:
        print(f"❌ Wikipedia fetch error: {e}")
        return None

# ---------- llm (OpenAI) ----------
def build_llm_client() -> Optional[OpenAI]:
    if not OPENAI_API_KEY:
        return None
    return OpenAI(api_key=OPENAI_API_KEY)

def synthesize_answer(
    query: str, 
    pdf_chunks: List[RetrievedChunk], 
    gcs_data: Optional[Dict[str, Any]],
    wiki: Optional[Dict[str, Any]]
) -> str:
    """Synthesize answer from available sources"""
    sources_text = ""
    
    # Priority 1: PDF chunks from ChromaDB
    if pdf_chunks:
        bullets = []
        for c in pdf_chunks:
            cite = []
            if c.section: cite.append(f"section: {c.section}")
            if c.page is not None: cite.append(f"page: {c.page}")
            cite_str = f" ({', '.join(cite)})" if cite else ""
            bullets.append(f"- {c.content[:700]}{cite_str}")
        sources_text = "PDF EXCERPTS:\n" + "\n".join(bullets)
    
    # Priority 2: GCS seed concepts
    elif gcs_data:
        summary = gcs_data.get("summary", "") or gcs_data.get("answer", "") or str(gcs_data)
        sources_text = f"SEED CONCEPT (from GCS):\n- {summary[:1500]}"
    
    # Priority 3: Wikipedia fallback
    elif wiki:
        sources_text = f"WIKIPEDIA:\n- {wiki.get('summary','')}"

    system = (
        "You are an expert financial advisor. Answer clearly using the provided context. "
        "Do not invent citations—stick to section/page when present."
    )
    user = f"QUESTION:\n{query}\n\nCONTEXT:\n{sources_text}"

    client = build_llm_client()
    if not client:
        return f"(dev mode / no LLM)\n\n{user[:3500]}"

    try:
        resp = client.chat.completions.create(
            model=ANSWER_MODEL,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            temperature=0.2,
        )
        return resp.choices[0].message.content.strip()
    except Exception as e:
        print(f"❌ OpenAI error: {e}")
        return f"Error generating answer: {str(e)}"

# ---------- cache helpers (PostgreSQL like DAG) ----------
def cache_get_pg(query: str) -> Optional[Dict[str, Any]]:
    """Get cached result from PostgreSQL (like DAG)"""
    conn = get_pg_conn()
    if conn is None:
        print("⚠️  Cache get skipped - no database connection")
        return None
    
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT answer_json FROM rag_cache WHERE query = %s", (query,))
            row = cur.fetchone()
            if row:
                cached_data = row[0]
                if isinstance(cached_data, dict):
                    return cached_data
            return None
    except Exception as e:
        print(f"❌ Cache get error: {e}")
        return None
    finally:
        if conn:
            conn.close()

def cache_put_pg(query: str, collection: str, gen_model: str, embed_model: str,
                 answer_text: str, answer_json: dict, sources_json: list, retrieval_source: str) -> None:
    """Put result in PostgreSQL cache (like DAG)"""
    conn = get_pg_conn()
    if conn is None:
        print("⚠️  Cache put skipped - no database connection")
        return
    
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO rag_cache (query, collection_name, gen_model, embed_model,
                                       answer_text, answer_json, sources_json, retrieval_source)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (query)
                DO UPDATE SET 
                    collection_name = EXCLUDED.collection_name,
                    gen_model = EXCLUDED.gen_model,
                    embed_model = EXCLUDED.embed_model,
                    answer_text = EXCLUDED.answer_text,
                    answer_json = EXCLUDED.answer_json,
                    sources_json = EXCLUDED.sources_json,
                    retrieval_source = EXCLUDED.retrieval_source,
                    created_at = CURRENT_TIMESTAMP
            """, (
                query, collection, gen_model, embed_model,
                answer_text, json.dumps(answer_json), json.dumps(sources_json), retrieval_source
            ))
    except Exception as e:
        print(f"❌ Cache put error: {e}")
    finally:
        if conn:
            conn.close()

# ---------- pydantic models ----------
class QueryRequest(BaseModel):
    query: str = Field(..., description="The user's question or concept")
    refresh: bool = Field(False, description="If true, bypass cache and regenerate")

class SourceCite(BaseModel):
    type: Literal["pdf", "gcs", "wikipedia"]
    page: Optional[int] = None
    section: Optional[str] = None
    url: Optional[str] = None
    title: Optional[str] = None
    gcs_path: Optional[str] = None  # Added for GCS sources

class QueryResponse(BaseModel):
    query: str
    answer: str
    used_cache: bool
    sources: List[SourceCite]

class SeedRequest(BaseModel):
    concepts: List[str]

class SeedResponse(BaseModel):
    seeded: List[str]
    failures: List[str]

class ConceptListResponse(BaseModel):
    concepts: List[str]
    count: int

# ---------- lifespan ----------
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    print("🚀 Starting up AURELIA...")
    
    # Initialize cache table (optional)
    try:
        ensure_cache_table()
    except Exception as e:
        print(f"⚠️  Cache initialization failed: {e}")
        print("⚠️  Application will continue without caching")
    
    # Test GCS connection
    try:
        client = get_gcs_client()
        if client:
            bucket = client.bucket(GCP_BUCKET_NAME)
            if bucket.exists():
                print(f"✅ Connected to GCS bucket: {GCP_BUCKET_NAME}")
            else:
                print(f"⚠️  GCS bucket not found: {GCP_BUCKET_NAME}")
        else:
            print("⚠️  GCS client not initialized")
    except Exception as e:
        print(f"⚠️  GCS connection test failed: {e}")
    
    # Test ChromaDB connection
    try:
        col = get_chroma_collection()
        if col:
            print("✅ ChromaDB connection successful")
        else:
            print("⚠️  ChromaDB not available")
    except Exception as e:
        print(f"⚠️  ChromaDB connection test failed: {e}")
    
    yield
    # Shutdown
    print("🛑 Shutting down...")

# ---------- app initialization ----------
app = FastAPI(title=APP_NAME, version="1.0.0", lifespan=lifespan)

# ---------- endpoints ----------
@app.get("/")
def root():
    return {
        "service": APP_NAME,
        "status": "running",
        "docs": "/docs",
        "health": "/healthz",
        "version": "1.0.0"
    }

@app.get("/healthz")
def healthz():
    return {"ok": True, "service": APP_NAME, "time": int(time.time())}

@app.get("/concepts", response_model=ConceptListResponse)
def list_concepts():
    """List all available concepts from GCS bucket"""
    concepts = list_all_concepts_from_gcs()
    return ConceptListResponse(concepts=concepts, count=len(concepts))

@app.post("/query", response_model=QueryResponse)
def query(req: QueryRequest):
    q = req.query.strip()
    if not q:
        raise HTTPException(status_code=400, detail="query cannot be empty")

    # 1) Check cache first (like DAG)
    if not req.refresh:
        cached = cache_get_pg(q.lower())
        if cached and "answer" in cached:
            srcs = []
            for s in cached.get("sources", []):
                srcs.append(SourceCite(**s))
            return QueryResponse(
                query=q, 
                answer=cached["answer"], 
                used_cache=True, 
                sources=srcs
            )

    # 2) Search ChromaDB for PDF chunks
    pdf_hits = search_pdf_chunks(q, k=PDF_TOP_K)
    
    # 3) Search GCS for seed concepts (only if no PDF hits)
    gcs_data = search_gcs_concepts(q) if not pdf_hits else None

    # 4) Wikipedia fallback (only if no PDF hits and no GCS data)
    wiki = None
    if not pdf_hits and not gcs_data and USE_WIKI:
        wiki = fetch_wikipedia_summary(q)

    if not pdf_hits and not gcs_data and not wiki:
        raise HTTPException(
            status_code=404, 
            detail="No sources found in PDF, seed concepts, or Wikipedia."
        )

    # 5) LLM synthesis
    answer = synthesize_answer(q, pdf_hits, gcs_data, wiki)

    # 6) Prepare citations
    cites: List[SourceCite] = []
    retrieval_source = "pdf"
    
    if pdf_hits:
        for c in pdf_hits:
            cites.append(SourceCite(
                type="pdf", 
                page=c.page, 
                section=c.section
            ))
        retrieval_source = "pdf"
    elif gcs_data:
        cites.append(SourceCite(
            type="gcs",
            gcs_path=f"gs://{GCP_BUCKET_NAME}/{GCP_SEED_FOLDER}",
            title="Seed Concept from GCS"
        ))
        retrieval_source = "gcs"
    elif wiki:
        cites.append(SourceCite(
            type="wikipedia", 
            title=wiki.get("title"),
            url=f"https://en.wikipedia.org/wiki/{wiki.get('title', '').replace(' ', '_') if wiki.get('title') else ''}"
        ))
        retrieval_source = "wikipedia"

    # 7) Save to cache (like DAG)
    cache_put_pg(
        query=q.lower(),
        collection=CHROMA_COLLECTION,
        gen_model=ANSWER_MODEL,
        embed_model="text-embedding-3-large",
        answer_text=answer,
        answer_json={"answer": answer, "sources": [c.model_dump() for c in cites]},
        sources_json=[c.model_dump() for c in cites],
        retrieval_source=retrieval_source
    )

    return QueryResponse(query=q, answer=answer, used_cache=False, sources=cites)

@app.post("/seed", response_model=SeedResponse)
def seed(req: SeedRequest):
    """Seed concepts by querying them and caching results"""
    seeded, failures = [], []
    for concept in req.concepts:
        try:
            _ = query(QueryRequest(query=concept, refresh=True))
            seeded.append(concept)
        except Exception as e:
            print(f"❌ Failed to seed {concept}: {e}")
            failures.append(concept)
    return SeedResponse(seeded=seeded, failures=failures)