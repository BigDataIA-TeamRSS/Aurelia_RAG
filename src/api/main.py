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
import psycopg
from psycopg.rows import dict_row
from contextlib import asynccontextmanager


# Optional: Chroma HTTP client (works with chroma server)
import chromadb
from chromadb.config import Settings

# Optional: OpenAI (LLM for final answer)
from openai import OpenAI

# ---------- boot ----------
load_dotenv(override=True)

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
import psycopg
from psycopg.rows import dict_row
from contextlib import asynccontextmanager

# ChromaDB Cloud client
import chromadb
from chromadb.config import Settings

# OpenAI (LLM for final answer)
from openai import OpenAI

# ---------- boot ----------
load_dotenv(override=True)

APP_NAME = "aurelia-fastapi"
PDF_TOP_K = int(os.getenv("PDF_TOP_K", "5"))
USE_WIKI = os.getenv("WIKI_FALLBACK", "1") == "1"

# vector store config - CHROMADB CLOUD
CHROMA_HOST = os.getenv("CHROMA_SERVER_HOST", "https://api.trychroma.com")
CHROMADB_API_KEY = os.getenv("CHROMADB_API_KEY", "ck-BrgcfXPxcyK22Lir6SGqqiVQsEaU5EE8mDbQ71omUUg8")
CHROMA_TENANT_ID = os.getenv("CHROMA_TENANT_ID", "default_tenant")
CHROMA_DATABASE = os.getenv("CHROMA_DATABASE", "AURELIA_CHROMA_DB")
CHROMA_COLLECTION = os.getenv("CHROMA_COLLECTION", "fintbx_concepts")

# GCP Storage config
GCP_BUCKET_NAME = os.getenv("GCP_BUCKET_NAME", "aurelia-concepts")
GCP_SEED_FOLDER = os.getenv("GCP_SEED_FOLDER", "seed_concept/out/")
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
print(f"📁 Base directory: {BASE_DIR}")

# Build absolute path to service-account.json
GCP_CREDENTIALS_PATH = os.path.join(BASE_DIR, "service-account.json")
print(f"🔑 GCP credentials path: {GCP_CREDENTIALS_PATH}")
print(f"✅ File exists: {os.path.exists(GCP_CREDENTIALS_PATH)}")
# GCP_CREDENTIALS_PATH = os.getenv("GCP_CREDENTIALS_PATH")  # Fixed path

# postgres config
PG_HOST = os.getenv("PG_HOST", "127.0.0.1")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB = os.getenv("PG_DB", "rag_DB")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "postgres")

# llm config
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
ANSWER_MODEL = os.getenv("ANSWER_MODEL", "gpt-4o-mini")

# ---------- GCP Storage Client ----------
def get_gcs_client():
    """Initialize GCP Storage client"""
    try:
        if GCP_CREDENTIALS_PATH and os.path.exists(GCP_CREDENTIALS_PATH):
            credentials = service_account.Credentials.from_service_account_file(
                GCP_CREDENTIALS_PATH
            )
            return storage.Client(credentials=credentials)
        else:
            # Use default credentials (for GCP environments)
            print("⚠️  Using default GCP credentials")
            return storage.Client()
    except Exception as e:
        print(f"⚠️  GCP client initialization warning: {e}")
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

# ---------- db (psycopg3) ----------
def get_pg_conn():
    return psycopg.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
        row_factory=dict_row,
        autocommit=True,
    )

def ensure_cache_table():
    try:
        with get_pg_conn() as conn, conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS concept_cache (
              concept TEXT PRIMARY KEY,
              answer TEXT NOT NULL,
              sources JSONB NOT NULL,
              created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """)
        print("✅ Cache table ready")
    except Exception as e:
        print(f"⚠️  Cache table creation skipped: {e}")

# ---------- vector store (ChromaDB Cloud) ----------
def get_chroma_collection():
    """Connect to ChromaDB Cloud"""
    if not CHROMADB_API_KEY:
        raise ValueError("CHROMADB_API_KEY is required for ChromaDB Cloud")
    
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
        raise

@dataclass
class RetrievedChunk:
    content: str
    page: Optional[int] = None
    section: Optional[str] = None
    source: Literal["pdf", "gcs"] = "pdf"
    score: Optional[float] = None

def search_gcs_concepts(query: str) -> Optional[Dict[str, Any]]:
    """
    Search for concept data in GCS bucket
    Returns the concept data if found
    """
    try:
        # Try to fetch the specific concept
        concept_data = fetch_concept_from_gcs(query)
        
        if concept_data:
            return concept_data
        
        # If not found, try to find similar concepts
        all_concepts = list_all_concepts_from_gcs()
        query_lower = query.lower()
        
        for concept in all_concepts:
            if query_lower in concept.lower():
                return fetch_concept_from_gcs(concept)
        
        return None
        
    except Exception as e:
        print(f"❌ Error searching GCS concepts: {e}")
        return None
    
def search_pdf_chunks(query: str, k: int = PDF_TOP_K) -> List[RetrievedChunk]:
    """Search ChromaDB Cloud for relevant chunks"""
    try:
        col = get_chroma_collection()
        res = col.query(
            query_texts=[query], 
            n_results=k, 
            include=["documents", "metadatas", "distances"]
        )
        chunks: List[RetrievedChunk] = []
        if not res or not res.get("documents"):
            return chunks
        for doc, meta, dist in zip(res["documents"][0], res["metadatas"][0], res.get("distances", [[None]])[0]):
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
def fetch_wikipedia_summary(topic: str, sentences: int = 3) -> Optional[Dict[str, Any]]:
    """Uses Wikipedia REST summary API"""
    url = f"https://en.wikipedia.org/api/rest_v1/page/summary/{requests.utils.quote(topic)}"
    try:
        r = requests.get(url, timeout=10)
        if r.status_code != 200:
            return None
        data = r.json()
        text = data.get("extract") or ""
        if not text:
            return None
        # trim to roughly N sentences
        parts = text.split(". ")
        short = ". ".join(parts[:sentences]).strip()
        if short and not short.endswith("."):
            short += "."
        return {
            "url": data.get("content_urls", {}).get("desktop", {}).get("page"),
            "title": data.get("title"),
            "summary": short
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
    """
    Synthesize answer from multiple sources:
    1. ChromaDB PDF chunks
    2. GCS seed concepts
    3. Wikipedia fallback
    """
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
        if isinstance(gcs_data, dict):
            # Extract relevant info from GCS data
            summary = gcs_data.get("summary", "") or gcs_data.get("answer", "") or str(gcs_data)
            sources_text = f"SEED CONCEPT (from GCS):\n- {summary[:1500]}"
        else:
            sources_text = f"SEED CONCEPT (from GCS):\n- {str(gcs_data)[:1500]}"
    
    # Priority 3: Wikipedia fallback
    elif wiki:
        sources_text = f"WIKIPEDIA:\n- {wiki.get('summary','')}\n- url: {wiki.get('url','')}"

    system = (
        "You are an expert financial advisor and technical writer. "
        "Answer clearly and concisely using the provided context. "
        "Prioritize information in this order: PDF excerpts, seed concepts from internal database, then Wikipedia. "
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

# ---------- cache helpers ----------
def cache_get(concept: str) -> Optional[Dict[str, Any]]:
    try:
        with get_pg_conn() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT concept, answer, sources, created_at FROM concept_cache WHERE concept=%s", 
                (concept,)
            )
            row = cur.fetchone()
            return row if row else None
    except Exception as e:
        print(f"❌ Cache get error: {e}")
        return None

def cache_put(concept: str, answer: str, sources: List[Dict[str, Any]]) -> None:
    try:
        with get_pg_conn() as conn, conn.cursor() as cur:
            cur.execute("""
            INSERT INTO concept_cache (concept, answer, sources)
            VALUES (%s, %s, %s)
            ON CONFLICT (concept) DO UPDATE
              SET answer=EXCLUDED.answer, sources=EXCLUDED.sources, created_at=NOW()
            """, (concept, answer, json.dumps(sources)))
    except Exception as e:
        print(f"❌ Cache put error: {e}")

# ---------- pydantic models ----------
class QueryRequest(BaseModel):
    query: str = Field(..., description="The user's question or concept")
    refresh: bool = Field(False, description="If true, bypass cache and regenerate")

class SourceCite(BaseModel):
    type: Literal["pdf", "gcs", "wikipedia"]  # FIXED: Added "gcs"
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
    ensure_cache_table()
    
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

    # 1) Check cache
    if not req.refresh:
        cached = cache_get(q.lower())
        if cached:
            srcs = []
            for s in cached["sources"]:
                srcs.append(SourceCite(**s))
            return QueryResponse(
                query=q, 
                answer=cached["answer"], 
                used_cache=True, 
                sources=srcs
            )

    # 2) Search ChromaDB for PDF chunks
    pdf_hits = search_pdf_chunks(q, k=PDF_TOP_K)
    
    # 3) Search GCS for seed concepts
    gcs_data = search_gcs_concepts(q) if not pdf_hits else None

    # 4) Wikipedia fallback
    wiki = fetch_wikipedia_summary(q) if (USE_WIKI and not pdf_hits and not gcs_data) else None

    if not pdf_hits and not gcs_data and not wiki:
        raise HTTPException(
            status_code=404, 
            detail="No sources found in PDF, seed concepts, or Wikipedia."
        )

    # 5) LLM synthesis
    answer = synthesize_answer(q, pdf_hits, gcs_data, wiki)

    # 6) Prepare citations
    cites: List[SourceCite] = []
    if pdf_hits:
        for c in pdf_hits:
            cites.append(SourceCite(
                type="pdf", 
                page=c.page, 
                section=c.section
            ))
    elif gcs_data:
        cites.append(SourceCite(
            type="gcs",
            gcs_path=f"gs://{GCP_BUCKET_NAME}/{GCP_SEED_FOLDER}",
            title="Seed Concept from GCS"
        ))
    elif wiki:
        cites.append(SourceCite(
            type="wikipedia", 
            url=wiki.get("url"), 
            title=wiki.get("title")
        ))

    # 7) Save to cache
    cache_put(q.lower(), answer, [c.model_dump() for c in cites])

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