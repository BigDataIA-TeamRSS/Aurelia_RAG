import os
import json
import time
from typing import List, Optional, Literal, Dict, Any
from dataclasses import dataclass

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from dotenv import load_dotenv

import requests
import psycopg
from psycopg.rows import dict_row

# Optional: Chroma HTTP client (works with chroma server)
import chromadb
from chromadb.config import Settings

# Optional: OpenAI (LLM for final answer)
from openai import OpenAI

# ---------- boot ----------
load_dotenv(override=True)

APP_NAME = "aurelia-fastapi"
PDF_TOP_K = int(os.getenv("PDF_TOP_K", "5"))
USE_WIKI = os.getenv("WIKI_FALLBACK", "1") == "1"

# vector store config
CHROMADB_HOST = os.getenv("CHROMADB_HOST", "localhost")
CHROMADB_PORT = int(os.getenv("CHROMADB_PORT", "8000"))
CHROMA_COLLECTION = os.getenv("CHROMA_COLLECTION", "fintbx_concepts")
CHROMADB_API_KEY = os.getenv("CHROMADB_API_KEY", "")

# postgres config
PG_HOST = os.getenv("PG_HOST", "127.0.0.1")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB = os.getenv("PG_DB", "rag_DB")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "postgres")

# llm config
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
ANSWER_MODEL = os.getenv("ANSWER_MODEL", "gpt-4o-mini")

# ---------- app ----------
app = FastAPI(title=APP_NAME, version="1.0.0")

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
    with get_pg_conn() as conn, conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS concept_cache (
          concept TEXT PRIMARY KEY,
          answer TEXT NOT NULL,
          sources JSONB NOT NULL,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """)

# ---------- vector store (Chroma) ----------
def get_chroma_collection():
<<<<<<< HEAD
    try:
        # If you're running chroma server: `chroma run --path <dir> --port 8000`
        client = chromadb.HttpClient(
            host=CHROMADB_HOST,
            port=CHROMADB_PORT,
            settings=Settings(allow_reset=False, anonymized_telemetry=False, headers={
                "Authorization": f"Bearer {CHROMADB_API_KEY}"
            } if CHROMADB_API_KEY else None),
        )
        return client.get_or_create_collection(name=CHROMA_COLLECTION, metadata={"hnsw:space": "cosine"})
    except Exception as e:
        print(f"Warning: Could not connect to ChromaDB: {e}")
        return None
=======
    # If you’re running chroma server: `chroma run --path <dir> --port 8000`
    client = chromadb.HttpClient(
        host=CHROMADB_HOST,
        port=CHROMADB_PORT,
        settings=Settings(allow_reset=False, anonymized_telemetry=False, headers={
            "Authorization": f"Bearer {CHROMADB_API_KEY}"
        } if CHROMADB_API_KEY else None),
    )
    return client.get_or_create_collection(name=CHROMA_COLLECTION, metadata={"hnsw:space": "cosine"})
>>>>>>> 2effb648194d6c5260b340d210873751f7046573

@dataclass
class RetrievedChunk:
    content: str
    page: Optional[int] = None
    section: Optional[str] = None
    source: Literal["pdf"] = "pdf"
    score: Optional[float] = None

def search_pdf_chunks(query: str, k: int = PDF_TOP_K) -> List[RetrievedChunk]:
    col = get_chroma_collection()
<<<<<<< HEAD
    if col is None:
        print("ChromaDB not available, returning empty results")
        return []
    
    try:
        res = col.query(query_texts=[query], n_results=k, include=["documents", "metadatas", "distances"])
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
        print(f"Error searching ChromaDB: {e}")
        return []
=======
    res = col.query(query_texts=[query], n_results=k, include=["documents", "metadatas", "distances"])
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
>>>>>>> 2effb648194d6c5260b340d210873751f7046573

# ---------- wikipedia fallback ----------
def fetch_wikipedia_summary(topic: str, sentences: int = 3) -> Optional[Dict[str, Any]]:
    """
    Uses Wikipedia REST summary API (no key).
    """
    url = f"https://en.wikipedia.org/api/rest_v1/page/summary/{requests.utils.quote(topic)}"
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

# ---------- llm (OpenAI) ----------
def build_llm_client() -> Optional[OpenAI]:
    if not OPENAI_API_KEY:
        return None
    return OpenAI(api_key=OPENAI_API_KEY)

def synthesize_answer(query: str, pdf_chunks: List[RetrievedChunk], wiki: Optional[Dict[str, Any]]) -> str:
    """
    Simple instruction: prefer PDF chunks; if empty, use wiki.
    """
    sources_text = ""
    if pdf_chunks:
        bullets = []
        for c in pdf_chunks:
            cite = []
            if c.section: cite.append(f"section: {c.section}")
            if c.page is not None: cite.append(f"page: {c.page}")
            cite_str = f" ({', '.join(cite)})" if cite else ""
            bullets.append(f"- {c.content[:700]}{cite_str}")
        sources_text = "PDF EXCERPTS:\n" + "\n".join(bullets)
    elif wiki:
        sources_text = f"WIKIPEDIA:\n- {wiki.get('summary','')}\n- url: {wiki.get('url','')}"

    system = (
        "You are an expert technical writer. "
        "Answer clearly and concisely using the provided context. "
        "Prefer the PDF excerpts; only use Wikipedia if PDF is empty. "
        "Do not invent citations—stick to section/page when present."
    )
    user = f"QUESTION:\n{query}\n\nCONTEXT:\n{sources_text}"

    client = build_llm_client()
    if not client:
        # fallback if no key—return stitched context (good for local smoke tests)
        return f"(dev mode / no LLM)\n\n{user[:3500]}"

    resp = client.chat.completions.create(
        model=ANSWER_MODEL,
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        temperature=0.2,
    )
    return resp.choices[0].message.content.strip()

# ---------- cache helpers ----------
def cache_get(concept: str) -> Optional[Dict[str, Any]]:
<<<<<<< HEAD
    try:
        with get_pg_conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT concept, answer, sources, created_at FROM concept_cache WHERE concept=%s", (concept,))
            row = cur.fetchone()
            return row if row else None
    except Exception as e:
        print(f"Warning: Could not access cache: {e}")
        return None

def cache_put(concept: str, answer: str, sources: Dict[str, Any]) -> None:
    try:
        with get_pg_conn() as conn, conn.cursor() as cur:
            cur.execute("""
            INSERT INTO concept_cache (concept, answer, sources)
            VALUES (%s, %s, %s)
            ON CONFLICT (concept) DO UPDATE
              SET answer=EXCLUDED.answer, sources=EXCLUDED.sources, created_at=NOW()
            """, (concept, answer, json.dumps(sources)))
    except Exception as e:
        print(f"Warning: Could not save to cache: {e}")
=======
    with get_pg_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT concept, answer, sources, created_at FROM concept_cache WHERE concept=%s", (concept,))
        row = cur.fetchone()
        return row if row else None

def cache_put(concept: str, answer: str, sources: Dict[str, Any]) -> None:
    with get_pg_conn() as conn, conn.cursor() as cur:
        cur.execute("""
        INSERT INTO concept_cache (concept, answer, sources)
        VALUES (%s, %s, %s)
        ON CONFLICT (concept) DO UPDATE
          SET answer=EXCLUDED.answer, sources=EXCLUDED.sources, created_at=NOW()
        """, (concept, answer, json.dumps(sources)))
>>>>>>> 2effb648194d6c5260b340d210873751f7046573

# ---------- pydantic models ----------
class QueryRequest(BaseModel):
    query: str = Field(..., description="The user's question or concept")
    refresh: bool = Field(False, description="If true, bypass cache and regenerate")

class SourceCite(BaseModel):
    type: Literal["pdf","wikipedia"]
    page: Optional[int] = None
    section: Optional[str] = None
    url: Optional[str] = None
    title: Optional[str] = None

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

# ---------- startup ----------
@app.on_event("startup")
def on_startup():
<<<<<<< HEAD
    try:
        ensure_cache_table()
    except Exception as e:
        print(f"Warning: Could not initialize database cache: {e}")
        print("Application will continue without caching functionality")
=======
    ensure_cache_table()
>>>>>>> 2effb648194d6c5260b340d210873751f7046573

# ---------- endpoints ----------
@app.get("/healthz")
def healthz():
<<<<<<< HEAD
    return {
        "ok": True, 
        "service": APP_NAME, 
        "time": int(time.time()),
        "status": "healthy"
    }

@app.get("/")
def root():
    return {
        "message": f"Welcome to {APP_NAME}",
        "version": "1.0.0",
        "endpoints": ["/healthz", "/query", "/seed", "/docs"]
    }
=======
    return {"ok": True, "service": APP_NAME, "time": int(time.time())}
>>>>>>> 2effb648194d6c5260b340d210873751f7046573

@app.post("/query", response_model=QueryResponse)
def query(req: QueryRequest):
    q = req.query.strip()
    if not q:
        raise HTTPException(status_code=400, detail="query cannot be empty")

    # 1) cache
    if not req.refresh:
        cached = cache_get(q.lower())
        if cached:
            srcs = []
            for s in cached["sources"]:
                srcs.append(SourceCite(**s))
            return QueryResponse(query=q, answer=cached["answer"], used_cache=True, sources=srcs)

    # 2) vector search on PDF
    pdf_hits = search_pdf_chunks(q, k=PDF_TOP_K)

    # 3) wikipedia fallback (optional)
    wiki = fetch_wikipedia_summary(q) if (USE_WIKI and not pdf_hits) else None

    if not pdf_hits and not wiki:
        raise HTTPException(status_code=404, detail="No sources found in PDF or Wikipedia.")

    # 4) LLM synthesis
    answer = synthesize_answer(q, pdf_hits, wiki)

    # 5) prepare citations
    cites: List[SourceCite] = []
    if pdf_hits:
        for c in pdf_hits:
            cites.append(SourceCite(type="pdf", page=c.page, section=c.section))
    elif wiki:
        cites.append(SourceCite(type="wikipedia", url=wiki.get("url"), title=wiki.get("title")))

    # 6) save to cache
    cache_put(q.lower(), answer, [c.model_dump() for c in cites])

    return QueryResponse(query=q, answer=answer, used_cache=False, sources=cites)

@app.post("/seed", response_model=SeedResponse)
def seed(req: SeedRequest):
    seeded, failures = [], []
    for concept in req.concepts:
        try:
            # force refresh behavior
            _ = query(QueryRequest(query=concept, refresh=True))
            seeded.append(concept)
        except Exception:
            failures.append(concept)
    return SeedResponse(seeded=seeded, failures=failures)
