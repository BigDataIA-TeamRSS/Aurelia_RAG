from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional, List, Dict
import os
from datetime import datetime
import psycopg2
import json

from ..vector_store.pinecone_client import PineconeVectorStore
from ..utils.instructor_client import InstructorClient
from ..utils.wikipedia_fallback import WikipediaFallback

app = FastAPI(title="AURELIA RAG Service")

# Initialize components
vector_store = PineconeVectorStore()
instructor_client = InstructorClient()
wikipedia_fallback = WikipediaFallback()

# Database connection
def get_db_connection():
    return psycopg2.connect(
        host=os.getenv('POSTGRES_HOST'),
        database=os.getenv('POSTGRES_DB'),
        user=os.getenv('POSTGRES_USER'),
        password=os.getenv('POSTGRES_PASSWORD')
    )

class QueryRequest(BaseModel):
    concept: str
    force_regenerate: bool = False

class ConceptNote(BaseModel):
    concept: str
    definition: str
    key_points: List[str]
    formulas: Optional[List[str]]
    examples: Optional[List[str]]
    related_concepts: List[str]
    source: str
    citations: List[Dict[str, str]]
    generated_at: datetime

@app.post("/query", response_model=ConceptNote)
async def query_concept(request: QueryRequest):
    """Query for a financial concept"""
    
    # Check cache first
    if not request.force_regenerate:
        cached = get_cached_concept(request.concept)
        if cached:
            return cached
    
    # Query vector store
    matches = vector_store.query(request.concept, top_k=5)
    
    if not matches or len(matches) == 0:
        # Fallback to Wikipedia
        return await wikipedia_fallback.get_concept(request.concept)
    
    # Build context from matches
    context = build_context_from_matches(matches)
    
    # Generate structured note using instructor
    note = await instructor_client.generate_concept_note(
        concept=request.concept,
        context=context,
        matches=matches
    )
    
    # Cache the result
    cache_concept(note)
    
    return note

@app.post("/seed")
async def seed_concept(request: QueryRequest):
    """Seed a new concept into the database"""
    note = await query_concept(request)
    return {"status": "success", "concept": note.concept}

def get_cached_concept(concept: str) -> Optional[ConceptNote]:
    """Retrieve cached concept from PostgreSQL"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute(
        "SELECT data FROM concept_notes WHERE concept = %s",
        (concept.lower(),)
    )
    
    result = cur.fetchone()
    cur.close()
    conn.close()
    
    if result:
        return ConceptNote(**json.loads(result[0]))
    return None

def cache_concept(note: ConceptNote):
    """Cache concept in PostgreSQL"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute(
        """
        INSERT INTO concept_notes (concept, data, created_at)
        VALUES (%s, %s, %s)
        ON CONFLICT (concept) DO UPDATE
        SET data = %s, updated_at = %s
        """,
        (
            note.concept.lower(),
            json.dumps(note.dict()),
            datetime.now(),
            json.dumps(note.dict()),
            datetime.now()
        )
    )
    
    conn.commit()
    cur.close()
    conn.close()

def build_context_from_matches(matches) -> str:
    """Build context string from vector search matches"""
    context_parts = []
    for match in matches:
        metadata = match.metadata
        context_parts.append(
            f"[Page {metadata.get('page', 'N/A')}, "
            f"Section: {metadata.get('section', 'General')}]\n"
            f"{match.metadata.get('text', '')}\n"
        )
    return "\n---\n".join(context_parts)