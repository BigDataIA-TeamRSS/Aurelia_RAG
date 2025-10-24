import os
import json
from typing import List, Optional, Literal
from pydantic import BaseModel
from contextlib import closing
# from google.cloud import storage  # Moved inside functions where needed
from langchain_openai import OpenAIEmbeddings
import chromadb

# import psycopg2  # Commented out for cloud compatibility
import sys

# Ensure project-level imports work
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from pdf_processor.parser_chunker import PDFProcessor


# ------------------------
# Models
# ------------------------
class SourceChunk(BaseModel):
    id: str
    score: float
    sec: Optional[str] = None
    page_s: Optional[int] = None
    page_e: Optional[int] = None

class RAGAnswer(BaseModel):
    answer: str
    reasoning: Optional[str] = None
    sources: List[SourceChunk]
    query: str
    served_from: Optional[Literal["cache","rag"]] = None

# ------------------------
# Environment Detection
# ------------------------

IS_RUNNING_IN_CLOUD = os.getenv('AIRFLOW_ENV') is not None and 'composer' in os.getenv('AIRFLOW_ENV', '')
if not IS_RUNNING_IN_CLOUD:
    import psycopg2

# ------------------------
# Postgres Cache Utils
# ------------------------
PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")
PG_DB = os.getenv("PG_DB")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
CLOUD_SQL_INSTANCE = os.getenv("CLOUD_SQL_INSTANCE", "aurelia-475916:us-central1:cache-db")

def get_pg_conn():
    if IS_RUNNING_IN_CLOUD:
        # CLOUD: Use GCP Cloud SQL connector
        try:
            from google.cloud.sql.connector import Connector
            
            # HARDCODED VALUES FOR DEBUGGING
            print("🔧 DEBUG: Using hardcoded Cloud SQL connection details")
            instance_connection_name = "aurelia-475916:us-central1:cache-db"
            db_user = "postgres"
            db_pass = "postGre@123"
            db_name = "rag_DB"
            
            print(f"🔧 DEBUG: Connection details - instance: {instance_connection_name}")
            print(f"🔧 DEBUG: Connection details - user: {db_user}")
            print(f"🔧 DEBUG: Connection details - pass: {'***' if db_pass else 'None'}")
            print(f"🔧 DEBUG: Connection details - db: {db_name}")

            if not all([instance_connection_name, db_user, db_pass, db_name]):
                print("⚠️  Cloud SQL connection details not found. Skipping PostgreSQL cache.")
                return None
            
            # Use Cloud SQL connector
            print("🔧 DEBUG: Creating Cloud SQL connector...")
            connector = Connector()
            print("🔧 DEBUG: Attempting to connect to Cloud SQL...")
            conn = connector.connect(
                instance_connection_name,
                "pg8000",
                user=db_user,
                password=db_pass,
                db=db_name,
            )
            print(f"✅ DEBUG: Cloud SQL connection successful: {conn}")
            return conn
            
        except ImportError as e:
            print(f"⚠️  Cloud SQL connector not available: {e}. Skipping PostgreSQL cache.")
            return None
        except Exception as e:
            print(f"⚠️  Cloud SQL connection failed: {e}. Skipping PostgreSQL cache.")
            print(f"🔧 DEBUG: Exception type: {type(e).__name__}")
            print(f"🔧 DEBUG: Exception details: {str(e)}")
            return None
    else:
        # LOCAL DEVELOPMENT: Use direct connection (commented out for cloud compatibility)
        if not all([PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD]):
            print("⚠️  PostgreSQL connection details not found. Skipping cache.")
            return None
        
        return psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            dbname=PG_DB,
            user=PG_USER,
            password=PG_PASSWORD
        )

def cache_get_pg(query: str):
    print(f"🔍 DEBUG: Attempting to get cache for query: {query[:50]}...")
    conn = get_pg_conn()
    print(f"✅ DEBUG: Cache get connection successful: {conn}")
    if conn is None:
        print("⚠️  Cache get skipped - no database connection")
        return None
    
    try:
        with closing(conn) as conn:
            cur = conn.cursor()
            print(f"✅ Cache get cursor successful: {cur}")
            try:
                cur.execute("SELECT answer_json FROM rag_cache WHERE query = %s", (query,))
                row = cur.fetchone()
                if row:
                    print(f"✅ Cache get row successful: {row}")
                    # answer_json is already a Python dict (JSONB), no need to parse
                    cached_data = row[0]
                    
                    # Validate that we have the expected structure
                    if not isinstance(cached_data, dict):
                        print(f"⚠️  DEBUG: Cached data is not a dict: {type(cached_data)}")
                        return None
                    
                    # Check if this is a concept note (has 'concept' field)
                    if "concept" in cached_data:
                        print(f"✅ DEBUG: Found concept note in cache: {cached_data.get('concept', 'Unknown')}")
                        return cached_data
                    # Check if this is a RAG answer (has 'sources' field)
                    elif "sources" in cached_data:
                        print(f"✅ DEBUG: Found RAG answer in cache")
                        return cached_data
                    else:
                        print(f"⚠️  DEBUG: Unknown cached data format. Keys: {list(cached_data.keys())}")
                        return None
                return None
            finally:
                cur.close()
    except Exception as e:
        print(f"⚠️  Cache get failed: {e}. Skipping cache.")
        return None

def cache_put_pg(query: str, collection: str, gen_model: str, embed_model: str,
                 answer_text: str, answer_json: dict, sources_json: list, retrieval_source: str):
    print(f"💾 DEBUG: Attempting to cache query: {query[:50]}...")
    conn = get_pg_conn()
    print(f"✅ DEBUG: Cache put connection successful: {conn}")
    if conn is None:
        print("⚠️  Cache put skipped - no database connection")
        return
    
    print(f"💾 DEBUG: Caching query: {query[:50]}...")
    
    # Validate input data
    if not query or not answer_json:
        print(f"⚠️  DEBUG: Invalid cache data - query: {bool(query)}, answer_json: {bool(answer_json)}")
        return
    
    if not isinstance(answer_json, dict):
        print(f"⚠️  DEBUG: answer_json must be a dict, got: {type(answer_json)}")
        return
    
    if not isinstance(sources_json, list):
        print(f"⚠️  DEBUG: sources_json must be a list, got: {type(sources_json)}")
        return
    
    try:
        with closing(conn) as conn:
            cur = conn.cursor()
            try:
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
                conn.commit()
            finally:
                cur.close()
    except Exception as e:
        print(f"⚠️  Cache put failed: {e}. Skipping cache.")
        return

# ------------------------
# ChromaVectorStore
# ------------------------
class ChromaVectorStore:
    def __init__(self, collection_name="fintbx_concepts", batch_size=100):
        self.batch_size = batch_size
        # CHROMADB_API_KEY="ck-BrgcfXPxcyK22Lir6SGqqiVQsEaU5EE8mDbQ71omUUg8"
        CHROMADB_API_KEY = "ck-DABQ5shjt43T6dCo3jZJndSzUEr8gAK54VcnQnuRbyJo"
        CHROMA_SERVER_HOST = os.getenv("CHROMA_SERVER_HOST", "https://api.trychroma.com")
        OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "sk-your-openai-key")
        if not OPENAI_API_KEY:
            OPENAI_API_KEY = "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"
            os.environ["OPENAI_API_KEY"] = OPENAI_API_KEY
            print("⚠️  Loaded fallback OpenAI API key from code (set in env for production).")
        self.embedding_fn = OpenAIEmbeddings(
            model="text-embedding-3-large",
            api_key=OPENAI_API_KEY
        )
        self.client = chromadb.CloudClient(
            api_key=CHROMADB_API_KEY,
            tenant=os.getenv("CHROMA_TENANT_ID"),
            database=os.getenv("CHROMA_DATABASE", "AURELIA_CHROMA_DB")
        )
        self.collection = self.client.get_or_create_collection(
            name=collection_name,
            metadata={"description": "Financial concepts extracted from PDFs"}
        )

    def clear_collection(self):
        self.client.delete_collection(name=self.collection.name)
        self.collection = self.client.get_or_create_collection(name=self.collection.name)

    def add_chunks(self, chunks: List[dict], backup_gcs=True):
        total_chunks = len(chunks)
        backup_data = []
        for start in range(0, total_chunks, self.batch_size):
            batch = chunks[start:start+self.batch_size]
            documents = [c["text"] for c in batch]
            metadatas = [c["metadata"] for c in batch]
            ids = [f"chunk_{start+i}" for i in range(len(batch))]
            embeddings = self.embedding_fn.embed_documents(documents)
            self.collection.add(documents=documents, metadatas=metadatas, ids=ids, embeddings=embeddings)
            for i, (doc, meta, emb) in enumerate(zip(documents, metadatas, embeddings)):
                backup_data.append({"id": ids[i], "text": doc, "metadata": meta, "embedding": emb})
        if backup_gcs:
            backup_file = os.path.join(os.path.dirname(__file__), "embeddings_backup.json")
            with open(backup_file, "w", encoding="utf-8") as f:
                json.dump(backup_data, f, ensure_ascii=False, indent=2)
            self._upload_to_gcs(backup_file, "embeddings_backup.json")

    def _upload_to_gcs(self, local_path, gcs_path):
        from google.cloud import storage
        client = storage.Client()
        bucket_name = os.getenv("GCS_OUTPUT_BUCKET", "fintbx-chunks")
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(gcs_path)
        blob.upload_from_filename(local_path)
        print(f"💾 Uploaded backup to GCS: {gcs_path}")

    # ------------------------
    # Retrieval
    # ------------------------
    def retrieve_hits(self, query_text: str, top_k=5, similarity_threshold=0.7) -> List[dict]:
        print(f"🔍 DEBUG: Checking cache for query: {query_text[:50]}...")
        cached = cache_get_pg(query_text)
        if cached:
            print(f"✅ DEBUG: Cache hit for query: {query_text[:50]}...")
            # Only return cached data if it has 'sources' (RAG answer format)
            if "sources" in cached:
                return cached["sources"]
            else:
                print(f"⚠️  DEBUG: Cached data is not a RAG answer format. Proceeding with vector search...")
                return None
        print(f"🔍 DEBUG: No cache hit, proceeding with vector search for: {query_text[:50]}...")
        query_embedding = self.embedding_fn.embed_query(query_text)
        results = self.collection.query(
            query_embeddings=[query_embedding],
            n_results=top_k,
            include=["documents", "metadatas", "distances"]
        )
        hits = []
        ids = results["ids"][0]
        docs = results["documents"][0]
        metas = results["metadatas"][0]
        distances = results["distances"][0]
        
        print(f"🔍 DEBUG: Found {len(ids)} results, filtering by similarity threshold: {similarity_threshold}")
        
        for i in range(len(ids)):
            # Calculate similarity score (1 - distance)
            similarity_score = 1.0 - distances[i] if distances[i] is not None else 0.0
            
            # Only include hits above the similarity threshold
            if similarity_score >= similarity_threshold:
                hits.append({
                    "id": ids[i],
                    "score": similarity_score,
                    "document": docs[i],
                    "meta": metas[i]
                })
                print(f"✅ DEBUG: Added hit with similarity {similarity_score:.3f}")
            else:
                print(f"❌ DEBUG: Filtered out hit with similarity {similarity_score:.3f} (below threshold {similarity_threshold})")
        
        print(f"🔍 DEBUG: Returning {len(hits)} hits after similarity filtering")
        return hits

    # ------------------------
    # Generate RAG Answer
    # ------------------------
    def generate_rag_answer(self, query_text: str, top_k=5) -> RAGAnswer:
        print(f"🔍 DEBUG: Checking cache for RAG answer query: {query_text[:50]}...")
        cached = cache_get_pg(query_text)
        if cached:
            print(f"✅ DEBUG: Cache hit for RAG answer query: {query_text[:50]}...")
            ans = RAGAnswer.model_validate(cached)
            ans.served_from = "cache"
            return ans
        print(f"🔍 DEBUG: No cache hit for RAG answer, proceeding with generation for: {query_text[:50]}...")
        hits = self.retrieve_hits(query_text, top_k)
        context = self._build_context_block(hits)
        sources = [
            SourceChunk(
                id=h["id"],
                score=h["score"],
                sec=h["meta"].get("sec"),
                page_s=h["meta"].get("page_s"),
                page_e=h["meta"].get("page_e")
            ) for h in hits
        ]
        answer_text = "\n".join([h["document"] for h in hits])[:1000]
        rag_answer = RAGAnswer(answer=answer_text, reasoning="Retrieved from top chunks",
                               sources=sources, query=query_text, served_from="rag")
        print(f"💾 DEBUG: About to cache results for query: {query_text[:50]}...")
        cache_put_pg(query_text, self.collection.name, "llm_stub",
                     "openai_text_embedding_3_large",
                     answer_text, rag_answer.model_dump(),
                     [s.model_dump() for s in sources], "rag")
        print(f"✅ DEBUG: Cache update completed for query: {query_text[:50]}...")
        return rag_answer

    # ------------------------
    # Build Context Block
    # ------------------------
    def _build_context_block(self, hits: List[dict]) -> str:
        lines = []
        for h in hits:
            meta = h.get("meta") or {}
            sec = meta.get("sec", "")
            ps = meta.get("page_s")
            pe = meta.get("page_e")
            head = f"[{h.get('id')}] sec={sec} pages={ps}-{pe} score={h.get('score',0.0):.3f}"
            body = (h.get("document") or "")[:1000].replace("\n", " ")
            lines.append(head + ("\n" + body if body else ""))
        return "\n\n".join(lines)
    
if __name__ == "__main__":
    from pdf_processor.parser_chunker import PDFProcessor

    # 1️⃣ Initialize ChromaVectorStore
    store = ChromaVectorStore()

    # 2️⃣ Optional: Clear the collection (useful for fresh testing)
    # store.clear_collection()
    # print("✅ Cleared Chroma collection")

    # 3️⃣ Optional: Add PDF chunks to the vector store
    try:
        processor = PDFProcessor()
        chunks = processor.get_document_chunks()  # returns list of {"text":..., "metadata":...}
        if chunks:
            store.add_chunks(chunks)
            print(f"✅ Added {len(chunks)} chunks to ChromaVectorStore")
        else:
            print("⚠️ No chunks found to add")
    except Exception as e:
        print(f"❌ Error adding chunks: {e}")

    # 4️⃣ Test query and generate RAG answer
    test_queries = [
        "Explain Sharpe Ratio",
        "What is Net Present Value",
        "Difference between equity and debt"
    ]

    for query in test_queries:
        try:
            answer = store.generate_rag_answer(query)
            print(f"\n--- Query: {query} ---")
            print(f"Served from: {answer.served_from}")
            print("Answer (truncated 500 chars):")
            print(answer.answer[:500])
            print("Sources:")
            for src in answer.sources:
                print(f"  - id: {src.id}, score: {src.score:.3f}, sec: {src.sec}, pages: {src.page_s}-{src.page_e}")
        except Exception as e:
            print(f"❌ Error generating RAG answer for '{query}': {e}")

