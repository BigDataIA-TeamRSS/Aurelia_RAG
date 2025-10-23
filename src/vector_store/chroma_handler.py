
"""

#Upload the embeddings to GCP :
import os
import sys
import json
from dotenv import load_dotenv
from google.cloud import storage
import chromadb
from langchain_openai import OpenAIEmbeddings

# Ensure project-level imports work
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from pdf_processor.parser_chunker import PDFProcessor

# ------------------------
# Load environment variables
# ------------------------
load_dotenv()

GCS_BUCKET = os.getenv("GCS_OUTPUT_BUCKET", "fintbx-chunks")

CHROMADB_API_KEY="ck-BrgcfXPxcyK22Lir6SGqqiVQsEaU5EE8mDbQ71omUUg8"
CHROMA_SERVER_HOST = os.getenv("CHROMA_SERVER_HOST", "https://api.trychroma.com")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "sk-your-openai-key")
if not OPENAI_API_KEY:
            OPENAI_API_KEY = "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"
            os.environ["OPENAI_API_KEY"] = OPENAI_API_KEY
            print("⚠️  Loaded fallback OpenAI API key from code (set in env for production).")

os.environ["CHROMA_API_IMPL"] = "chromadb.api.fastapi.FastAPI"
os.environ["CHROMA_SERVER_HOST"] = CHROMA_SERVER_HOST
os.environ["CHROMA_API_KEY"] = CHROMADB_API_KEY

# ------------------------
# GCP Helper
# ------------------------
def upload_to_gcs(local_path: str, gcs_path: str, bucket_name: str = GCS_BUCKET):
    # Upload local file to GCS bucket
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(gcs_path)
    blob.upload_from_filename(local_path)
    print(f"💾 Uploaded backup to gs://{bucket_name}/{gcs_path}")

# ------------------------
# Chroma Vector Store
# ------------------------
class ChromaVectorStore:
    def __init__(self, collection_name: str = "fintbx_concepts", batch_size: int = 100):
        print(f"🆕 Connecting to Chroma collection: {collection_name}")
        self.client = chromadb.CloudClient(
            api_key=CHROMADB_API_KEY,
            tenant=os.getenv("CHROMA_TENANT_ID", ""),
            
        )

        self.embedding_fn = OpenAIEmbeddings(
            model="text-embedding-3-large",
            api_key=OPENAI_API_KEY
        )

        self.collection = self.client.get_or_create_collection(
            name=collection_name,
            metadata={"description": "Financial concepts extracted from fintbx.pdf"},
        )
        self.batch_size = batch_size
        print(f"✅ ChromaVectorStore initialized for collection: '{collection_name}'")

    def clear_collection(self):
        print(f"🧹 Clearing collection '{self.collection.name}'...")
        self.client.delete_collection(name=self.collection.name)
        self.collection = self.client.get_or_create_collection(name=self.collection.name)
        print("✅ Collection reset successfully.")

    def add_chunks(self, chunks, backup_gcs: bool = True):
        #Add chunks in batches to ChromaDB and optionally backup to GCS
        total_chunks = len(chunks)
        print(f"📦 Adding {total_chunks} chunks in batches of {self.batch_size}...")

        # Prepare GCS backup
        backup_data = []

        for start in range(0, total_chunks, self.batch_size):
            batch = chunks[start:start+self.batch_size]
            documents = [c["text"] for c in batch]
            metadatas = [c["metadata"] for c in batch]
            ids = [f"chunk_{start+i}" for i in range(len(batch))]

            embeddings = self.embedding_fn.embed_documents(documents)

            self.collection.add(
                documents=documents,
                metadatas=metadatas,
                ids=ids,
                embeddings=embeddings,
            )
            print(f"✅ Batch {start}-{start+len(batch)-1} added.")

            # Append to backup
            for i, (doc, meta, emb) in enumerate(zip(documents, metadatas, embeddings)):
                # No .tolist(), emb is already a list
                backup_data.append({"id": ids[i], "text": doc, "metadata": meta, "embedding": emb})

        # Upload single backup to GCS
        if backup_gcs:
            backup_file = os.path.join(os.path.dirname(__file__), "embeddings_backup.json")
            with open(backup_file, "w", encoding="utf-8") as f:
                json.dump(backup_data, f, ensure_ascii=False, indent=2)
            upload_to_gcs(backup_file, "embeddings_backup.json")
            print(f"💾 Backup of {len(backup_data)} embeddings uploaded to GCS.")


    def query(self, query_text: str, top_k: int = 5):
        print(f"🔍 Searching for top-{top_k} matches for: '{query_text}'")
        query_embedding = self.embedding_fn.embed_query(query_text)
        results = self.collection.query(
            query_embeddings=[query_embedding],
            n_results=top_k,
            include=["documents", "metadatas"],
        )
        formatted = []
        for i in range(len(results["ids"][0])):
            formatted.append({
                "text": results["documents"][0][i],
                "metadata": results["metadatas"][0][i],
            })
        return formatted

# ------------------------
# Main
# ------------------------
if __name__ == "__main__":
    print("🔧 Running ChromaVectorStore pipeline with GCS backup...\n")
    try:
        processor = PDFProcessor()
        chunks = processor.get_document_chunks()

        store = ChromaVectorStore(batch_size=100)  # adjust batch size < quota
        store.clear_collection()
        store.add_chunks(chunks, backup_gcs=True)

        # Example query
        query = "Explain the concept of Sharpe Ratio"
        results = store.query(query)
        print("\n--- Query Results ---")
        for r in results:
            print(f"[Page {r['metadata']['page_number']}] {r['text'][:200]}...\n")

    except Exception as e:
        print(f"❌ Error: {e}")

"""       

import os
import json
from typing import List, Optional, Literal
from pydantic import BaseModel
from contextlib import closing
from google.cloud import storage
from langchain_openai import OpenAIEmbeddings
import chromadb
import psycopg2
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
# Postgres Cache Utils
# ------------------------
PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")
PG_DB = os.getenv("PG_DB")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")

def get_pg_conn():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD
    )

def cache_get_pg(query: str):
    with closing(get_pg_conn()) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT answer_json FROM rag_cache WHERE query = %s", (query,))
            row = cur.fetchone()
            if row:
                return json.loads(row[0])
            return None

def cache_put_pg(query: str, collection: str, gen_model: str, embed_model: str,
                 answer_text: str, answer_json: dict, sources_json: list, retrieval_source: str):
    with closing(get_pg_conn()) as conn:
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
        conn.commit()

# ------------------------
# ChromaVectorStore
# ------------------------
class ChromaVectorStore:
    def __init__(self, collection_name="fintbx_concepts", batch_size=100):
        self.batch_size = batch_size
        CHROMADB_API_KEY="ck-BrgcfXPxcyK22Lir6SGqqiVQsEaU5EE8mDbQ71omUUg8"
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
        client = storage.Client()
        bucket_name = os.getenv("GCS_OUTPUT_BUCKET", "fintbx-chunks")
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(gcs_path)
        blob.upload_from_filename(local_path)
        print(f"💾 Uploaded backup to GCS: {gcs_path}")

    # ------------------------
    # Retrieval
    # ------------------------
    def retrieve_hits(self, query_text: str, top_k=5) -> List[dict]:
        cached = cache_get_pg(query_text)
        if cached:
            print(f"✅ Cache hit for query: {query_text}")
            return cached["sources"]
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
        for i in range(len(ids)):
            hits.append({
                "id": ids[i],
                "score": 1.0 - distances[i] if distances[i] is not None else 0.0,
                "document": docs[i],
                "meta": metas[i]
            })
        return hits

    # ------------------------
    # Generate RAG Answer
    # ------------------------
    def generate_rag_answer(self, query_text: str, top_k=5) -> RAGAnswer:
        cached = cache_get_pg(query_text)
        if cached:
            ans = RAGAnswer.model_validate(cached)
            ans.served_from = "cache"
            return ans
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
        cache_put_pg(query_text, self.collection.name, "llm_stub",
                     "openai_text_embedding_3_large",
                     answer_text, rag_answer.model_dump(),
                     [s.model_dump() for s in sources], "rag")
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

