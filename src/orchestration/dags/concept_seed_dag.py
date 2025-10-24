"""
concept_seed_dag.py
Cloud Composer DAG: query concepts -> RAG retrieval -> fallback to Wikipedia if no hits
"""

from __future__ import annotations
import os
import logging
import tempfile
import json
from typing import List

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable

# Lazy imports inside tasks
from vector_store.chroma_handler import ChromaVectorStore
import wikipedia
from langchain_text_splitters import RecursiveCharacterTextSplitter
from utils.instructor_client import InstructorClient, FinancialConceptNote

# --- default DAG args ---
default_args = {
    "owner": "aurelia",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
}

dag = DAG(
    dag_id="concept_seed_dag",
    default_args=default_args,
    description="Seed concepts into ChromaVectorStore with Wikipedia fallback",
    schedule="@weekly",
    start_date=datetime.now() - timedelta(days=1),
    catchup=False,
    max_active_runs=1,
)

# --- Environment Detection ---
IS_RUNNING_IN_CLOUD = os.getenv('AIRFLOW_ENV') is not None and 'composer' in os.getenv('AIRFLOW_ENV', '')

# --- Airflow Variables ---
BATCH_SIZE = int(Variable.get("BATCH_SIZE", default_var="100"))

# --- Load Concepts from GCS Bucket ---
def load_concepts_from_gcs():
    """Load concepts from concepts.json file in GCS bucket"""
    from google.cloud import storage
    
    try:
        # Get bucket name from Airflow Variables
        concepts_bucket = Variable.get("CONCEPTS_BUCKET", default_var="aurelia-concepts")
        
        # Construct GCS path: seed_concept/in/concepts.json
        gcs_file_path = "seed_concept/in/concepts.json"
        
        # Download from GCS
        client = storage.Client()
        bucket = client.bucket(concepts_bucket)
        blob = bucket.blob(gcs_file_path)
        
        # Download to temporary file
        import tempfile
        with tempfile.NamedTemporaryFile(mode='w+', suffix='.json', delete=False) as tmp_file:
            blob.download_to_filename(tmp_file.name)
            
            # Read the JSON file
            with open(tmp_file.name, 'r', encoding='utf-8') as f:
                data = json.load(f)
                concepts = data.get("concepts", [])
                logging.info(f"Loaded {len(concepts)} concepts from gs://{concepts_bucket}/{gcs_file_path}")
                return concepts
                
    except Exception as e:
        logging.warning(f"Error loading concepts from GCS: {e}, falling back to Airflow Variables")
    
    # Fallback to Airflow Variables
    concepts_str = Variable.get("CONCEPTS_LIST", default_var="Duration,Sharpe Ratio,Black-Scholes")
    concepts = [concept.strip() for concept in concepts_str.split(",")]
    logging.info(f"Loaded {len(concepts)} concepts from Airflow Variables")
    return concepts

CONCEPTS = load_concepts_from_gcs()

# --- tasks ---
def seed_concepts(**context):
    vector_store = ChromaVectorStore(batch_size=BATCH_SIZE)
    instructor_client = InstructorClient()
    structured_notes = []

    for i, concept in enumerate(CONCEPTS, 1):
        logging.info("Processing concept %d/%d: %s", i, len(CONCEPTS), concept)
        
        # Check if we already have a cached concept note
        print(f"🔍 DEBUG: Checking cache for concept note: {concept}")
        from vector_store.chroma_handler import cache_get_pg
        cached_concept = cache_get_pg(concept)
        if cached_concept and "concept" in cached_concept:
            print(f"✅ DEBUG: Found cached concept note for: {concept}")
            print(f"🔍 DEBUG: Cached concept name: {cached_concept.get('concept', 'Unknown')}")
            print(f"🔍 DEBUG: Cached source: {cached_concept.get('source', 'Unknown')}")
            
            # Validate required fields in cached concept note
            required_fields = ["concept", "definition", "key_points", "source"]
            missing_fields = [field for field in required_fields if field not in cached_concept]
            
            if missing_fields:
                print(f"⚠️  DEBUG: Cached concept note missing fields: {missing_fields}. Regenerating...")
            else:
                print(f"✅ DEBUG: Cached concept note is valid, using cached version")
                structured_notes.append(cached_concept)
                continue
        else:
            print(f"🔍 DEBUG: No cached concept note found for: {concept}")
        
        print(f"🔍 DEBUG: No cached concept note, generating new one for: {concept}")
        print(f"🔍 DEBUG: Retrieving hits for concept: {concept}")
        # Use a higher similarity threshold for concept seeding to avoid irrelevant matches
        hits = vector_store.retrieve_hits(concept, top_k=5, similarity_threshold=0.75)
        print(f"🔍 DEBUG: Retrieved {len(hits) if hits else 0} hits for concept: {concept}")

        if not hits:
            logging.info("No PDF chunks found for '%s', fetching Wikipedia fallback", concept)
            try:
                summary = wikipedia.summary(concept, sentences=5)
                
                # Generate structured concept note using instructor
                concept_note = instructor_client.generate_concept_note(
                    concept=concept,
                    context=summary,
                    matches=[]
                )
                concept_note.source = "Wikipedia"
                # Clear citations since this is from Wikipedia, not PDF
                concept_note.citations = []
                structured_notes.append(concept_note.dict())
                
                # Split text into chunks and add to Chroma
                splitter = RecursiveCharacterTextSplitter(chunk_size=500, chunk_overlap=50)
                chunks_text = splitter.split_text(summary)
                chunks = [{"text": t, "metadata": {"source": "wikipedia", "concept": concept}} for t in chunks_text]
                vector_store.add_chunks(chunks)
                
                logging.info(f"Generated structured note for '{concept}' using Wikipedia fallback")
            except Exception as e:
                logging.error(f"Failed to generate concept note for '{concept}': {e}")
                # Create a basic note structure
                basic_note = {
                    "concept": concept,
                    "definition": f"Could not fetch information for {concept}",
                    "key_points": [],
                    "formulas": [],
                    "examples": [],
                    "related_concepts": [],
                    "source": "Error",
                    "citations": [],
                    "generated_at": datetime.now().isoformat()
                }
                structured_notes.append(basic_note)
        else:
            # Generate structured concept note from PDF content
            pdf_context = "\n".join([hit.get("document", hit.get("text", "")) for hit in hits])
            logging.info(f"Found {len(hits)} hits for '{concept}', context length: {len(pdf_context)}")
            
            if not pdf_context.strip():
                logging.warning(f"No content found in hits for '{concept}', falling back to Wikipedia")
                try:
                    summary = wikipedia.summary(concept, sentences=5)
                    concept_note = instructor_client.generate_concept_note(
                        concept=concept,
                        context=summary,
                        matches=[]
                    )
                    concept_note.source = "Wikipedia"
                    # Clear citations since this is from Wikipedia, not PDF
                    concept_note.citations = []
                    structured_notes.append(concept_note.dict())
                    logging.info(f"Generated structured note for '{concept}' using Wikipedia fallback")
                except Exception as e:
                    logging.error(f"Failed to generate concept note for '{concept}' with Wikipedia: {e}")
                    # Create a basic note structure
                    basic_note = {
                        "concept": concept,
                        "definition": f"Information available for {concept}",
                        "key_points": [],
                        "formulas": [],
                        "examples": [],
                        "related_concepts": [],
                        "source": "Error",
                        "citations": [],
                        "generated_at": datetime.now().isoformat()
                    }
                    structured_notes.append(basic_note)
            else:
                try:
                    concept_note = instructor_client.generate_concept_note(
                        concept=concept,
                        context=pdf_context,
                        matches=hits
                    )
                    concept_note.source = "Financial Toolbox PDF"
                    structured_notes.append(concept_note.dict())
                    logging.info(f"Generated structured note for '{concept}' from PDF content")
                except Exception as e:
                    logging.error(f"Failed to generate concept note for '{concept}': {e}")
                    # Create a basic note structure
                    basic_note = {
                        "concept": concept,
                        "definition": f"Information available for {concept}",
                        "key_points": [],
                        "formulas": [],
                        "examples": [],
                        "related_concepts": [],
                        "source": "PDF (Error in processing)",
                        "citations": [],
                        "generated_at": datetime.now().isoformat()
                    }
                    structured_notes.append(basic_note)

    # Create final output with metadata - convert datetime objects to ISO strings
    def convert_datetime_to_iso(obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return obj
    
    # Convert all datetime objects in structured_notes to ISO strings
    for note in structured_notes:
        if 'generated_at' in note and isinstance(note['generated_at'], datetime):
            note['generated_at'] = note['generated_at'].isoformat()
        concept_name = note.get('concept', 'Unknown')
        source = note.get('source', 'unknown')
        print(f"💾 DEBUG: Caching concept note for: {concept_name} (source: {source})")
        
        # Determine retrieval source based on the concept note source
        if source == "Wikipedia":
            retrieval_source = "wikipedia"
            sources_json = []  # Wikipedia doesn't have citations from PDF
        elif source == "Financial Toolbox PDF":
            retrieval_source = "pdf"
            sources_json = note.get('citations', [])
        else:
            retrieval_source = source.lower().replace(" ", "_")
            sources_json = note.get('citations', [])
        
        from vector_store.chroma_handler import cache_put_pg
        cache_put_pg(
            query=concept_name,
            collection="fintbx_concepts",
            gen_model="gpt-4o-mini",
            embed_model="text-embedding-3-large",
            answer_text=note.get('definition', ''),
            answer_json=note,
            sources_json=sources_json,
            retrieval_source=retrieval_source
        )

    output_data = {
        "concepts": structured_notes,
        "total_concepts": len(structured_notes),
        "last_run": datetime.now().isoformat(),
        "generated_by": "concept_seed_dag"
    }

    # Save structured notes to GCS bucket
    from google.cloud import storage
    
    try:
        # Get bucket from Airflow Variables
        output_bucket = Variable.get("CONCEPTS_BUCKET", default_var="aurelia-concepts")
        
        # Simple filename without timestamp
        gcs_path = "seed_concept/out/concept_seed_summary.json"
        
        # Upload to GCS
        client = storage.Client()
        bucket = client.bucket(output_bucket)
        blob = bucket.blob(gcs_path)
        
        # Upload JSON data - convert datetime objects to ISO strings
        def json_serializer(obj):
            if isinstance(obj, datetime):
                return obj.isoformat()
            raise TypeError(f"Object of type {type(obj)} is not JSON serializable")
        
        blob.upload_from_string(
            json.dumps(output_data, indent=2, default=json_serializer),
            content_type='application/json'
        )
        
        gcs_url = f"gs://{output_bucket}/{gcs_path}"
        logging.info(f"Structured concept notes uploaded to: {gcs_url}")
        context["ti"].xcom_push(key="concept_seed_summary", value=gcs_url)
        
    except Exception as e:
        logging.error(f"Failed to upload summary to GCS: {e}")
        # Fallback to local temp file
        if IS_RUNNING_IN_CLOUD:
            tmp_dir = "/tmp"
            summary_file = f"{tmp_dir}/concept_seed_summary.json"
        else:
            tmp_dir = tempfile.mkdtemp(prefix="concept_seed_")
            summary_file = f"{tmp_dir}/concept_seed_summary.json"
        
        with open(summary_file, "w", encoding="utf-8") as f:
            json.dump(output_data, f, indent=2, default=json_serializer)
        
        logging.info("Structured concept notes saved locally: %s", summary_file)
        context["ti"].xcom_push(key="concept_seed_summary", value=summary_file)

# --- Operator ---
task_seed_concepts = PythonOperator(
    task_id="seed_concepts_with_wikipedia_fallback",
    python_callable=seed_concepts,
    dag=dag,
)
