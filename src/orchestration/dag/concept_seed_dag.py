"""
concept_seed_dag.py
Cloud Composer DAG: query concepts -> RAG retrieval -> fallback to Wikipedia if no hits
"""

from __future__ import annotations
import logging
import tempfile
import json
from typing import List

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

# Lazy imports inside tasks
from vector_store.chroma_handler import ChromaVectorStore
import wikipedia
from langchain.text_splitter import RecursiveCharacterTextSplitter

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
    schedule_interval="@weekly",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
)

# --- Airflow Variables ---
BATCH_SIZE = int(Variable.get("BATCH_SIZE", default_var="100"))
CONCEPTS = Variable.get("CONCEPTS_LIST", default_var="Duration,Sharpe Ratio,Black-Scholes").split(",")

# --- tasks ---
def seed_concepts(**context):
    vector_store = ChromaVectorStore(batch_size=BATCH_SIZE)
    results_summary = {}

    for concept in CONCEPTS:
        logging.info("Processing concept: %s", concept)
        hits = vector_store.retrieve_hits(concept)

        if not hits:
            logging.info("No PDF chunks found for '%s', fetching Wikipedia fallback", concept)
            try:
                summary = wikipedia.summary(concept, sentences=5)
            except wikipedia.exceptions.DisambiguationError as e:
                summary = e.options[0]  # pick first option
            except Exception as e:
                summary = f"Could not fetch Wikipedia: {str(e)}"

            # Split text into chunks
            splitter = RecursiveCharacterTextSplitter(chunk_size=500, chunk_overlap=50)
            chunks_text = splitter.split_text(summary)
            chunks = [{"text": t, "metadata": {"source": "wikipedia", "concept": concept}} for t in chunks_text]

            # Add chunks to Chroma
            vector_store.add_chunks(chunks)
            results_summary[concept] = f"Wikipedia fallback added {len(chunks)} chunks"
        else:
            results_summary[concept] = f"Retrieved {len(hits)} chunks from cache"

    # Save summary JSON backup
    tmp_dir = tempfile.mkdtemp(prefix="concept_seed_")
    summary_file = f"{tmp_dir}/concept_seed_summary.json"
    with open(summary_file, "w", encoding="utf-8") as f:
        json.dump(results_summary, f, indent=2)

    logging.info("Concept seed summary: %s", results_summary)
    context["ti"].xcom_push(key="concept_seed_summary", value=summary_file)

# --- Operator ---
task_seed_concepts = PythonOperator(
    task_id="seed_concepts_with_wikipedia_fallback",
    python_callable=seed_concepts,
    provide_context=True,
    dag=dag,
)
