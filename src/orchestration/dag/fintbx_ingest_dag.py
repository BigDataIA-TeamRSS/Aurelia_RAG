"""
fintbx_ingest_dag.py
Cloud Composer DAG: download PDF from GCS -> parse/chunk -> embed -> store (Chroma + GCS)
"""

from __future__ import annotations
import os
import tempfile
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

# Import your existing modules
from pdf_processor.parser_chunker import PDFProcessor
from vector_store.chroma_handler import ChromaVectorStore

from google.cloud import storage

# --- default DAG args ---
default_args = {
    "owner": "aurelia",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
}

dag = DAG(
    dag_id="fintbx_ingest_dag",
    default_args=default_args,
    description="Ingest fintbx.pdf -> chunk -> embed -> store (Chroma + GCS)",
    schedule_interval="@weekly",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
)

# --- Airflow Variables ---
GCS_PDF_BUCKET = Variable.get("GCS_PDF_BUCKET", default_var="fintbx-pdf")
GCS_PDF_OBJECT = Variable.get("GCS_PDF_OBJECT", default_var="fintbx.pdf")
GCS_OUTPUT_BUCKET = Variable.get("GCS_OUTPUT_BUCKET", default_var="fintbx-chunks")
BATCH_SIZE = int(Variable.get("BATCH_SIZE", default_var="100"))

# --- helper clients ---
def get_gcs_client():
    return storage.Client()

# --- tasks ---
def download_pdf(**context):
    client = get_gcs_client()
    bucket = client.bucket(GCS_PDF_BUCKET)
    blob = bucket.blob(GCS_PDF_OBJECT)

    tmp_dir = tempfile.mkdtemp(prefix="fintbx_")
    local_pdf_path = os.path.join(tmp_dir, os.path.basename(GCS_PDF_OBJECT))

    logging.info("Downloading gs://%s/%s -> %s", GCS_PDF_BUCKET, GCS_PDF_OBJECT, local_pdf_path)
    blob.download_to_filename(local_pdf_path)

    context["ti"].xcom_push(key="local_pdf_path", value=local_pdf_path)
    return local_pdf_path

def parse_and_chunk(**context):
    local_pdf_path = context["ti"].xcom_pull(key="local_pdf_path")
    if not local_pdf_path:
        raise FileNotFoundError("PDF not found for parsing")

    processor = PDFProcessor(pdf_path=local_pdf_path)
    chunks = processor.get_document_chunks()

    tmp_dir = tempfile.mkdtemp(prefix="fintbx_chunks_")
    json_path = os.path.join(tmp_dir, "fintbx_chunks.json")
    with open(json_path, "w", encoding="utf-8") as f:
        import json
        json.dump(chunks, f, ensure_ascii=False, indent=2)

    logging.info("Parsed and chunked PDF: %d chunks", len(chunks))
    context["ti"].xcom_push(key="chunks_json", value=json_path)
    return json_path

def embed_and_store(**context):
    chunks_json = context["ti"].xcom_pull(key="chunks_json")
    if not chunks_json:
        raise FileNotFoundError("Chunks JSON missing")

    import json
    with open(chunks_json, "r", encoding="utf-8") as f:
        chunks = json.load(f)

    vector_store = ChromaVectorStore(batch_size=BATCH_SIZE)
    vector_store.add_chunks(chunks)

    logging.info("Added %d chunks to ChromaVectorStore", len(chunks))
    context["ti"].xcom_push(key="chunks_count", value=len(chunks))

def backup_chunks_to_gcs(**context):
    chunks_json = context["ti"].xcom_pull(key="chunks_json")
    client = get_gcs_client()
    bucket = client.bucket(GCS_OUTPUT_BUCKET)

    object_name = f"chunks/{os.path.basename(chunks_json)}"
    bucket.blob(object_name).upload_from_filename(chunks_json)
    logging.info("Uploaded chunks JSON backup to gs://%s/%s", GCS_OUTPUT_BUCKET, object_name)
    return f"gs://{GCS_OUTPUT_BUCKET}/{object_name}"

# --- Operators wiring ---
task_download = PythonOperator(
    task_id="download_pdf",
    python_callable=download_pdf,
    provide_context=True,
    dag=dag,
)

task_parse = PythonOperator(
    task_id="parse_and_chunk",
    python_callable=parse_and_chunk,
    provide_context=True,
    dag=dag,
)

task_embed_store = PythonOperator(
    task_id="embed_and_store",
    python_callable=embed_and_store,
    provide_context=True,
    dag=dag,
)

task_backup_gcs = PythonOperator(
    task_id="backup_chunks_to_gcs",
    python_callable=backup_chunks_to_gcs,
    provide_context=True,
    dag=dag,
)

# --- DAG order ---
task_download >> task_parse >> task_embed_store >> task_backup_gcs
