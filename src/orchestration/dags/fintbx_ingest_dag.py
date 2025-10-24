"""
fintbx_ingest_dag.py
Cloud Composer DAG: download PDF from GCS -> parse/chunk -> embed -> store (Chroma + GCS)
"""

from __future__ import annotations
import os
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
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
    schedule="@weekly",
    start_date=datetime.now() - timedelta(days=1),
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

# --- Environment Detection ---
IS_RUNNING_IN_CLOUD = os.getenv('AIRFLOW_ENV') is not None and 'composer' in os.getenv('AIRFLOW_ENV', '')

# --- tasks ---
def download_pdf(**context):
    # COMMENTED OUT: Unnecessary GCS download when running in Cloud Composer
    # We're already in GCS, no need to download from GCS to local
    if IS_RUNNING_IN_CLOUD:
        logging.info("Running in Cloud Composer - skipping GCS download")
        # In cloud, we can work directly with GCS paths or use the PDF from the same bucket
        gcs_pdf_path = f"gs://{GCS_PDF_BUCKET}/{GCS_PDF_OBJECT}"
        context["ti"].xcom_push(key="gcs_pdf_path", value=gcs_pdf_path)
        return gcs_pdf_path
    else:
        # LOCAL DEVELOPMENT: Download from GCS to local temp file
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
    import json
    import uuid
    
    if IS_RUNNING_IN_CLOUD:
        # CLOUD: Use GCS path directly
        gcs_pdf_path = context["ti"].xcom_pull(key="gcs_pdf_path")
        if not gcs_pdf_path:
            raise FileNotFoundError("GCS PDF path not found for parsing")
        
        # Download PDF temporarily for PyMuPDF processing (limitation of PyMuPDF)
        logging.info("Cloud environment detected - downloading PDF temporarily for processing")
        client = get_gcs_client()
        bucket_name, object_name = gcs_pdf_path.replace("gs://", "").split("/", 1)
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(object_name)
        
        # Use a unique temp file name to avoid conflicts
        temp_pdf_path = f"/tmp/fintbx_pdf_{uuid.uuid4().hex}.pdf"
        blob.download_to_filename(temp_pdf_path)
        
        try:
            processor = PDFProcessor(pdf_path=temp_pdf_path)
            chunks = processor.get_document_chunks()
            
            # Store chunks directly in GCS
            chunks_object_name = f"chunks/fintbx_chunks_{uuid.uuid4().hex}.json"
            chunks_blob = client.bucket(GCS_OUTPUT_BUCKET).blob(chunks_object_name)
            
            # Upload chunks as JSON to GCS
            chunks_json = json.dumps(chunks, ensure_ascii=False, indent=2)
            chunks_blob.upload_from_string(chunks_json, content_type='application/json')
            
            gcs_chunks_path = f"gs://{GCS_OUTPUT_BUCKET}/{chunks_object_name}"
            logging.info("Parsed and chunked PDF: %d chunks, stored at %s", len(chunks), gcs_chunks_path)
            context["ti"].xcom_push(key="gcs_chunks_path", value=gcs_chunks_path)
            return gcs_chunks_path
            
        finally:
            # Clean up temp PDF file
            if os.path.exists(temp_pdf_path):
                os.remove(temp_pdf_path)
    else:
        # LOCAL DEVELOPMENT: Use local file
        local_pdf_path = context["ti"].xcom_pull(key="local_pdf_path")
        if not local_pdf_path:
            raise FileNotFoundError("PDF not found for parsing")

        processor = PDFProcessor(pdf_path=local_pdf_path)
        chunks = processor.get_document_chunks()

        # Store chunks in GCS for consistency
        client = get_gcs_client()
        chunks_object_name = f"chunks/fintbx_chunks_{uuid.uuid4().hex}.json"
        chunks_blob = client.bucket(GCS_OUTPUT_BUCKET).blob(chunks_object_name)
        
        # Upload chunks as JSON to GCS
        chunks_json = json.dumps(chunks, ensure_ascii=False, indent=2)
        chunks_blob.upload_from_string(chunks_json, content_type='application/json')
        
        gcs_chunks_path = f"gs://{GCS_OUTPUT_BUCKET}/{chunks_object_name}"
        logging.info("Parsed and chunked PDF: %d chunks, stored at %s", len(chunks), gcs_chunks_path)
        context["ti"].xcom_push(key="gcs_chunks_path", value=gcs_chunks_path)
        return gcs_chunks_path

def embed_and_store(**context):
    gcs_chunks_path = context["ti"].xcom_pull(key="gcs_chunks_path")
    if not gcs_chunks_path:
        raise FileNotFoundError("GCS chunks path missing")

    import json
    
    # Download chunks from GCS
    client = get_gcs_client()
    bucket_name, object_name = gcs_chunks_path.replace("gs://", "").split("/", 1)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    
    # Download chunks JSON from GCS
    chunks_json_content = blob.download_as_text()
    chunks = json.loads(chunks_json_content)

    vector_store = ChromaVectorStore(batch_size=BATCH_SIZE)
    vector_store.add_chunks(chunks)

    logging.info("Added %d chunks to ChromaVectorStore", len(chunks))
    context["ti"].xcom_push(key="chunks_count", value=len(chunks))
    context["ti"].xcom_push(key="gcs_chunks_path", value=gcs_chunks_path)  # Pass along for backup task

def backup_chunks_to_gcs(**context):
    gcs_chunks_path = context["ti"].xcom_pull(key="gcs_chunks_path")
    chunks_count = context["ti"].xcom_pull(key="chunks_count")
    
    if not gcs_chunks_path:
        logging.warning("No GCS chunks path found - chunks may not have been processed")
        return None
    
    # Since chunks are already stored in GCS, this task now just logs the backup location
    logging.info("Chunks already stored in GCS at: %s", gcs_chunks_path)
    logging.info("Total chunks processed: %d", chunks_count or 0)
    
    # Optional: Create a backup with timestamp for historical tracking
    if IS_RUNNING_IN_CLOUD:
        logging.info("Cloud environment - chunks are already backed up in GCS")
        return gcs_chunks_path
    else:
        logging.info("Local development - chunks backed up to GCS")
        return gcs_chunks_path

# --- Operators wiring ---
task_download = PythonOperator(
    task_id="download_pdf",
    python_callable=download_pdf,
    dag=dag,
)

task_parse = PythonOperator(
    task_id="parse_and_chunk",
    python_callable=parse_and_chunk,
    dag=dag,
)

task_embed_store = PythonOperator(
    task_id="embed_and_store",
    python_callable=embed_and_store,
    dag=dag,
)

task_backup_gcs = PythonOperator(
    task_id="backup_chunks_to_gcs",
    python_callable=backup_chunks_to_gcs,
    dag=dag,
)

# --- DAG order ---
task_download >> task_parse >> task_embed_store >> task_backup_gcs
