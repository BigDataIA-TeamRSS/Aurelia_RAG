import fitz  # PyMuPDF
import os
import json
from typing import List, Dict, Any
from langchain_text_splitters import RecursiveCharacterTextSplitter
from google.cloud import storage
from dotenv import load_dotenv

# ------------------------
# Load environment variables
# ------------------------
load_dotenv()

# Environment detection
IS_RUNNING_IN_CLOUD = os.getenv('AIRFLOW_ENV') is not None and 'composer' in os.getenv('AIRFLOW_ENV', '')

# COMMENTED OUT: Local development credential check
# In Cloud Composer, authentication is handled automatically
if not IS_RUNNING_IN_CLOUD:
    # LOCAL DEVELOPMENT: Check GOOGLE_APPLICATION_CREDENTIALS
    cred_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if not cred_path or not os.path.exists(cred_path):
        raise FileNotFoundError(
            "❌ GOOGLE_APPLICATION_CREDENTIALS is not set or the file does not exist. "
            "Please set it in your .env file or environment."
        )
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = cred_path
else:
    # CLOUD: Authentication is handled by Cloud Composer service account
    print("✅ Running in Cloud Composer - using service account authentication")


# Check GOOGLE_APPLICATION_CREDENTIALS
# cred_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
# if not cred_path or not os.path.exists(cred_path):
#     raise FileNotFoundError(
#         "❌ GOOGLE_APPLICATION_CREDENTIALS is not set or the file does not exist. "
#         "Please set it in your .env file or environment."
#     )
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = cred_path
# Buckets
GCS_PDF_BUCKET = os.getenv("GCS_PDF_BUCKET", "fintbx-pdf")        # Original PDF
GCS_OUTPUT_BUCKET = os.getenv("GCS_OUTPUT_BUCKET", "fintbx-chunks")  # Processed output

# ------------------------
# Local temp folder (cross-platform)
# ------------------------
if IS_RUNNING_IN_CLOUD:
    # CLOUD: Use /tmp directory (Cloud Composer standard)
    TMP_DIR = "/tmp"
    PDF_LOCAL_PATH = os.path.join(TMP_DIR, "fintbx.pdf")
else:
    # LOCAL DEVELOPMENT: Use relative tmp directory
    TMP_DIR = os.path.join(os.path.dirname(__file__), "tmp")
    os.makedirs(TMP_DIR, exist_ok=True)
    PDF_LOCAL_PATH = os.path.join(TMP_DIR, "fintbx.pdf")

# ------------------------
# PDF Chunking Configuration
# ------------------------
CHUNK_SIZE = 1000
CHUNK_OVERLAP = 200

text_splitter = RecursiveCharacterTextSplitter(
    chunk_size=CHUNK_SIZE,
    chunk_overlap=CHUNK_OVERLAP,
    length_function=len,
    add_start_index=True,
)

# ------------------------
# GCS Helper Functions
# ------------------------
def download_pdf_from_gcs(bucket_name: str, gcs_pdf_path: str, local_path: str):
    """Download PDF from GCS to local path"""
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(gcs_pdf_path)
    blob.download_to_filename(local_path)
    print(f"📥 Downloaded PDF from gs://{bucket_name}/{gcs_pdf_path} → {local_path}")

def upload_to_gcs(local_path: str, gcs_path: str, bucket_name: str = GCS_OUTPUT_BUCKET):
    """Upload a local file to GCS"""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(gcs_path)
    blob.upload_from_filename(local_path)
    print(f"💾 Uploaded to GCS: gs://{bucket_name}/{gcs_path}")

# ------------------------
# PDF Processor Class
# ------------------------
class PDFProcessor:
    def __init__(self, pdf_path: str = PDF_LOCAL_PATH):
        if not os.path.exists(pdf_path):
            raise FileNotFoundError(f"PDF not found at {pdf_path}.")
        self.pdf_path = pdf_path
        self.text_splitter = text_splitter
        print(f"✅ PDFProcessor initialized for: {self.pdf_path}")

    def get_document_chunks(self) -> List[Dict[str, Any]]:
        print(f"📖 Opening PDF: {self.pdf_path}")
        doc = fitz.open(self.pdf_path)
        all_chunks = []
        all_text_full = ""

        for page_num, page in enumerate(doc):
            page_data = self._extract_page_data(page, page_num)
            if not page_data["text"]:
                continue

            all_text_full += page_data["text"] + "\n"
            print(f"✂️ Chunking Page {page_num + 1}...")

            chunks = self.text_splitter.split_text(page_data["text"])
            for chunk_text in chunks:
                metadata = {
                    "source": os.path.basename(self.pdf_path),
                    "page_number": page_num + 1,
                    "section_heading": page_data["main_heading"],
                    "captions": ", ".join(page_data["captions"]) if page_data["captions"] else "",
                }
                # Ensure metadata is string/int/float/bool
                for k, v in metadata.items():
                    if isinstance(v, list):
                        metadata[k] = ", ".join(map(str, v))
                    elif v is None:
                        metadata[k] = ""
                all_chunks.append({"text": chunk_text, "metadata": metadata})

        doc.close()
        print(f"✅ Total chunks extracted: {len(all_chunks)}")

        # Save TXT and JSON locally
        txt_file = os.path.join(TMP_DIR, f"{os.path.basename(self.pdf_path)}.txt")
        json_file = os.path.join(TMP_DIR, f"{os.path.basename(self.pdf_path)}_chunks.json")
        with open(txt_file, "w", encoding="utf-8") as f:
            f.write(all_text_full)
        with open(json_file, "w", encoding="utf-8") as f:
            json.dump(all_chunks, f, ensure_ascii=False, indent=2)

        # Upload to GCS
        upload_to_gcs(txt_file, os.path.basename(txt_file))
        upload_to_gcs(json_file, os.path.basename(json_file))

        return all_chunks

    def _extract_page_data(self, page: fitz.Page, page_num: int) -> Dict[str, Any]:
        text_blocks = page.get_text("dict")["blocks"]
        all_text = ""
        captions = []
        main_heading = "Unknown"
        max_font_size = 0

        for block in text_blocks:
            if "lines" not in block:
                continue
            block_text = ""
            for line in block["lines"]:
                for span in line["spans"]:
                    block_text += span["text"] + " "
                    if span["size"] > max_font_size and len(span["text"].strip()) > 3:
                        max_font_size = span["size"]
                        main_heading = span["text"].strip()
            all_text += block_text.strip() + "\n"

            # Detect captions
            if "Figure" in block_text or "Table" in block_text:
                captions.append(block_text.strip())

        # Optional: extract images to temp folder
        image_list = page.get_images(full=True)
        for img_index, img in enumerate(image_list):
            xref = img[0]
            base_image = page.parent.extract_image(xref)
            img_bytes = base_image["image"]
            image_ext = base_image["ext"]
            image_path = os.path.join(TMP_DIR, f"page_{page_num+1}_img_{img_index+1}.{image_ext}")
            with open(image_path, "wb") as f:
                f.write(img_bytes)
            # Optional: upload_to_gcs(image_path, f"images/{os.path.basename(image_path)}")

        return {"text": all_text, "main_heading": main_heading, "captions": captions}

# ------------------------
# Main
# ------------------------
if __name__ == "__main__":
    print("🔍 Testing PDF Processor...")
    try:
        # Step 1: download PDF from GCS
        download_pdf_from_gcs(GCS_PDF_BUCKET, "fintbx.pdf", PDF_LOCAL_PATH)

        # Step 2: process PDF
        processor = PDFProcessor(pdf_path=PDF_LOCAL_PATH)
        chunks = processor.get_document_chunks()

        if chunks:
            print(f"\n✅ Extracted {len(chunks)} chunks.")
            print("\n--- Example Chunk ---")
            print(chunks[0]["text"])
            print("\n--- Metadata ---")
            print(chunks[0]["metadata"])
        else:
            print("⚠️ No chunks extracted.")
    except Exception as e:
        print(f"❌ Error: {e}")

