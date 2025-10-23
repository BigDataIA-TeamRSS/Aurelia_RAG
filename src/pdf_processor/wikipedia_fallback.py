"""
wikipedia_fallback.py
Fetch fallback content from Wikipedia for a given concept.
Splits the content into chunks for embedding/storage.
"""

import logging
from typing import List
import wikipedia
# from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_text_splitters import RecursiveCharacterTextSplitter
def fetch_wikipedia_chunks(concept: str, chunk_size: int = 500, chunk_overlap: int = 50) -> List[dict]:
    """
    Fetch Wikipedia summary for a concept and split into chunks.
    
    Returns a list of dicts: [{"text": ..., "metadata": {...}}, ...]
    """
    logging.info("Fetching Wikipedia summary for concept: %s", concept)
    
    try:
        summary = wikipedia.summary(concept, sentences=5)
    except wikipedia.exceptions.DisambiguationError as e:
        logging.warning("DisambiguationError: %s, using first option", e)
        summary = wikipedia.summary(e.options[0], sentences=5)
    except wikipedia.exceptions.PageError:
        logging.error("Wikipedia page not found for concept: %s", concept)
        summary = f"Wikipedia page not found for concept: {concept}"
    except Exception as e:
        logging.error("Error fetching Wikipedia for %s: %s", concept, e)
        summary = f"Error fetching Wikipedia: {str(e)}"

    # Split into chunks
    splitter = RecursiveCharacterTextSplitter(chunk_size=chunk_size, chunk_overlap=chunk_overlap)
    chunks_text = splitter.split_text(summary)

    chunks = [{"text": t, "metadata": {"source": "wikipedia", "concept": concept}} for t in chunks_text]

    logging.info("Created %d Wikipedia chunks for concept: %s", len(chunks), concept)
    return chunks
