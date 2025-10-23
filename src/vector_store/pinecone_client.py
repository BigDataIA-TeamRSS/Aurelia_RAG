import pinecone
from typing import List, Dict
import os
from langchain_openai import OpenAIEmbeddings

class PineconeVectorStore:
    def __init__(self):
        pinecone.init(
            api_key=os.getenv('PINECONE_API_KEY'),
            environment=os.getenv('PINECONE_ENVIRONMENT')
        )
        self.index_name = os.getenv('PINECONE_INDEX_NAME')
        self.embeddings = OpenAIEmbeddings(model="text-embedding-3-large")
        
        # Create index if it doesn't exist
        if self.index_name not in pinecone.list_indexes():
            pinecone.create_index(
                self.index_name,
                dimension=3072,  # text-embedding-3-large dimension
                metric='cosine'
            )
        
        self.index = pinecone.Index(self.index_name)
    
    def upsert_chunks(self, chunks: List[Dict], namespace: str = "financial-toolbox"):
        """Store chunks with embeddings"""
        texts = [chunk['text'] for chunk in chunks]
        embeddings = self.embeddings.embed_documents(texts)
        
        # Prepare vectors for upsert
        vectors = []
        for i, (chunk, embedding) in enumerate(zip(chunks, embeddings)):
            vectors.append({
                'id': f"{namespace}-{i}",
                'values': embedding,
                'metadata': chunk['metadata']
            })
        
        # Upsert in batches
        batch_size = 100
        for i in range(0, len(vectors), batch_size):
            self.index.upsert(
                vectors=vectors[i:i+batch_size],
                namespace=namespace
            )
    
    def query(self, query: str, top_k: int = 5, namespace: str = "financial-toolbox"):
        """Query similar chunks"""
        query_embedding = self.embeddings.embed_query(query)
        
        results = self.index.query(
            vector=query_embedding,
            top_k=top_k,
            namespace=namespace,
            include_metadata=True
        )
        
        return results.matches