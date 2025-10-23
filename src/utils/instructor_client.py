import instructor
from openai import OpenAI
from pydantic import BaseModel, Field
from typing import List, Dict, Optional
from datetime import datetime

class FinancialConceptNote(BaseModel):
    """Structured financial concept note"""
    concept: str = Field(description="The financial concept name")
    definition: str = Field(description="Clear, concise definition")
    key_points: List[str] = Field(description="Key points about the concept")
    formulas: Optional[List[str]] = Field(description="Mathematical formulas if applicable")
    examples: Optional[List[str]] = Field(description="Practical examples")
    related_concepts: List[str] = Field(description="Related financial concepts")
    source: str = Field(default="Financial Toolbox PDF")
    citations: List[Dict[str, str]] = Field(description="Page and section citations")

class InstructorClient:
    def __init__(self):
        self.client = instructor.patch(OpenAI())
    
    async def generate_concept_note(self, concept: str, context: str, matches) -> FinancialConceptNote:
        """Generate structured concept note using instructor"""
        
        # Build citations from matches
        citations = []
        for match in matches:
            citations.append({
                "page": str(match.metadata.get('page', 'N/A')),
                "section": match.metadata.get('section', 'General'),
                "score": str(match.score)
            })
        
        prompt = f"""
        Based on the following context from the Financial Toolbox, create a comprehensive concept note for: {concept}
        
        Context:
        {context}
        
        Provide a structured note with definition, key points, formulas (if any), examples, and related concepts.
        """
        
        response = self.client.chat.completions.create(
            model="gpt-4-turbo-preview",
            messages=[
                {"role": "system", "content": "You are a financial education expert creating structured concept notes."},
                {"role": "user", "content": prompt}
            ],
            response_model=FinancialConceptNote
        )
        
        # Add metadata
        response.citations = citations
        response.generated_at = datetime.now()
        
        return response