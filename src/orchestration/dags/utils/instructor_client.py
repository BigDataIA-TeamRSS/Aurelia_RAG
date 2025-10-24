import instructor
from openai import OpenAI
from pydantic import BaseModel, Field, field_validator
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
    citations: List[Dict[str, str]] = Field(default=[], description="Page and section citations")
    generated_at: datetime = Field(default_factory=datetime.now, description="When the note was generated")
    
    @field_validator('formulas', 'examples')
    def filter_none_values(cls, v):
        """Filter out None values from lists"""
        if v is None:
            return []
        return [item for item in v if item is not None]

class InstructorClient:
    def __init__(self):
        self.client = instructor.patch(OpenAI())
    
    def generate_concept_note(self, concept: str, context: str, matches) -> FinancialConceptNote:
        """Generate structured concept note using instructor"""
        
        try:
            # Build citations from matches
            citations = []
            for match in matches:
                # Handle both dict and object formats
                if isinstance(match, dict):
                    metadata = match.get('metadata', match.get('meta', {}))
                    score = match.get('score', 0.0)
                else:
                    metadata = getattr(match, 'metadata', {})
                    score = getattr(match, 'score', 0.0)
                
                # Extract page number from metadata
                page = metadata.get('page', metadata.get('source', 'N/A'))
                if isinstance(page, str) and 'page_' in page:
                    # Extract page number from "page_123" format
                    try:
                        page_num = page.split('_')[1]
                        page = page_num
                    except:
                        page = 'N/A'
                
                citations.append({
                    "page": str(page),
                    "section": metadata.get('section', metadata.get('chapter', 'General')),
                    "score": str(round(score, 4))
                })
            
            prompt = f"""
            Based on the following context from the Financial Toolbox, create a comprehensive concept note for: {concept}
            
            Context:
            {context}
            
            Provide a structured note with definition, key points, formulas (if any), examples, and related concepts.
            """
            
            response = self.client.chat.completions.create(
                # model="gpt-4.1-mini-2025-04-14",
                model="gpt-4o-mini-2024-07-18",
                messages=[
                    {"role": "system", "content": "You are a financial education expert creating structured concept notes."},
                    {"role": "user", "content": prompt}
                ],
                response_model=FinancialConceptNote
            )
            
            # Add citations to the response
            response.citations = citations
            return response
            
        except Exception as e:
            print(f"Error in generate_concept_note: {e}")
            # Return a basic note structure
            return FinancialConceptNote(
                concept=concept,
                definition=f"Error generating concept note: {str(e)}",
                key_points=[],
                formulas=[],
                examples=[],
                related_concepts=[],
                source="Error",
                citations=[],
                generated_at=datetime.now()
            )