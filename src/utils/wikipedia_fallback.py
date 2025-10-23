# src/utils/wikipedia_fallback.py

import wikipedia
from typing import Dict, List, Optional
from datetime import datetime
import re

class WikipediaFallback:
    def __init__(self):
        self.wikipedia = wikipedia
        
    async def get_concept(self, concept: str) -> Dict:
        """Fallback to Wikipedia when concept not found in PDF"""
        try:
            # Search for the concept
            search_results = self.wikipedia.search(concept, results=3)
            
            if not search_results:
                return self._create_empty_note(concept)
            
            # Get the page
            page = self.wikipedia.page(search_results[0])
            
            # Extract content
            content = page.content[:3000]  # Limit content length
            
            # Parse into structured format
            definition = self._extract_definition(content)
            key_points = self._extract_key_points(content)
            related = self._find_related_concepts(page.links[:10])
            
            return {
                "concept": concept,
                "definition": definition,
                "key_points": key_points,
                "formulas": [],  # Wikipedia content usually doesn't have LaTeX
                "examples": self._extract_examples(content),
                "related_concepts": related,
                "source": "Wikipedia",
                "citations": [{
                    "page": page.title,
                    "section": "Wikipedia Article",
                    "url": page.url
                }],
                "generated_at": datetime.now().isoformat()
            }
            
        except Exception as e:
            print(f"Wikipedia fallback error: {e}")
            return self._create_empty_note(concept)
    
    def _extract_definition(self, content: str) -> str:
        """Extract the first paragraph as definition"""
        paragraphs = content.split('\n\n')
        if paragraphs:
            # Clean the first paragraph
            definition = paragraphs[0].strip()
            # Remove Wikipedia citation markers
            definition = re.sub(r'\[\d+\]', '', definition)
            return definition
        return f"A financial concept related to {content}"
    
    def _extract_key_points(self, content: str) -> List[str]:
        """Extract key points from content"""
        points = []
        
        # Look for bullet points or key sentences
        lines = content.split('\n')
        for line in lines[:20]:  # Check first 20 lines
            line = line.strip()
            if line and (
                line.startswith('•') or 
                line.startswith('-') or
                line.startswith('*') or
                (len(line) > 20 and len(line) < 200)
            ):
                # Clean up the line
                line = re.sub(r'^[•\-\*]\s*', '', line)
                line = re.sub(r'\[\d+\]', '', line)
                if line and line not in points:
                    points.append(line)
                    
                if len(points) >= 5:
                    break
        
        return points if points else [
            "This is a financial concept",
            "Further research may be needed for detailed information"
        ]
    
    def _extract_examples(self, content: str) -> List[str]:
        """Try to extract examples from content"""
        examples = []
        
        # Look for example patterns
        example_patterns = [
            r'[Ff]or example[,:]?\s*([^.]+\.)',
            r'[Ff]or instance[,:]?\s*([^.]+\.)',
            r'[Ss]uch as[,:]?\s*([^.]+\.)',
            r'[Ee]\.g\.[,:]?\s*([^.]+\.)'
        ]
        
        for pattern in example_patterns:
            matches = re.findall(pattern, content)
            for match in matches[:2]:  # Limit to 2 examples
                example = match.strip()
                example = re.sub(r'\[\d+\]', '', example)
                if example and len(example) > 10:
                    examples.append(example)
        
        return examples
    
    def _find_related_concepts(self, links: List[str]) -> List[str]:
        """Filter links to find finance-related concepts"""
        finance_keywords = [
            'ratio', 'rate', 'value', 'price', 'market', 'stock',
            'bond', 'option', 'future', 'derivative', 'portfolio',
            'risk', 'return', 'yield', 'capital', 'asset', 'equity',
            'debt', 'finance', 'investment', 'trading'
        ]
        
        related = []
        for link in links:
            link_lower = link.lower()
            if any(keyword in link_lower for keyword in finance_keywords):
                related.append(link)
                if len(related) >= 5:
                    break
        
        return related if related else ["Finance", "Investment", "Financial Markets"]
    
    def _create_empty_note(self, concept: str) -> Dict:
        """Create an empty note when nothing is found"""
        return {
            "concept": concept,
            "definition": f"Information about {concept} could not be found.",
            "key_points": [
                "No information available in the Financial Toolbox PDF",
                "No information found on Wikipedia"
            ],
            "formulas": [],
            "examples": [],
            "related_concepts": ["Finance", "Investment"],
            "source": "Not Found",
            "citations": [],
            "generated_at": datetime.now().isoformat()
        }