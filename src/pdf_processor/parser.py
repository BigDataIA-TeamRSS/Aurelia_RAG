# src/pdf_processor/docling_complete_parser.py
# Complete Docling parser with image, figure, and table extraction and storage

import os
import json
import base64
from pathlib import Path
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, field
import hashlib
from datetime import datetime

# Docling imports
from docling.document_converter import DocumentConverter
from docling.datamodel.base_models import InputFormat
from docling.datamodel.pipeline_options import (
    PdfPipelineOptions,
    TableFormerMode,
    OCRMode,
    AcceleratorOptions
)
from docling.datamodel.base_models import Document

@dataclass
class ExtractedElement:
    """Base class for extracted elements"""
    element_type: str
    page_number: int
    bbox: Optional[List[float]] = None
    confidence: float = 1.0
    metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass
class ExtractedImage(ExtractedElement):
    """Extracted image with data"""
    image_data: Optional[bytes] = None
    image_format: str = "png"
    width: int = 0
    height: int = 0
    caption: Optional[str] = None
    storage_path: Optional[str] = None
    hash: Optional[str] = None

@dataclass
class ExtractedFigure(ExtractedElement):
    """Extracted figure (chart/graph)"""
    figure_type: str = "unknown"  # chart, graph, diagram, etc.
    caption: str = ""
    reference_number: Optional[str] = None
    associated_image_hash: Optional[str] = None  # Link to image if applicable

@dataclass 
class ExtractedTable(ExtractedElement):
    """Extracted table with data"""
    data: List[List[Any]] = field(default_factory=list)
    headers: List[str] = field(default_factory=list)
    caption: Optional[str] = None
    num_rows: int = 0
    num_cols: int = 0
    has_borders: bool = True
    table_format: str = "grid"  # grid, borderless, etc.

@dataclass
class DoclingPageContent:
    """Complete page content from Docling extraction"""
    page_number: int
    text: str
    section_title: str = ""
    subsection_title: str = ""
    
    # Extracted elements
    images: List[ExtractedImage] = field(default_factory=list)
    figures: List[ExtractedFigure] = field(default_factory=list)
    tables: List[ExtractedTable] = field(default_factory=list)
    
    # Text elements
    paragraphs: List[Dict[str, Any]] = field(default_factory=list)
    code_blocks: List[Dict[str, Any]] = field(default_factory=list)
    equations: List[Dict[str, Any]] = field(default_factory=list)
    
    # Metadata
    metadata: Dict[str, Any] = field(default_factory=dict)

class DoclingCompleteParser:
    """Complete Docling parser with full element extraction and storage"""
    
    def __init__(self, 
                 pdf_path: str,
                 output_dir: str = "extracted_content",
                 extract_images: bool = True,
                 extract_tables: bool = True,
                 extract_figures: bool = True,
                 ocr_enabled: bool = True):
        
        self.pdf_path = pdf_path
        self.output_dir = Path(output_dir)
        self.extract_images = extract_images
        self.extract_tables = extract_tables
        self.extract_figures = extract_figures
        
        # Create output directories
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.images_dir = self.output_dir / "images"
        self.tables_dir = self.output_dir / "tables"
        self.figures_dir = self.output_dir / "figures"
        
        if self.extract_images:
            self.images_dir.mkdir(exist_ok=True)
        if self.extract_tables:
            self.tables_dir.mkdir(exist_ok=True)
        if self.extract_figures:
            self.figures_dir.mkdir(exist_ok=True)
        
        # Configure Docling
        self.pipeline_options = self._configure_pipeline(ocr_enabled)
        self.converter = DocumentConverter(
            pipeline_options=self.pipeline_options,
            pdf_backend="pypdfium2"  # Better for image extraction
        )
        
    def _configure_pipeline(self, ocr_enabled: bool) -> PdfPipelineOptions:
        """Configure Docling pipeline options"""
        pipeline_options = PdfPipelineOptions()
        
        # Table extraction settings
        if self.extract_tables:
            pipeline_options.table_structure_options = {
                "mode": TableFormerMode.ACCURATE,  # Best quality
                "detect_borderless": True,  # Important for financial documents
            }
        
        # OCR settings for scanned pages
        if ocr_enabled:
            pipeline_options.ocr_options = {
                "mode": OCRMode.AUTO,  # Auto-detect when OCR is needed
                "lang": ["eng"],
            }
        
        # Image extraction
        pipeline_options.images_options = {
            "extract": self.extract_images,
            "extract_inline": True,
            "extract_figures": self.extract_figures,
        }
        
        return pipeline_options
    
    def parse(self) -> List[DoclingPageContent]:
        """Parse PDF and extract all content"""
        print(f"Starting Docling parsing of {self.pdf_path}...")
        
        # Convert document
        result = self.converter.convert(self.pdf_path)
        document = result.document
        
        # Process each page
        pages_content = []
        
        for page_num in range(1, document.num_pages + 1):
            print(f"Processing page {page_num}/{document.num_pages}...")
            
            page_content = self._extract_page_content(document, page_num)
            pages_content.append(page_content)
        
        # Save extraction summary
        self._save_extraction_summary(pages_content)
        
        return pages_content
    
    def _extract_page_content(self, document: Document, page_num: int) -> DoclingPageContent:
        """Extract all content from a page"""
        
        # Initialize page content
        page_content = DoclingPageContent(page_number=page_num)
        
        # Get page elements
        page_elements = document.get_page(page_num)
        
        # Track current section
        current_section = ""
        current_subsection = ""
        
        # Process each element
        text_parts = []
        
        for element in page_elements:
            elem_type = element.type
            
            # Handle different element types
            if elem_type == "title":
                current_section = element.text
                page_content.section_title = current_section
                text_parts.append(element.text)
                
            elif elem_type == "section_title":
                current_subsection = element.text
                page_content.subsection_title = current_subsection
                text_parts.append(element.text)
                
            elif elem_type == "paragraph":
                para_data = {
                    'text': element.text,
                    'bbox': element.bbox if hasattr(element, 'bbox') else None,
                    'style': element.style if hasattr(element, 'style') else {}
                }
                page_content.paragraphs.append(para_data)
                text_parts.append(element.text)
                
            elif elem_type == "code":
                code_data = {
                    'code': element.text,
                    'language': element.metadata.get('language', 'text') if hasattr(element, 'metadata') else 'text',
                    'bbox': element.bbox if hasattr(element, 'bbox') else None
                }
                page_content.code_blocks.append(code_data)
                text_parts.append(element.text)
                
            elif elem_type == "equation":
                eq_data = {
                    'text': element.text,
                    'latex': element.metadata.get('latex', '') if hasattr(element, 'metadata') else '',
                    'bbox': element.bbox if hasattr(element, 'bbox') else None
                }
                page_content.equations.append(eq_data)
                text_parts.append(element.text)
                
            elif elem_type == "table" and self.extract_tables:
                table = self._extract_table(element, page_num)
                if table:
                    page_content.tables.append(table)
                    # Save table data
                    self._save_table(table, page_num, len(page_content.tables))
                    
            elif elem_type == "figure" and self.extract_figures:
                figure = self._extract_figure(element, page_num)
                if figure:
                    page_content.figures.append(figure)
                    
            elif elem_type == "image" and self.extract_images:
                image = self._extract_image(element, page_num)
                if image:
                    page_content.images.append(image)
                    # Save image data
                    if image.image_data:
                        self._save_image(image, page_num, len(page_content.images))
                        
            else:
                # Generic text element
                if hasattr(element, 'text'):
                    text_parts.append(element.text)
        
        # Combine all text
        page_content.text = '\n'.join(text_parts)
        
        # Add metadata
        page_content.metadata = {
            'num_elements': len(page_elements),
            'extraction_time': datetime.now().isoformat()
        }
        
        return page_content
    
    def _extract_table(self, element, page_num: int) -> Optional[ExtractedTable]:
        """Extract table from element"""
        table = ExtractedTable(
            element_type="table",
            page_number=page_num,
            bbox=element.bbox if hasattr(element, 'bbox') else None
        )
        
        # Extract table data
        if hasattr(element, 'table_data'):
            table_data = element.table_data
            table.data = table_data.get('cells', [])
            table.num_rows = table_data.get('num_rows', len(table.data))
            table.num_cols = table_data.get('num_cols', 0)
            
            # Extract headers if available
            if table.data and table.num_rows > 0:
                table.headers = table.data[0] if isinstance(table.data[0], list) else []
        
        # Extract caption
        if hasattr(element, 'caption'):
            table.caption = element.caption
        
        # Detect table format
        if hasattr(element, 'metadata'):
            table.has_borders = element.metadata.get('has_borders', True)
            table.table_format = element.metadata.get('format', 'grid')
        
        return table if table.data else None
    
    def _extract_figure(self, element, page_num: int) -> Optional[ExtractedFigure]:
        """Extract figure from element"""
        figure = ExtractedFigure(
            element_type="figure",
            page_number=page_num,
            bbox=element.bbox if hasattr(element, 'bbox') else None
        )
        
        # Extract caption
        if hasattr(element, 'caption'):
            figure.caption = element.caption
        elif hasattr(element, 'text'):
            figure.caption = element.text
        
        # Extract figure type and reference
        if hasattr(element, 'metadata'):
            figure.figure_type = element.metadata.get('type', 'unknown')
            figure.reference_number = element.metadata.get('ref_number')
        
        # Link to associated image if available
        if hasattr(element, 'image_ref'):
            figure.associated_image_hash = element.image_ref
        
        return figure
    
    def _extract_image(self, element, page_num: int) -> Optional[ExtractedImage]:
        """Extract image from element"""
        image = ExtractedImage(
            element_type="image",
            page_number=page_num,
            bbox=element.bbox if hasattr(element, 'bbox') else None
        )
        
        # Extract image data
        if hasattr(element, 'image'):
            image_obj = element.image
            
            # Get image bytes
            if hasattr(image_obj, 'data'):
                image.image_data = image_obj.data
                image.hash = hashlib.md5(image.image_data).hexdigest()
            
            # Get dimensions
            if hasattr(image_obj, 'width'):
                image.width = image_obj.width
                image.height = image_obj.height
            
            # Get format
            if hasattr(image_obj, 'format'):
                image.image_format = image_obj.format.lower()
        
        # Extract caption
        if hasattr(element, 'caption'):
            image.caption = element.caption
        
        return image if image.image_data else None
    
    def _save_image(self, image: ExtractedImage, page_num: int, image_idx: int):
        """Save image to disk"""
        if not image.image_data:
            return
        
        # Generate filename
        filename = f"page_{page_num:03d}_image_{image_idx:02d}_{image.hash[:8]}.{image.image_format}"
        filepath = self.images_dir / filename
        
        # Save image
        with open(filepath, 'wb') as f:
            f.write(image.image_data)
        
        image.storage_path = str(filepath)
        
        # Save metadata
        metadata = {
            'page': page_num,
            'index': image_idx,
            'hash': image.hash,
            'width': image.width,
            'height': image.height,
            'format': image.image_format,
            'caption': image.caption,
            'bbox': image.bbox
        }
        
        meta_path = filepath.with_suffix('.json')
        with open(meta_path, 'w') as f:
            json.dump(metadata, f, indent=2)
    
    def _save_table(self, table: ExtractedTable, page_num: int, table_idx: int):
        """Save table data to disk"""
        # Save as CSV
        csv_filename = f"page_{page_num:03d}_table_{table_idx:02d}.csv"
        csv_path = self.tables_dir / csv_filename
        
        import csv
        with open(csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            if table.headers:
                writer.writerow(table.headers)
            writer.writerows(table.data)
        
        # Save as JSON with metadata
        json_filename = f"page_{page_num:03d}_table_{table_idx:02d}.json"
        json_path = self.tables_dir / json_filename
        
        table_data = {
            'page': page_num,
            'index': table_idx,
            'caption': table.caption,
            'headers': table.headers,
            'data': table.data,
            'num_rows': table.num_rows,
            'num_cols': table.num_cols,
            'has_borders': table.has_borders,
            'format': table.table_format,
            'bbox': table.bbox
        }
        
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(table_data, f, indent=2, ensure_ascii=False)
    
    def _save_extraction_summary(self, pages_content: List[DoclingPageContent]):
        """Save summary of all extracted content"""
        summary = {
            'pdf_path': str(self.pdf_path),
            'total_pages': len(pages_content),
            'extraction_time': datetime.now().isoformat(),
            'statistics': {
                'total_images': sum(len(p.images) for p in pages_content),
                'total_figures': sum(len(p.figures) for p in pages_content),
                'total_tables': sum(len(p.tables) for p in pages_content),
                'total_code_blocks': sum(len(p.code_blocks) for p in pages_content),
                'total_equations': sum(len(p.equations) for p in pages_content)
            },
            'pages': []
        }
        
        # Add page summaries
        for page in pages_content:
            page_summary = {
                'page_number': page.page_number,
                'section': page.section_title,
                'text_length': len(page.text),
                'num_images': len(page.images),
                'num_figures': len(page.figures),
                'num_tables': len(page.tables),
                'num_code_blocks': len(page.code_blocks),
                'num_equations': len(page.equations),
                'images': [
                    {
                        'caption': img.caption,
                        'size': f"{img.width}x{img.height}",
                        'storage_path': img.storage_path
                    } for img in page.images
                ],
                'tables': [
                    {
                        'caption': tbl.caption,
                        'size': f"{tbl.num_rows}x{tbl.num_cols}"
                    } for tbl in page.tables
                ]
            }
            summary['pages'].append(page_summary)
        
        # Save summary
        summary_path = self.output_dir / 'extraction_summary.json'
        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)
        
        print(f"\nExtraction complete!")
        print(f"Summary saved to: {summary_path}")
        print(f"Images saved to: {self.images_dir}")
        print(f"Tables saved to: {self.tables_dir}")

# For compatibility
FinancialDoclingParser = DoclingCompleteParser