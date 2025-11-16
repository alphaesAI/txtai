"""Textractor for extracting text from files."""

import logging
from typing import Any, Dict, Iterator, List, Optional, Union
from pathlib import Path

from .base import BaseExtractor
from ..data.textractor import Textractor as DataTextractor


class TextractorExtractor(BaseExtractor):
    """
    File-based extractor using Textractor for text extraction from various file formats.
    """

    def __init__(self, connector: Any = None, **kwargs):
        """
        Initialize Textractor extractor.
        
        Args:
            connector: Not used for file extraction (kept for interface compatibility)
            **kwargs: Additional Textractor parameters
        """
        super().__init__(connector, **kwargs)
        
        # Initialize Textractor with provided parameters
        textractor_params = {
            "sentences": kwargs.get("sentences", False),
            "lines": kwargs.get("lines", False),
            "paragraphs": kwargs.get("paragraphs", False),
            "minlength": kwargs.get("minlength", None),
            "join": kwargs.get("join", False),
            "sections": kwargs.get("sections", False),
            "cleantext": kwargs.get("cleantext", True),
            "chunker": kwargs.get("chunker", None),
            "headers": kwargs.get("headers", None),
            "backend": kwargs.get("backend", "available")
        }
        
        self.textractor = DataTextractor(**textractor_params)
        self.supported_extensions = kwargs.get("supported_extensions", [
            ".txt", ".pdf", ".docx", ".xlsx", ".csv", ".json", ".html", ".htm", 
            ".xml", ".rtf", ".odt", ".ods", ".pptx", ".epub", ".md"
        ])
        self.max_file_size = kwargs.get("max_file_size", 100 * 1024 * 1024)  # 100MB default
        
        self.logger = logging.getLogger(__name__)

    def extract(
        self,
        source: str,
        query: Optional[str] = None,
        **kwargs
    ) -> Iterator[Dict[str, Any]]:
        """
        Extract text content from files.
        
        Args:
            source: File path or directory path
            query: Optional filter pattern (glob pattern)
            **kwargs: Additional extraction parameters
            
        Yields:
            Extracted text content as dictionaries
        """
        source_path = Path(source)
        
        if not source_path.exists():
            raise FileNotFoundError(f"Source path not found: {source}")
        
        # Get files to process
        files_to_process = self._get_files_to_process(source_path, query)
        
        self.logger.info(f"Processing {len(files_to_process)} files from {source}")
        
        for file_path in files_to_process:
            try:
                # Check file size
                if file_path.stat().st_size > self.max_file_size:
                    self.logger.warning(f"Skipping large file: {file_path} ({file_path.stat().st_size} bytes)")
                    continue
                
                # Extract text using Textractor
                extracted_text = self.textractor(str(file_path))
                
                # Yield result
                yield {
                    "file_path": str(file_path),
                    "file_name": file_path.name,
                    "file_extension": file_path.suffix.lower(),
                    "file_size": file_path.stat().st_size,
                    "extracted_text": extracted_text,
                    "extraction_method": "textractor",
                    "metadata": self._get_file_metadata(file_path)
                }
                
            except Exception as e:
                self.logger.error(f"Error extracting from {file_path}: {e}")
                yield {
                    "file_path": str(file_path),
                    "file_name": file_path.name,
                    "file_extension": file_path.suffix.lower(),
                    "error": str(e),
                    "extraction_method": "textractor",
                    "status": "failed"
                }

    def _get_files_to_process(self, source_path: Path, query: Optional[str] = None) -> List[Path]:
        """
        Get list of files to process from source path.
        
        Args:
            source_path: Source path (file or directory)
            query: Optional glob pattern for filtering
            
        Returns:
            List of file paths to process
        """
        files = []
        
        if source_path.is_file():
            files.append(source_path)
        elif source_path.is_dir():
            if query:
                # Use glob pattern if provided
                files = list(source_path.rglob(query))
            else:
                # Get all supported files
                for ext in self.supported_extensions:
                    files.extend(source_path.rglob(f"*{ext}"))
        else:
            raise ValueError(f"Invalid source path: {source_path}")
        
        # Filter to only include files (not directories)
        return [f for f in files if f.is_file()]

    def _get_file_metadata(self, file_path: Path) -> Dict[str, Any]:
        """
        Get metadata for a file.
        
        Args:
            file_path: Path to the file
            
        Returns:
            File metadata dictionary
        """
        try:
            stat = file_path.stat()
            return {
                "created_time": stat.st_ctime,
                "modified_time": stat.st_mtime,
                "accessed_time": stat.st_atime,
                "is_readable": os.access(file_path, os.R_OK)
            }
        except Exception as e:
            self.logger.warning(f"Could not get metadata for {file_path}: {e}")
            return {}

    def get_schema(self) -> Dict[str, Any]:
        """
        Get the output schema for this extractor.
        
        Returns:
            Schema dictionary
        """
        return {
            "type": "object",
            "properties": {
                "file_path": {"type": "string"},
                "file_name": {"type": "string"},
                "file_extension": {"type": "string"},
                "file_size": {"type": "integer"},
                "extracted_text": {"type": "string"},
                "extraction_method": {"type": "string"},
                "metadata": {"type": "object"},
                "error": {"type": "string"},
                "status": {"type": "string"}
            }
        }


# Import os for file access check
import os
