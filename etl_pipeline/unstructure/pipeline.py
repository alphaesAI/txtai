"""
ETL Pipeline Manager - Orchestrates the complete data processing pipeline.
"""

from typing import Any, Dict, List
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.manager import get_config
from unstructure.extractor.data.preprocessor import PreProcessor
from unstructure.extractor.data.textractor import Textractor


class ETLPipeline:
    """
    Main pipeline orchestrator that coordinates:
    connector -> preprocessor -> textractor -> embeddings (future)
    """
    
    def __init__(self, connector_type: str = "gmail", **extractor_kwargs):
        """
        Initialize the ETL pipeline.
        
        Args:
            connector_type: Type of connector to use (e.g., "gmail")
            **extractor_kwargs: Additional arguments for Textractor
        """
        self.config_manager = get_config()
        
        # Initialize preprocessor with connector
        connector_config = self.config_manager.get_connector_config("unstructure", connector_type)
        self.preprocessor = PreProcessor(connector_type, connector_config)
        
        # Initialize textractor
        self.textractor = Textractor(**extractor_kwargs)
        
        # Link preprocessor with textractor
        self.preprocessor.set_textractor(self.textractor)
    
    def run(self) -> List[str]:
        """
        Run the complete ETL pipeline.
        
        Returns:
            List of extracted text content ready for embeddings
        """
        try:
            # Option 1: Use integrated method
            return self.preprocessor.process_and_extract()
            
            # Option 2: Manual step-by-step
            # data = self.preprocessor.fetch()
            # return self.textractor.batch_extract(data)
            
        except Exception as e:
            print(f"Pipeline error: {e}")
            return []
    
    def fetch_data(self) -> Dict[str, List[str]]:
        """
        Fetch raw data without processing through textractor.
        
        Returns:
            Dictionary with files, html, and text lists
        """
        return self.preprocessor.fetch()
    
    def extract_text(self, data: Dict[str, List[str]]) -> List[str]:
        """
        Extract text from preprocessed data.
        
        Args:
            data: Output from preprocessor.fetch()
            
        Returns:
            List of extracted text content
        """
        return self.textractor.batch_extract(data)


if __name__ == "__main__":
    """Test the unstructure pipeline."""
    print("Testing unstructure pipeline...")
    
    # Initialize pipeline
    pipeline = ETLPipeline(connector_type="gmail")
    
    # Run the pipeline
    results = pipeline.run()
    
    print(f"Extracted {len(results)} text items:")
    for i, text in enumerate(results[:3], 1):  # Show first 3 items
        print(f"\n--- Item {i} ---")
        print(text[:200] + "..." if len(text) > 200 else text)
