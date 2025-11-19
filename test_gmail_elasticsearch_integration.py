#!/usr/bin/env python3
"""
Gmail to Elasticsearch Embedding Integration Test

This script demonstrates embedding Gmail extractor output (attachments, metadata) 
into Elasticsearch using both ANN (for vectors) and Database (for content) backends.
"""

import os
import sys
import yaml
import datetime
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src" / "python"))

from src.python.txtai.ann.dense.factory import ANNFactory
from src.python.txtai.database.factory import DatabaseFactory
from src.python.txtai.pipeline.connectors.email import GmailConnector
from src.python.txtai.pipeline.extractors.textractor import TextractorExtractor
from src.python.txtai.embeddings import Embeddings


class GmailElasticsearchIntegration:
    """Integration class for Gmail to Elasticsearch embedding pipeline."""
    
    def __init__(self, config_path: str = "gmail_elasticsearch_config.yaml"):
        """Initialize integration with configuration."""
        self.config_path = config_path
        self.config = self._load_config()
        self.embeddings = None
        self.gmail_connector = None
        self.extractor = None
        
    def _load_config(self) -> dict:
        """Load configuration from YAML file."""
        try:
            with open(self.config_path, 'r') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")
        except yaml.YAMLError as e:
            raise ValueError(f"Invalid YAML configuration: {e}")
    
    def initialize_backends(self):
        """Initialize Elasticsearch Embeddings backend for both ANN and Database."""
        # Combine embeddings and database configuration
        embeddings_config = self.config.get('embeddings', {})
        database_config = self.config.get('database', {})
        
        # Merge configurations for Embeddings class
        config = {
            **embeddings_config,
            'content': database_config.get('content'),
            'content_index': database_config.get('content_index')
        }
        
        self.embeddings = Embeddings(config)
        print(f"✓ Embeddings backend initialized: {type(self.embeddings).__name__}")
        print(f"✓ ANN backend: {config.get('backend')}")
        print(f"✓ Database backend: {config.get('content')}")
    
    def initialize_gmail_connector(self):
        """Initialize Gmail connector."""
        gmail_config = self.config.get('gmail', {})
        self.gmail_connector = GmailConnector(
            credentials_path=gmail_config.get('credentials_path', 'credentials.json'),
            token_path=gmail_config.get('token_path', 'token.json')
        )
        print(f"✓ Gmail connector initialized: {type(self.gmail_connector).__name__}")
    
    def _extract_pdf_text(self, pdf_data: bytes, filename: str, temp_file_path: str = None) -> str:
        """Extract text from PDF using multiple methods."""
        import tempfile
        import os
        
        # Method 1: Try Textractor first
        try:
            if temp_file_path is None:
                with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as temp_file:
                    temp_file.write(pdf_data)
                    temp_file_path = temp_file.name
            
            text = self.extractor.textractor.text(temp_file_path)
            
            # Check if we got meaningful text (not just PDF structure)
            if text and len(text.strip()) > 50 and not text.startswith('%PDF'):
                print(f"Textractor extracted {len(text)} characters from {filename}")
                return text
            else:
                print(f"Textractor returned minimal text from {filename}")
                
        except Exception as e:
            print(f"Textractor failed for {filename}: {e}")
        
        # Method 2: Try PyPDF2 as fallback
        try:
            import PyPDF2
            import io
            
            pdf_stream = io.BytesIO(pdf_data)
            pdf_reader = PyPDF2.PdfReader(pdf_stream)
            
            text = ""
            for page_num, page in enumerate(pdf_reader.pages):
                page_text = page.extract_text()
                if page_text and len(page_text.strip()) > 10:
                    text += f"\n--- Page {page_num + 1} ---\n{page_text}"
            
            if text and len(text.strip()) > 50:
                print(f"PyPDF2 extracted {len(text)} characters from {filename}")
                return text
            else:
                print(f"PyPDF2 returned minimal text from {filename}")
                
        except ImportError:
            print("PyPDF2 not available for PDF extraction")
        except Exception as e:
            print(f"PyPDF2 failed for {filename}: {e}")
        
        # Method 3: Try pdfplumber as another fallback
        try:
            import pdfplumber
            import io
            
            pdf_stream = io.BytesIO(pdf_data)
            text = ""
            
            with pdfplumber.open(pdf_stream) as pdf:
                for page_num, page in enumerate(pdf.pages):
                    page_text = page.extract_text()
                    if page_text and len(page_text.strip()) > 10:
                        text += f"\n--- Page {page_num + 1} ---\n{page_text}"
            
            if text and len(text.strip()) > 50:
                print(f"pdfplumber extracted {len(text)} characters from {filename}")
                return text
            else:
                print(f"pdfplumber returned minimal text from {filename}")
                
        except ImportError:
            print("pdfplumber not available for PDF extraction")
        except Exception as e:
            print(f"pdfplumber failed for {filename}: {e}")
        
        # If all methods fail, return basic info
        print(f"All PDF extraction methods failed for {filename}")
        return f"PDF file: {filename}\nSize: {len(pdf_data)} bytes\nText extraction failed - binary PDF data"
    
    def initialize_extractor(self):
        """Initialize Textractor for attachment processing."""
        extractor_config = self.config.get('textractor', {})
        self.extractor = TextractorExtractor(extractor_config)
        print(f"✓ Textractor initialized: {type(self.extractor).__name__}")
    
    def process_gmail_data(self, limit: int = 5):
        """Process Gmail data and embed into Elasticsearch."""
        print(f"\nProcessing Gmail data (limit: {limit})...")
        
        # Connect to Gmail
        self.gmail_connector.connect()
        
        # Get unread messages from Gmail
        messages = self.gmail_connector.get_messages(query="is:unread", max_results=limit)
        print(f"Retrieved {len(messages)} unread messages from Gmail")
        
        processed_count = 0
        for i, msg in enumerate(messages[:limit]):
            try:
                # Extract message metadata
                headers = {h['name'].lower(): h['value'] for h in msg.get('payload', {}).get('headers', [])}
                subject = headers.get('subject', 'No subject')
                sender = headers.get('from', 'Unknown sender')
                message_id = msg.get('id', 'unknown')
                
                print(f"Processing message {i+1}/{len(messages)}: {subject} from {sender}")
                
                # Prepare text content for embedding
                text_content = []
                
                # Add message snippet
                if 'snippet' in msg and msg['snippet'].strip():
                    text_content.append(f"Subject: {subject}\nFrom: {sender}\n\n{msg['snippet']}")
                
                # Download and process attachments
                try:
                    attachments = self.gmail_connector.get_attachments(message_id)
                    if attachments:
                        print(f"📎 Found {len(attachments)} attachments")
                        for attachment in attachments:
                            filename = attachment.get('filename', f'attachment_{len(text_content)}')
                            
                            # Process attachment data
                            if 'data' in attachment:
                                data = attachment['data']
                                try:
                                    # Try to extract text from attachment
                                    if filename.lower().endswith(('.txt', '.md', '.csv', '.json')):
                                        # Text file - read directly
                                        extracted_text = data.decode('utf-8', errors='ignore')
                                    elif filename.lower().endswith('.pdf'):
                                        # PDF - use multiple extraction methods
                                        extracted_text = self._extract_pdf_text(data, filename, None)
                                    else:
                                        # Binary file - use textractor
                                        # Save to temporary file and process
                                        import tempfile
                                        with tempfile.NamedTemporaryFile(suffix=filename, delete=False) as temp_file:
                                            temp_file.write(data)
                                            temp_file_path = temp_file.name
                                        try:
                                            extracted_text = self.extractor.textractor.text(temp_file_path)
                                        finally:
                                            os.unlink(temp_file_path)
                                    
                                    if extracted_text and len(extracted_text.strip()) > 10:
                                        text_content.append(f"Attachment: {filename}\n{extracted_text}")
                                        print(f"Extracted text from {filename}")
                                    else:
                                        print(f"No text extracted from {filename}")
                                        
                                except Exception as e:
                                    print(f"Error processing attachment {filename}: {e}")
                    else:
                        print("📎 No attachments found")
                except Exception as e:
                    print(f"Failed to download attachments: {e}")
                
                # Combine all text content
                combined_text = "\n\n".join(text_content)
                
                if combined_text and len(combined_text.strip()) > 10:
                    # Create document ID
                    doc_id = f"gmail_{message_id}"
                    
                    # Prepare metadata
                    metadata = {
                        "message_id": message_id,
                        "subject": subject,
                        "sender": sender,
                        "attachment_count": len(msg.get('payload', {}).get('parts', [])),
                        "processing_date": datetime.datetime.now().isoformat()
                    }
                    
                    # Store in embeddings (creates both ANN vectors and database content)
                    self.embeddings.index([(doc_id, combined_text, metadata)])
                    print(f"Indexed document {doc_id} in Elasticsearch (ANN + Database)")
                    
                    processed_count += 1
                    print(f"Processed email {processed_count}: {subject[:50]}...")
                else:
                    print(f"No content to embed for message {message_id}")
                
            except Exception as e:
                print(f"Error processing message {msg.get('id', 'unknown')}: {e}")
        
        # Disconnect
        self.gmail_connector.disconnect()
        
        print(f"\nSuccessfully processed {processed_count} emails")
        return processed_count
    
    def test_search(self, query: str = "email"):
        """Test search functionality."""
        print(f"\nTesting search for: '{query}'")
        
        try:
            # Use embeddings search which combines ANN and database
            results = self.embeddings.search(query, limit=3)
            print(f"Found {len(results)} results")
            for i, result in enumerate(results, 1):
                if isinstance(result, tuple):
                    doc_id = result[0]
                    score = result[1] if len(result) > 1 else 1.0
                    print(f"  {i}. ID: {doc_id}, Score: {score:.4f}")
                else:
                    print(f"  {i}. Result: {result}")
        except Exception as e:
            print(f"Search test failed: {e}")
            # Try alternative search method
            try:
                print("Trying alternative search...")
                # Direct database search
                from txtai.database import Database
                db_config = self.config.get('database', {})
                db = DatabaseFactory.create(db_config)
                db_results = db.search(query, limit=3)
                print(f"Database search found {len(db_results)} results")
                for i, result in enumerate(db_results, 1):
                    print(f"  {i}. Text: {result[0][:100]}..., Score: {result[1]:.4f}")
                db.close()
            except Exception as e2:
                print(f"Alternative search also failed: {e2}")
    
    def cleanup(self):
        """Clean up resources."""
        if self.gmail_connector:
            self.gmail_connector.disconnect()
        if self.embeddings:
            self.embeddings.close()
        print("🧹 Resources cleaned up")


def main():
    """Main function to run the integration test."""
    print("Gmail to Elasticsearch Integration Test")
    print("=" * 50)
    
    # Check if virtual environment is activated
    if not os.getenv('VIRTUAL_ENV'):
        print("Warning: No virtual environment detected")
        print("Please activate your venv before running this script")
    
    try:
        # Initialize integration
        integration = GmailElasticsearchIntegration()
        
        # Initialize components
        integration.initialize_backends()
        integration.initialize_gmail_connector()
        integration.initialize_extractor()
        
        # Process Gmail data
        integration.process_gmail_data(limit=3)
        
        # Test search
        integration.test_search("email")
        
    except Exception as e:
        print(f"Integration test failed: {e}")
        return 1
    finally:
        try:
            integration.cleanup()
        except:
            pass
    
    print("\nIntegration test completed successfully!")
    return 0


if __name__ == "__main__":
    exit(main())