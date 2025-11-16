"""Elasticsearch loader for ETL pipeline."""

from typing import Any, Dict, List, Optional
import logging
import time

from .base import BaseLoader


class ElasticsearchLoader(BaseLoader):
    """
    Loader for Elasticsearch.
    Supports single document indexing and bulk operations.
    """
    
    def __init__(
        self,
        connector: Any,
        index_name: str,
        doc_type: Optional[str] = None,
        id_field: Optional[str] = None,
        bulk_size: int = 1000,
        max_retries: int = 3,
        raise_on_error: bool = True,
        **kwargs
    ):
        """
        Initialize Elasticsearch loader.
        
        Args:
            connector: Elasticsearch connector instance
            index_name: Name of the Elasticsearch index
            doc_type: Document type (deprecated in ES 7+)
            id_field: Field to use as document ID (None for auto-generated)
            bulk_size: Batch size for bulk operations
            max_retries: Maximum number of retries for failed operations
            raise_on_error: Raise exception on errors
            **kwargs: Additional loader parameters
        """
        super().__init__(connector, **kwargs)
        self.index_name = index_name
        self.doc_type = doc_type
        self.id_field = id_field
        self.bulk_size = bulk_size
        self.max_retries = max_retries
        self.raise_on_error = raise_on_error
        
        # Statistics tracking
        self._total_loaded = 0
        self._total_failed = 0
        self._last_load_time = None
        
        # Setup logging
        self.logger = logging.getLogger(__name__)
    
    def load(self, data: Any, **kwargs) -> bool:
        """
        Load a single document to Elasticsearch.
        
        Args:
            data: Document to load (dictionary)
            **kwargs: Additional loading parameters
            
        Returns:
            True if loading was successful
        """
        if not self.validate_connection():
            raise RuntimeError("Elasticsearch connection is not valid")
        
        try:
            doc_id = self._get_document_id(data)
            
            # Index document
            response = self.connector.index_document(
                index=self.index_name,
                doc_id=doc_id,
                document=data
            )
            
            if response.get('result') in ['created', 'updated']:
                self._total_loaded += 1
                self._last_load_time = time.time()
                return True
            else:
                self._total_failed += 1
                self.logger.error(f"Failed to index document: {response}")
                return False
                
        except Exception as e:
            self._total_failed += 1
            self.logger.error(f"Error loading document to Elasticsearch: {e}")
            if self.raise_on_error:
                raise
            return False
    
    def batch_load(self, data_batch: List[Any], **kwargs) -> bool:
        """
        Load a batch of documents to Elasticsearch.
        
        Args:
            data_batch: List of documents to load
            **kwargs: Additional loading parameters
            
        Returns:
            True if batch loading was successful
        """
        if not self.validate_connection():
            raise RuntimeError("Elasticsearch connection is not valid")
        
        if not data_batch:
            return True
        
        try:
            # Prepare documents for bulk indexing
            documents = []
            for doc in data_batch:
                doc_dict = doc.copy() if isinstance(doc, dict) else doc
                doc_id = self._get_document_id(doc_dict)
                
                if doc_id:
                    doc_dict['id'] = doc_id
                
                documents.append(doc_dict)
            
            # Bulk index
            response = self.connector.bulk_index(
                index=self.index_name,
                documents=documents
            )
            
            # Update statistics
            success_count = len(response.get('items', []))
            error_count = 0
            
            for item in response.get('items', []):
                if 'error' in item.get('index', {}):
                    error_count += 1
            
            self._total_loaded += success_count - error_count
            self._total_failed += error_count
            self._last_load_time = time.time()
            
            if error_count > 0 and self.raise_on_error:
                self.logger.error(f"Bulk loading had {error_count} errors")
                return False
            
            return error_count == 0
            
        except Exception as e:
            self._total_failed += len(data_batch)
            self.logger.error(f"Error in batch loading to Elasticsearch: {e}")
            if self.raise_on_error:
                raise
            return False
    
    def load_stream(self, data_stream, **kwargs) -> Dict[str, int]:
        """
        Load data from a stream/generator.
        
        Args:
            data_stream: Generator yielding documents
            **kwargs: Additional loading parameters
            
        Returns:
            Statistics dictionary
        """
        batch = []
        stats = {"loaded": 0, "failed": 0}
        
        for document in data_stream:
            batch.append(document)
            
            if len(batch) >= self.bulk_size:
                if self.batch_load(batch, **kwargs):
                    stats["loaded"] += len(batch)
                else:
                    stats["failed"] += len(batch)
                batch = []
        
        # Load remaining documents
        if batch:
            if self.batch_load(batch, **kwargs):
                stats["loaded"] += len(batch)
            else:
                stats["failed"] += len(batch)
        
        return stats
    
    def _get_document_id(self, document: Dict[str, Any]) -> Optional[str]:
        """
        Extract document ID from document.
        
        Args:
            document: Document dictionary
            
        Returns:
            Document ID or None if not found
        """
        if self.id_field and self.id_field in document:
            return str(document[self.id_field])
        elif 'id' in document:
            return str(document['id'])
        elif '_id' in document:
            return str(document['_id'])
        else:
            return None
    
    def create_index(self, mapping: Optional[Dict[str, Any]] = None) -> bool:
        """
        Create Elasticsearch index with optional mapping.
        
        Args:
            mapping: Index mapping configuration
            
        Returns:
            True if index creation was successful
        """
        if not self.validate_connection():
            raise RuntimeError("Elasticsearch connection is not valid")
        
        try:
            # Check if index exists
            if self.connector.connection.indices.exists(index=self.index_name):
                self.logger.info(f"Index {self.index_name} already exists")
                return True
            
            # Create index
            if mapping:
                response = self.connector.connection.indices.create(
                    index=self.index_name,
                    body={"mappings": mapping}
                )
            else:
                response = self.connector.connection.indices.create(
                    index=self.index_name
                )
            
            return response.get('acknowledged', False)
            
        except Exception as e:
            self.logger.error(f"Failed to create index {self.index_name}: {e}")
            raise
    
    def delete_index(self) -> bool:
        """
        Delete Elasticsearch index.
        
        Returns:
            True if index deletion was successful
        """
        if not self.validate_connection():
            raise RuntimeError("Elasticsearch connection is not valid")
        
        try:
            response = self.connector.connection.indices.delete(
                index=self.index_name,
                ignore=[400, 404]
            )
            
            return response.get('acknowledged', False)
            
        except Exception as e:
            self.logger.error(f"Failed to delete index {self.index_name}: {e}")
            raise
