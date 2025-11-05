"""
Elasticsearch loader for indexing documents.
Supports both single and bulk loading modes.
"""
from typing import Any, Dict, List, Optional, Union
from elasticsearch.helpers import bulk, streaming_bulk
import logging
from datetime import datetime

from .base import BaseLoader


logger = logging.getLogger(__name__)


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
    
    def load(
        self,
        data: Dict[str, Any],
        **kwargs
    ) -> bool:
        """
        Load a single document to Elasticsearch.
        
        Args:
            data: Document data as dictionary
            **kwargs: Additional parameters (doc_id, routing, etc.)
            
        Returns:
            True if successful, False otherwise
        """
        if not self.validate(data):
            raise ValueError("Invalid data for loading")
        
        try:
            client = self.connector.get_connection()
            
            # Extract document ID if id_field is specified
            doc_id = kwargs.get('doc_id')
            if not doc_id and self.id_field and self.id_field in data:
                doc_id = data[self.id_field]
            
            # Add metadata
            if kwargs.get('add_timestamp', True):
                data['_indexed_at'] = datetime.utcnow().isoformat()
            
            # Index document
            response = client.index(
                index=self.index_name,
                id=doc_id,
                document=data,
                **{k: v for k, v in kwargs.items() if k not in ['doc_id', 'add_timestamp']}
            )
            
            logger.info(f"Indexed document {response['_id']} to {self.index_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error loading document: {e}")
            if self.raise_on_error:
                raise
            return False
    
    def load_batch(
        self,
        data: List[Dict[str, Any]],
        **kwargs
    ) -> bool:
        """
        Load multiple documents using bulk API.
        
        Args:
            data: List of documents to load
            **kwargs: Additional parameters
            
        Returns:
            True if successful, False otherwise
        """
        if not data:
            logger.warning("No data to load")
            return True
        
        try:
            client = self.connector.get_connection()
            
            # Prepare bulk actions
            actions = self._prepare_bulk_actions(data, **kwargs)
            
            # Execute bulk operation
            success, failed = bulk(
                client,
                actions,
                chunk_size=self.bulk_size,
                max_retries=self.max_retries,
                raise_on_error=self.raise_on_error,
                raise_on_exception=self.raise_on_error
            )
            
            logger.info(
                f"Bulk load completed: {success} successful, {len(failed)} failed"
            )
            
            if failed and self.raise_on_error:
                raise Exception(f"Bulk load had {len(failed)} failures")
            
            return len(failed) == 0
            
        except Exception as e:
            logger.error(f"Error in bulk load: {e}")
            if self.raise_on_error:
                raise
            return False
    
    def load_streaming(
        self,
        data: List[Dict[str, Any]],
        **kwargs
    ) -> Dict[str, int]:
        """
        Load documents using streaming bulk API (memory efficient).
        
        Args:
            data: List of documents to load
            **kwargs: Additional parameters
            
        Returns:
            Dictionary with success and failure counts
        """
        try:
            client = self.connector.get_connection()
            
            # Prepare bulk actions
            actions = self._prepare_bulk_actions(data, **kwargs)
            
            # Execute streaming bulk
            success_count = 0
            error_count = 0
            
            for ok, response in streaming_bulk(
                client,
                actions,
                chunk_size=self.bulk_size,
                max_retries=self.max_retries,
                raise_on_error=False
            ):
                if ok:
                    success_count += 1
                else:
                    error_count += 1
                    logger.error(f"Failed to index document: {response}")
            
            logger.info(
                f"Streaming bulk completed: {success_count} successful, "
                f"{error_count} failed"
            )
            
            return {'success': success_count, 'failed': error_count}
            
        except Exception as e:
            logger.error(f"Error in streaming bulk load: {e}")
            if self.raise_on_error:
                raise
            return {'success': 0, 'failed': len(data)}
    
    def _prepare_bulk_actions(
        self,
        data: List[Dict[str, Any]],
        **kwargs
    ) -> List[Dict[str, Any]]:
        """
        Prepare bulk actions for Elasticsearch.
        
        Args:
            data: List of documents
            **kwargs: Additional parameters
            
        Returns:
            List of bulk action dictionaries
        """
        actions = []
        add_timestamp = kwargs.get('add_timestamp', True)
        
        for doc in data:
            # Extract document ID
            doc_id = None
            if self.id_field and self.id_field in doc:
                doc_id = doc[self.id_field]
            
            # Add metadata
            if add_timestamp:
                doc['_indexed_at'] = datetime.utcnow().isoformat()
            
            # Create action
            action = {
                '_op_type': 'index',
                '_index': self.index_name,
                '_source': doc
            }
            
            if doc_id:
                action['_id'] = doc_id
            
            # Add routing if specified
            if 'routing' in kwargs:
                action['routing'] = kwargs['routing']
            
            actions.append(action)
        
        return actions
    
    def validate(self, data: Any) -> bool:
        """
        Validate data before loading.
        
        Args:
            data: Data to validate
            
        Returns:
            True if valid, False otherwise
        """
        if isinstance(data, list):
            # Validate batch
            if not data:
                logger.warning("Empty data list")
                return True
            
            for item in data:
                if not isinstance(item, dict):
                    logger.error(f"Invalid item in batch: {type(item)}")
                    return False
        elif isinstance(data, dict):
            # Valid single document
            return True
        else:
            logger.error(f"Invalid data type: {type(data)}")
            return False
        
        return True
    
    def delete_by_id(
        self,
        doc_id: str,
        **kwargs
    ) -> bool:
        """
        Delete a document by ID.
        
        Args:
            doc_id: Document ID
            **kwargs: Additional parameters
            
        Returns:
            True if successful, False otherwise
        """
        try:
            client = self.connector.get_connection()
            response = client.delete(
                index=self.index_name,
                id=doc_id,
                **kwargs
            )
            
            logger.info(f"Deleted document {doc_id} from {self.index_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error deleting document: {e}")
            if self.raise_on_error:
                raise
            return False
    
    def delete_by_query(
        self,
        query: Dict[str, Any],
        **kwargs
    ) -> int:
        """
        Delete documents matching a query.
        
        Args:
            query: Elasticsearch query
            **kwargs: Additional parameters
            
        Returns:
            Number of documents deleted
        """
        try:
            client = self.connector.get_connection()
            response = client.delete_by_query(
                index=self.index_name,
                body={'query': query},
                **kwargs
            )
            
            deleted = response.get('deleted', 0)
            logger.info(f"Deleted {deleted} documents from {self.index_name}")
            return deleted
            
        except Exception as e:
            logger.error(f"Error in delete by query: {e}")
            if self.raise_on_error:
                raise
            return 0
    
    def update_by_id(
        self,
        doc_id: str,
        update_data: Dict[str, Any],
        **kwargs
    ) -> bool:
        """
        Update a document by ID.
        
        Args:
            doc_id: Document ID
            update_data: Data to update
            **kwargs: Additional parameters
            
        Returns:
            True if successful, False otherwise
        """
        try:
            client = self.connector.get_connection()
            response = client.update(
                index=self.index_name,
                id=doc_id,
                body={'doc': update_data},
                **kwargs
            )
            
            logger.info(f"Updated document {doc_id} in {self.index_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error updating document: {e}")
            if self.raise_on_error:
                raise
            return False
    
    def count(self, query: Optional[Dict[str, Any]] = None) -> int:
        """
        Count documents in index.
        
        Args:
            query: Optional query to filter documents
            
        Returns:
            Document count
        """
        try:
            client = self.connector.get_connection()
            
            if query:
                response = client.count(index=self.index_name, body={'query': query})
            else:
                response = client.count(index=self.index_name)
            
            count = response['count']
            logger.info(f"Document count in {self.index_name}: {count}")
            return count
            
        except Exception as e:
            logger.error(f"Error counting documents: {e}")
            return 0
