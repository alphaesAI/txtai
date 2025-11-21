#!/usr/bin/env python3
"""
Test script for ElasticsearchConnector.
"""

import os
import sys
import json
import time
import logging
import unittest
from typing import Dict, List, Any
from unittest.mock import patch, MagicMock

# Add the src directory to the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), 'src/python')))

from txtai.pipeline.connectors.elasticsearch import ElasticsearchConnector

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TestElasticsearchConnector(unittest.TestCase):    
    @classmethod
    def setUpClass(cls):
        """Initialize test environment."""
        # Test configuration
        cls.config = {
            "connection_string": "http://localhost:9200",
            "verify_certs": False,
            "timeout": 30,
            "max_retries": 3,
            "test_index": "test_connector_index"
        }
        
        # Test data
        cls.test_doc = {
            "id": "1",
            "title": "Test Document",
            "content": "This is a test document for Elasticsearch connector",
            "tags": ["test", "elasticsearch", "connector"]
        }
        
        # Initialize connector
        cls.connector = ElasticsearchConnector(
            connection_string=cls.config["connection_string"],
            verify_certs=cls.config["verify_certs"],
            timeout=cls.config["timeout"],
            max_retries=cls.config["max_retries"]
        )
        
        # Connect to Elasticsearch
        cls.connector.connect()
    
    def test_01_connection(self):
        """Test connection to Elasticsearch."""
        self.assertTrue(hasattr(self.connector, 'client'))
        self.assertIsNotNone(self.connector.client)
        self.assertTrue(self.connector.test_connection())
        
    def test_02_index_operations(self):
        """Test document indexing and retrieval."""
        # Index document
        response = self.connector.index_document(
            index=self.config["test_index"],
            doc_id=self.test_doc["id"],
            document=self.test_doc
        )
        
        # Give Elasticsearch time to index the document
        time.sleep(1)
        
        # Verify document was indexed
        self.assertEqual(response["result"], "created")
        
        # Get document
        doc = self.connector.get_document(
            index=self.config["test_index"],
            doc_id=self.test_doc["id"]
        )
        self.assertEqual(doc["_source"]["title"], self.test_doc["title"])
    
    def test_03_search(self):
        """Test search functionality."""
        # Search for document
        query = {
            "query": {
                "match": {
                    "content": "test document"
                }
            }
        }
        
        results = self.connector.search(
            index=self.config["test_index"],
            query=query
        )
        
        self.assertGreater(results["hits"]["total"]["value"], 0)
    
    def test_04_bulk_operations(self):
        """Test bulk indexing."""
        # Prepare test data
        docs = [
            {"id": "2", "title": "Bulk Test 1", "content": "First bulk test document"},
            {"id": "3", "title": "Bulk Test 2", "content": "Second bulk test document"},
            {"id": "4", "title": "Bulk Test 3", "content": "Third bulk test document"}
        ]
        
        # Bulk index documents
        response = self.connector.bulk_index(
            index=self.config["test_index"],
            documents=docs
        )
        
        # Give Elasticsearch time to index the documents
        time.sleep(1)
        
        # Verify bulk operation
        self.assertEqual(len(response[1]), 0)  # No errors
        
        # Verify documents were indexed
        for doc in docs:
            result = self.connector.get_document(
                index=self.config["test_index"],
                doc_id=doc["id"]
            )
            self.assertEqual(result["_source"]["title"], doc["title"])
    
    def test_05_delete_document(self):
        """Test document deletion."""
        # Delete test document
        response = self.connector.delete_document(
            index=self.config["test_index"],
            doc_id=self.test_doc["id"]
        )
        
        self.assertEqual(response["result"], "deleted")
        
        # Verify document was deleted
        with self.assertRaises(Exception):
            self.connector.get_document(
                index=self.config["test_index"],
                doc_id=self.test_doc["id"]
            )
    
    @classmethod
    def tearDownClass(cls):
        """Clean up test data."""
        try:
            # Delete test index
            if cls.connector.client.indices.exists(index=cls.config["test_index"]):
                cls.connector.client.indices.delete(index=cls.config["test_index"])
                logger.info(f"Deleted test index: {cls.config['test_index']}")
            
            # Close connection
            cls.connector.close()
            logger.info("Closed Elasticsearch connection")
            
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")

if __name__ == "__main__":
    unittest.main()
