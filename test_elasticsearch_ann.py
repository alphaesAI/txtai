#!/usr/bin/env python3
"""
Test script for Elasticsearch ANN backend.

This script demonstrates how to use the Elasticsearch ANN backend
for indexing and searching vector embeddings.
"""

import numpy as np
from src.python.txtai.ann.dense.elasticsearch import Elasticsearch

def test_elasticsearch_ann():
    """Test Elasticsearch ANN functionality."""
    # Configuration
    config = {
        "elasticsearch": {
            "hosts": ["http://localhost:9200"],
            "index": "test_embeddings",
            "dimension": 384,
            "similarity": "cosine",
            "verify_certs": False,
            "timeout": 30
        }
    }

    # Initialize the Elasticsearch backend
    print("Initializing Elasticsearch ANN backend...")
    es_ann = Elasticsearch(config)
    
    try:
        # Test data
        documents = [
            {
                "id": "doc1",
                "text": "First document about artificial intelligence",
                "embedding": np.random.rand(384).astype(np.float32).tolist(),
                "tags": {"category": "ai", "source": "test"}
            },
            {
                "id": "doc2",
                "text": "Second document about machine learning",
                "embedding": np.random.rand(384).astype(np.float32).tolist(),
                "tags": {"category": "ml", "source": "test"}
            },
            {
                "id": "doc3",
                "text": "Third document about deep learning",
                "embedding": np.random.rand(384).astype(np.float32).tolist(),
                "tags": {"category": "dl", "source": "test"}
            }
        ]

        # Index documents
        print("\nIndexing test documents...")
        es_ann.index([(doc["id"], doc["embedding"], doc) for doc in documents])
        print(f"Indexed {len(documents)} documents")

        # Create a test query vector (similar to doc1)
        query_vector = np.array(documents[0]["embedding"]) + np.random.normal(0, 0.1, 384)
        
        # Search for similar documents
        print("\nSearching for similar documents...")
        results = es_ann.search(query_vector, limit=2)
        
        print("\nSearch results:")
        for i, (doc_id, score) in enumerate(results, 1):
            print(f"{i}. Document ID: {doc_id}, Score: {score:.4f}")
            
        # Get document count
        count = es_ann.count()
        print(f"\nTotal documents in index: {count}")
        
        # Test delete
        print("\nTesting delete operation...")
        es_ann.delete(["doc1"])
        print("Deleted document 'doc1'")
        
        # Verify delete
        new_count = es_ann.count()
        print(f"Documents after delete: {new_count}")
        
    except Exception as e:
        print(f"Error during test: {str(e)}")
        raise
    finally:
        # Clean up
        es_ann.close()
        print("\nTest completed. Connection closed.")

if __name__ == "__main__":
    test_elasticsearch_ann()
