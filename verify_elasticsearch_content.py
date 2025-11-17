#!/usr/bin/env python3
"""
Verify Gmail Attachments Indexed in Elasticsearch

This script checks what's actually stored in Elasticsearch indices
to confirm attachments are properly indexed.
"""

import os
import sys
import yaml
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src" / "python"))

from txtai.database.factory import DatabaseFactory
from elasticsearch import Elasticsearch

def verify_elasticsearch_content():
    """Verify what's stored in Elasticsearch."""
    print("🔍 Verifying Elasticsearch Content")
    print("=" * 50)
    
    # Load configuration
    with open("gmail_elasticsearch_config.yaml", 'r') as f:
        config = yaml.safe_load(f)
    
    es_config = config.get('elasticsearch', {})
    db_config = config.get('database', {})
    
    # Connect to Elasticsearch
    es = Elasticsearch(
        hosts=es_config.get('hosts', ['http://localhost:9200']),
        verify_certs=es_config.get('verify_certs', True)
    )
    
    # Check indices
    print("\n📚 Checking Elasticsearch Indices:")
    indices = es.cat.indices(format='json')
    for index in indices:
        print(f"  - {index['index']}: {index['docs.count']} docs, {index['store.size']} size")
    
    # Check content index
    content_index = db_config.get('content_index', 'gmail_content')
    print(f"\n📄 Content Index: {content_index}")
    
    try:
        # Get sample documents from content index
        result = es.search(
            index=content_index,
            body={
                "query": {"match_all": {}},
                "size": 5,
                "sort": [{"_id": {"order": "desc"}}]
            }
        )
        
        print(f"Found {len(result['hits']['hits'])} documents:")
        for hit in result['hits']['hits']:
            doc_id = hit['_id']
            score = hit['_score']
            source = hit['_source']
            
            print(f"\n📧 Document ID: {doc_id}")
            print(f"   Score: {score:.4f}")
            
            # Show metadata
            if 'metadata' in source:
                metadata = source['metadata']
                print(f"   Subject: {metadata.get('subject', 'N/A')}")
                print(f"   Sender: {metadata.get('sender', 'N/A')}")
                print(f"   Attachments: {metadata.get('attachment_count', 0)}")
                print(f"   Processed: {metadata.get('processing_date', 'N/A')}")
            
            # Show content preview
            text = source.get('text', source.get('content', ''))
            if text:
                preview = text[:200].replace('\n', ' ').strip()
                print(f"   Content: {preview}...")
            
    except Exception as e:
        print(f"⚠️  Error querying content index: {e}")
    
    # Check embeddings index
    embeddings_index = config.get('embeddings', {}).get('index', 'gmail_embeddings')
    print(f"\n🔢 Embeddings Index: {embeddings_index}")
    
    try:
        # Get vector documents
        result = es.search(
            index=embeddings_index,
            body={
                "query": {"match_all": {}},
                "size": 5,
                "sort": [{"_id": {"order": "desc"}}]
            }
        )
        
        print(f"Found {len(result['hits']['hits'])} embedding documents:")
        for hit in result['hits']['hits']:
            doc_id = hit['_id']
            source = hit['_source']
            
            print(f"\n🔢 Embedding ID: {doc_id}")
            
            # Show vector info
            if 'values' in source:
                vector = source['values']
                print(f"   Vector dimensions: {len(vector)}")
                print(f"   Vector sample: {vector[:5]}...")
            
            # Show metadata
            if 'metadata' in source:
                metadata = source['metadata']
                print(f"   Document ID: {metadata.get('id', 'N/A')}")
                
    except Exception as e:
        print(f"⚠️  Error querying embeddings index: {e}")
    
    # Test search functionality
    print(f"\n🔍 Testing Search Queries:")
    
    # Initialize database for search
    database = DatabaseFactory.create(db_config)
    
    test_queries = ["attachment", "pdf", "email", "test"]
    for query in test_queries:
        try:
            results = database.search(query, limit=2)
            print(f"\n🔎 Query: '{query}' - Found {len(results)} results")
            for i, (text, score) in enumerate(results, 1):
                preview = text[:100].replace('\n', ' ').strip()
                print(f"   {i}. Score: {score:.4f}, Text: {preview}...")
        except Exception as e:
            print(f"   ⚠️  Search failed: {e}")
    
    database.close()
    print(f"\n✅ Verification completed!")

if __name__ == "__main__":
    verify_elasticsearch_content()
