"""
Quick script to verify what's actually in Elasticsearch
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from elasticsearch import Elasticsearch

# Connect to Elasticsearch
es = Elasticsearch(['http://localhost:9200'])

index_name = "etl_test_students"

print("=" * 70)
print("ELASTICSEARCH INDEX VERIFICATION")
print("=" * 70)

# Check if index exists
if not es.indices.exists(index=index_name):
    print(f"❌ Index '{index_name}' does not exist!")
    sys.exit(1)

print(f"✓ Index '{index_name}' exists\n")

# Get index stats
stats = es.indices.stats(index=index_name)
doc_count = stats['indices'][index_name]['total']['docs']['count']
print(f"Total documents in index: {doc_count}\n")

# Get all documents
print("Fetching all documents...")
result = es.search(
    index=index_name,
    body={
        "query": {"match_all": {}},
        "size": 100,
        "sort": [{"id": "asc"}]
    }
)

print(f"Documents found: {result['hits']['total']['value']}\n")

# Print all document IDs
print("Document IDs in Elasticsearch:")
print("-" * 70)
for hit in result['hits']['hits']:
    doc = hit['_source']
    doc_id = doc.get('id', 'N/A')
    name = doc.get('name', 'N/A')
    department = doc.get('department', 'N/A')
    marks = doc.get('marks', 'N/A')
    print(f"  ID: {doc_id:>3} | Name: {name:<15} | Dept: {department:<10} | Marks: {marks}")

print("-" * 70)

# Check specifically for ID 8
print("\n🔍 Checking for ID 8 specifically...")
result_id8 = es.search(
    index=index_name,
    body={
        "query": {"term": {"id": 8}},
        "size": 1
    }
)

if result_id8['hits']['total']['value'] > 0:
    print("✓ ID 8 EXISTS in Elasticsearch!")
    doc = result_id8['hits']['hits'][0]['_source']
    print(f"  Document: {doc}")
else:
    print("❌ ID 8 NOT FOUND in Elasticsearch!")
    print("\n📋 Missing record details:")
    print("  - Expected ID: 8")
    print("  - Status: Not indexed")
    print("  - Possible cause: Load operation may have failed silently")

print("\n" + "=" * 70)
