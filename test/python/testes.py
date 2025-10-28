import os
import sys
from pathlib import Path

sys.path.append(os.path.abspath("paper_curator/"))

from src.elasticsearch.connection import ElasticsearchConfig, ElasticsearchClientFactory
from elasticsearch import Elasticsearch

def main():

    config = ElasticsearchConfig(
        host="http://localhost:9200",
        username="elastic",
        password="q+nR3Kse*QW5kpoacWn3",
        verify_certs=False
        )


    factory = ElasticsearchClientFactory(config)

    try:
        client = factory.create_client()
        print("Connected to Elasticsearch successfully!")
        print("Cluster info:", client.info())
    except Exception as e:
        print("Failed to connect: ", e)


if __name__ == "__main__":
    main()