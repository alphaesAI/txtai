from typing import Dict, Any, List
import pandas as pd
import json

class DataTransformer:
    """Transformer class to convert DataFrame to JSON"""
    @staticmethod
    def transform_to_json(df: pd.DataFrame, index_name: str) -> List[Dict[str, Any]]:
        """Transform DataFrame to JSON format suitable for Elasticsearch"""
        json_records = []
        for _, row in df.iterrows():
            document = {
                "_index": index_name,
                "_source": row.to_dict()
            }
            json_records.append(document)
        return json_records