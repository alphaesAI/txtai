import pandas as pd
from transformers.df_to_json import DataTransformer

def test_data_transformer():
    # Sample DataFrame (pretend this came from extraction)
    data = {
        "id": [1, 2, 3],
        "name": ["Logidhasan", "Nisha", "Deva"],
        "department": ["AI", "Data Science", "Tamil"],
        "marks": [95, 90, 100]
    }
    df = pd.DataFrame(data)

    # Transform to JSON format for Elasticsearch
    index_name = "students_index"
    json_output = DataTransformer.transform_to_json(df, index_name)

    print("\nTransformed JSON Output:")
    for record in json_output:
        print(record)

    # Simple validation
    assert isinstance(json_output, list), "Output should be a list"
    assert all("_index" in doc and "_source" in doc for doc in json_output), "Missing required keys"

if __name__ == "__main__":
    test_data_transformer()
    print("\nDataTransformer test completed successfully.")
