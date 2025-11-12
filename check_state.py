#!/usr/bin/env python3
"""
Script to check ETL state manager last extraction dates
"""

import json
from pathlib import Path

def check_extraction_state():
    """Check and display the current extraction state"""
    
    # Path to state file (from your config)
    state_file = Path("/tmp/etl_state.json")
    
    if not state_file.exists():
        print("No state file found at /tmp/etl_state.json")
        return
    
    # Load state
    with open(state_file, 'r') as f:
        state = json.load(f)
    
    print("=== ETL Extraction State ===")
    print(f"State file: {state_file}")
    print()
    
    # Group by table
    tables = {}
    for key, value in state.items():
        if '_timestamp' in key:
            continue  # Skip timestamp metadata
        
        # Extract table and column from key (format: "table.column")
        if '.' in key:
            table, column = key.split('.', 1)
            if table not in tables:
                tables[table] = {}
            tables[table][column] = value
    
    # Display by table
    for table_name, columns in tables.items():
        print(f"Table: {table_name}")
        for column, last_value in columns.items():
            print(f"  {column}: {last_value}")
        print()
    
    # Show raw state if needed
    print("=== Raw State ===")
    for key, value in state.items():
        print(f"{key}: {value}")

if __name__ == "__main__":
    check_extraction_state()
