#!/usr/bin/env python3
"""
Minimal ETL Test Script for Gmail
Reads config from pipeline_config.yaml and extracts attachments & emails
"""

import yaml
import logging
from pathlib import Path
from src.python.txtai.pipeline.etl import ETLPipeline

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("GMAIL_ETL_TEST")

# Load pipeline_config.yaml
CONFIG_PATH = Path(__file__).parent / "pipeline_config.yaml"
if not CONFIG_PATH.exists():
    logger.error(f"Config file not found: {CONFIG_PATH}")
    exit(1)

with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)

# Initialize ETL
etl = ETLPipeline(config=config)

# Get Gmail connector from config
gmail_connector_name = next((name for name in etl.list_connectors() if "gmail" in name.lower()), None)
if not gmail_connector_name:
    logger.error("No Gmail connector found in config")
    exit(1)

connector = etl.get_connector(gmail_connector_name)
logger.info(f"Gmail connector loaded: {gmail_connector_name}")

# Get textractor for processing Gmail data
textractor_name = next((name for name in etl.list_extractors() if "textractor" in name.lower()), None)
if not textractor_name:
    logger.error("No textractor found in config")
    exit(1)

extractor = etl.get_extractor(textractor_name)
logger.info(f"Textractor loaded: {textractor_name}")

# Extract emails using Gmail connector
try:
    # Connect to Gmail
    connector.connect()
    
    # Get messages from Gmail
    messages = connector.get_messages(query="", max_results=5)
    logger.info(f"Fetched {len(messages)} messages from Gmail")
    
    # Display message info
    for i, msg in enumerate(messages[:5]):
        logger.info(f"Message {i+1}: {msg.get('subject', 'No subject')} from {msg.get('sender', 'Unknown sender')}")
    
    # Disconnect
    connector.disconnect()
    
except Exception as e:
    logger.error(f"Failed to extract emails: {e}")
