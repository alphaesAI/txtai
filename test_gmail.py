#!/usr/bin/env python3
"""
Minimal ETL Test Script for Gmail
Reads config from pipeline_config.yaml and extracts attachments & emails
"""

import os
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
    logger.info("Connected to Gmail successfully")
    
    # Get unread messages from Gmail
    messages = connector.get_messages(query="is:unread", max_results=10)
    logger.info(f"Fetched {len(messages)} unread messages from Gmail")
    
    # Process messages with textractor
    for i, msg in enumerate(messages[:5]):
        # Extract message metadata
        headers = {h['name'].lower(): h['value'] for h in msg.get('payload', {}).get('headers', [])}
        subject = headers.get('subject', 'No subject')
        sender = headers.get('from', 'Unknown sender')
        message_id = msg.get('id', 'unknown')
        
        logger.info(f"Processing message {i+1}/{len(messages)}: {subject} from {sender} (ID: {message_id})")
        
        # Create permanent storage directory for extracted data
        import datetime
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        storage_dir = f"/home/logi/txtai/static/extracted_data/gmail_{message_id}_{timestamp}"
        os.makedirs(storage_dir, exist_ok=True)
        logger.info(f"Created storage directory: {storage_dir}")
        
        sources_to_process = []
        
        # Save message snippet as text file
        if 'snippet' in msg and msg['snippet'].strip():
            snippet_file = os.path.join(storage_dir, "message_snippet.txt")
            with open(snippet_file, "w", encoding="utf-8") as f:
                f.write(f"Subject: {subject}\nFrom: {sender}\n\n{msg['snippet']}")
            sources_to_process.append(snippet_file)
            logger.info(f"Saved message snippet to: {snippet_file}")
        
        # Download attachments
        try:
            attachments = connector.get_attachments(message_id)
            if attachments:
                logger.info(f"Found {len(attachments)} attachments for message {message_id}")
                for attachment in attachments:
                    filename = attachment.get('filename', f'attachment_{len(sources_to_process)}')
                    file_path = os.path.join(storage_dir, filename)
                    
                    # Save attachment data to file
                    if 'data' in attachment:
                        # Data is already decoded by Gmail connector
                        data = attachment['data']
                        with open(file_path, 'wb') as f:
                            f.write(data)
                        sources_to_process.append(file_path)
                        logger.info(f"Saved attachment: {filename} ({len(data)} bytes)")
            else:
                logger.info("No attachments found for this message")
        except Exception as e:
            logger.warning(f"Failed to download attachments for message {message_id}: {e}")
        
        # Process all sources with textractor
        if sources_to_process:
            logger.info(f"Processing {len(sources_to_process)} files with textractor")
            
            for source_file in sources_to_process:
                try:
                    logger.info(f"Extracting text from: {os.path.basename(source_file)}")
                    
                    # Use textractor directly to extract text from file
                    try:
                        # Try to read file content directly for text files
                        if source_file.endswith('.txt'):
                            with open(source_file, 'r', encoding='utf-8') as f:
                                extracted_text = f.read()
                        else:
                            # Use textractor for other file types
                            extracted_text = extractor.textractor.text(source_file)
                        
                        # Save extracted text to a separate file
                        extracted_file = os.path.join(storage_dir, f"extracted_{os.path.basename(source_file)}.txt")
                        with open(extracted_file, 'w', encoding='utf-8') as f:
                            f.write(f"SOURCE: {os.path.basename(source_file)}\n")
                            f.write(f"MESSAGE: {subject}\n")
                            f.write(f"FROM: {sender}\n")
                            f.write(f"EXTRACTED ON: {datetime.datetime.now().isoformat()}\n")
                            f.write("="*60 + "\n")
                            f.write(extracted_text)
                        logger.info(f"Saved extracted text to: {extracted_file}")
                        
                        print(f"\n{'='*60}")
                        print(f"MESSAGE: {subject}")
                        print(f"FROM: {sender}")
                        print(f"FILE: {os.path.basename(source_file)}")
                        print(f"{'='*60}")
                        print(f"EXTRACTED TEXT:")
                        print(extracted_text)
                        print(f"{'='*60}\n")
                        
                        logger.info(f"Successfully extracted {len(extracted_text)} characters from {os.path.basename(source_file)}")
                    except Exception as tex_error:
                        logger.error(f"Textractor error: {tex_error}")
                        # Fallback to basic file reading
                        try:
                            with open(source_file, 'rb') as f:
                                content = f.read()
                            extracted_text = f"Binary file ({len(content)} bytes) - could not extract text"
                            
                            print(f"\n{'='*60}")
                            print(f"MESSAGE: {subject}")
                            print(f"FROM: {sender}")
                            print(f"FILE: {os.path.basename(source_file)}")
                            print(f"{'='*60}")
                            print(f"EXTRACTED TEXT:")
                            print(extracted_text)
                            print(f"{'='*60}\n")
                        except Exception as fallback_error:
                            logger.error(f"Fallback also failed: {fallback_error}")
                    
                except Exception as e:
                    logger.error(f"Failed to extract text from {source_file}: {e}")
        else:
            logger.warning(f"No content to process for message {message_id}")
        
        # Log completion for this message
        logger.info(f"Completed processing message {i+1}/{len(messages)}")
        logger.info(f"All data saved to: {storage_dir}")

    # Disconnect
    connector.disconnect()
    
except Exception as e:
    logger.error(f"Failed to extract emails: {e}")
