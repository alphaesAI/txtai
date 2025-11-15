#!/usr/bin/env python3
"""Show unread emails and attachments."""

import os
import sys
import tempfile
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

import yaml
from etl_pipeline.unstructure.connector import ConnectorFactory

def main():
    """Show unread emails."""
    print("=== Gmail Unread Emails ===")
    
    # Load config
    config_path = Path("etl_pipeline/config/config.yaml")
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    
    gmail_config = config["unstructure"]["gmail"]
    connector = ConnectorFactory.create("gmail", gmail_config)
    
    # Connect
    connector.connect()
    
    # Create output file
    output_file = Path("unread_emails.txt")
    attachments_dir = Path("email_attachments")
    attachments_dir.mkdir(exist_ok=True)
    
    try:
        # Fetch unread messages
        messages = connector.fetch_unread_messages()
        
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(f"Gmail Unread Emails Report\n")
            f.write(f"Generated: {Path.cwd()}\n")
            f.write(f"Found {len(messages)} unread messages:\n")
            f.write("=" * 50 + "\n\n")
            
            for i, msg in enumerate(messages, 1):
                f.write(f"--- Email {i} ---\n")
                
                # Get metadata
                metadata = connector.fetch_message_metadata(msg)
                f.write(f"From: {metadata.get('from', 'N/A')}\n")
                f.write(f"Subject: {metadata.get('subject', 'N/A')}\n")
                f.write(f"Date: {metadata.get('date', 'N/A')}\n")
                f.write(f"Message ID: {metadata.get('message_id', 'N/A')}\n")
                
                # Get body
                body = connector.fetch_message_body(msg)
                if body.get('text'):
                    f.write(f"\nText Body:\n{body['text']}\n")
                
                if body.get('html'):
                    f.write(f"\nHTML Body:\n{body['html']}\n")
                
                # Download and save attachments
                attachments = connector.download_attachments(msg, str(attachments_dir))
                if attachments:
                    f.write(f"\nAttachments ({len(attachments)}):\n")
                    for att in attachments:
                        size = os.path.getsize(att)
                        f.write(f"  - {os.path.basename(att)} ({size} bytes)\n")
                else:
                    f.write("\nNo attachments\n")
                
                f.write("\n" + "-" * 50 + "\n\n")
        
        print(f"✅ Saved {len(messages)} emails to: {output_file}")
        print(f"📎 Attachments saved to: {attachments_dir}")
        
        # Show preview in terminal
        with open(output_file, "r", encoding="utf-8") as f:
            content = f.read()
            print(f"\nPreview (first 1000 chars):\n{content[:1000]}...")
    
    finally:
        connector.disconnect()

if __name__ == "__main__":
    main()
