import os
from typing import List, Dict, Tuple, Any
from ..data.textractor import Textractor


class DataTransformer:
    """
    Transformer to convert extractor output into a format suitable
    for txtai embeddings via Textractor.
    """

    def __init__(self, textractor_config: Dict[str, Any] = None, filetohtml_config: Dict[str, Any] = None):
        """
        Initialize Textractor with optional config.

        Args:
            textractor_config: dict with Textractor parameters
            filetohtml_config: dict with FileToHTML backend parameters
        """
        textractor_config = textractor_config or {}
        filetohtml_config = filetohtml_config or {}
        
        # Merge backend configuration into textractor config
        if 'backend' in filetohtml_config:
            textractor_config['backend'] = filetohtml_config['backend']
            
        self.textractor = Textractor(**textractor_config)

    def transform(self, data: List[Dict[str, Any]]) -> List[Tuple[str, str, Dict[str, Any]]]:
        """
        Transform extractor output to embeddings-ready tuples.

        Args:
            data: List of extractor output dictionaries, each with keys:
                  - metadata: dict
                  - html: str
                  - attachments: List[str]

        Returns:
            List of tuples: (id, text, tags)
        """
        results: List[Tuple[str, str, Dict[str, Any]]] = []

        for item in data:
            metadata = item.get("metadata", {})
            email_id = metadata.get("id", "unknown")
            tags = {
                "subject": metadata.get("subject"),
                "from": metadata.get("from"),
                "to": metadata.get("to"),
                "date": metadata.get("date")
            }

            # 1️⃣ Process HTML body
            html_content = item.get("html")
            if html_content:
                segments = self.textractor.text(html_content)
                # Handle both string and list returns from Textractor
                if isinstance(segments, str):
                    results.append((email_id, segments, tags))
                else:
                    for seg in segments:
                        results.append((email_id, seg, tags))

            # 2️⃣ Process attachments
            attachments = item.get("attachments", [])
            for filepath in attachments:
                if os.path.exists(filepath):
                    segments = self.textractor.text(filepath)
                    # Handle both string and list returns from Textractor
                    if isinstance(segments, str):
                        results.append((os.path.basename(filepath), segments, tags))
                    else:
                        for seg in segments:
                            results.append((os.path.basename(filepath), seg, tags))

        return results
