"""
State management for incremental extraction.
"""
from typing import Any, Dict, Optional
import json
import os
from datetime import datetime
from pathlib import Path
import logging


logger = logging.getLogger(__name__)


class StateManager:
    """
    Manages extraction state for incremental loads.
    Stores last extracted values for CDC and date-based extraction.
    """
    
    def __init__(self, state_file: str):
        """
        Initialize state manager.
        
        Args:
            state_file: Path to state file
        """
        self.state_file = Path(state_file)
        self._state: Dict[str, Any] = {}
        self._load_state()
    
    def _load_state(self) -> None:
        """Load state from file."""
        if self.state_file.exists():
            try:
                with open(self.state_file, 'r') as f:
                    self._state = json.load(f)
                logger.info(f"Loaded state from {self.state_file}")
            except Exception as e:
                logger.warning(f"Error loading state file: {e}")
                self._state = {}
        else:
            logger.info(f"No existing state file found at {self.state_file}")
            self._state = {}
    
    def _save_state(self) -> None:
        """Save state to file."""
        try:
            # Create directory if it doesn't exist
            self.state_file.parent.mkdir(parents=True, exist_ok=True)
            
            with open(self.state_file, 'w') as f:
                json.dump(self._state, f, indent=2, default=str)
            
            logger.info(f"Saved state to {self.state_file}")
            
        except Exception as e:
            logger.error(f"Error saving state file: {e}")
            raise
    
    def get_last_extracted_value(
        self,
        table_name: str,
        column: str
    ) -> Optional[Any]:
        """
        Get last extracted value for a table/column.
        
        Args:
            table_name: Name of the table
            column: Name of the column
            
        Returns:
            Last extracted value or None
        """
        key = f"{table_name}.{column}"
        value = self._state.get(key)
        
        logger.info(f"Last extracted value for {key}: {value}")
        return value
    
    def set_last_extracted_value(
        self,
        table_name: str,
        column: str,
        value: Any
    ) -> None:
        """
        Set last extracted value for a table/column.
        
        Args:
            table_name: Name of the table
            column: Name of the column
            value: Value to store
        """
        key = f"{table_name}.{column}"
        self._state[key] = value
        self._state[f"{key}_timestamp"] = datetime.now().isoformat()
        
        logger.info(f"Set last extracted value for {key}: {value}")
        self._save_state()
    
    def get_state(self) -> Dict[str, Any]:
        """
        Get complete state dictionary.
        
        Returns:
            State dictionary
        """
        return self._state.copy()
    
    def clear_state(self, table_name: Optional[str] = None) -> None:
        """
        Clear state for a specific table or all tables.
        
        Args:
            table_name: Name of the table (None to clear all)
        """
        if table_name:
            # Remove all keys for this table
            keys_to_remove = [
                key for key in self._state.keys()
                if key.startswith(f"{table_name}.")
            ]
            for key in keys_to_remove:
                del self._state[key]
            logger.info(f"Cleared state for table {table_name}")
        else:
            self._state = {}
            logger.info("Cleared all state")
        
        self._save_state()
