"""
Configuration classes for extraction strategies.
"""
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
from datetime import datetime
from enum import Enum


class ExtractionMode(Enum):
    """Extraction mode enumeration."""
    FULL = "full"
    INCREMENTAL_DATE = "incremental_date"


@dataclass
class TableConfig:
    """
    Configuration for table extraction.
    """
    table_name: str
    columns: Optional[List[str]] = None
    extraction_mode: ExtractionMode = ExtractionMode.FULL
    
    # Incremental date-based extraction
    date_column: Optional[str] = None
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    
    # Common options
    batch_size: int = 10000
    where_clause: Optional[str] = None
    order_by: Optional[str] = None
    
    # Schema
    schema: Optional[str] = None
    
    # Additional metadata
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        """Validate configuration after initialization."""
        if isinstance(self.extraction_mode, str):
            self.extraction_mode = ExtractionMode(self.extraction_mode)
        
        # Validate incremental date configuration
        if self.extraction_mode == ExtractionMode.INCREMENTAL_DATE:
            if not self.date_column:
                raise ValueError(
                    f"date_column is required for incremental date extraction "
                    f"on table {self.table_name}"
                )
    
    def get_full_table_name(self) -> str:
        """
        Get fully qualified table name.
        
        Returns:
            Fully qualified table name
        """
        if self.schema:
            return f"{self.schema}.{self.table_name}"
        return self.table_name


@dataclass
class ExtractionConfig:
    """
    Configuration for data extraction job.
    """
    tables: List[TableConfig]
    connection_id: str
    job_name: Optional[str] = None
    parallel_extraction: bool = False
    max_workers: int = 4
    
    # State management
    state_file: Optional[str] = None
    save_state: bool = True
    
    def __post_init__(self):
        """Validate configuration after initialization."""
        if not self.tables:
            raise ValueError("At least one table configuration is required")
        
        # Ensure all tables are TableConfig instances
        self.tables = [
            t if isinstance(t, TableConfig) else TableConfig(**t)
            for t in self.tables
        ]
