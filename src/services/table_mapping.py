"""Table mapping service for Spark to BigQuery table name conversion."""

import csv
import logging
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class TableMapping:
    """Represents a Hive to BigQuery table mapping."""
    
    spark_table: str
    bigquery_table: str
    note: Optional[str] = None


class TableMappingService:
    """Service for managing Spark to BigQuery table name mappings."""
    
    _instance: Optional["TableMappingService"] = None
    _mappings: dict[str, str] = {}
    _loaded: bool = False
    
    def __new__(cls) -> "TableMappingService":
        """Singleton pattern to ensure mappings are loaded only once."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        """Initialize the service and load mappings if not already loaded."""
        if not self._loaded:
            self.load_mappings()
    
    def load_mappings(self, csv_path: Optional[str] = None) -> None:
        """Load table mappings from CSV file.
        
        Args:
            csv_path: Path to the CSV file. If not provided, uses default path.
        """
        if csv_path is None:
            # Default path: tests/data/table_mapping.csv relative to project root
            csv_path = os.getenv(
                "TABLE_MAPPING_CSV",
                str(Path(__file__).parent.parent.parent / "data" / "table_mapping.csv")
            )
        
        if not os.path.exists(csv_path):
            logger.warning(f"Table mapping file not found: {csv_path}")
            return
        
        try:
            with open(csv_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    hive_table = row.get("hive_table", "").strip()
                    bq_table = row.get("bigquery_table", "").strip()
                    
                    if hive_table and bq_table and bq_table != "无":
                        self._mappings[hive_table.lower()] = bq_table
                        
            logger.info(f"Loaded {len(self._mappings)} table mappings from {csv_path}")
            TableMappingService._loaded = True
            
        except Exception as e:
            logger.error(f"Failed to load table mappings: {e}")
    
    def get_bigquery_table(self, spark_table: str) -> Optional[str]:
        """Get the BigQuery table name for a Spark table.
        
        Args:
            spark_table: The Spark table name (e.g., "dim_hoteldb.dimhotel").
            
        Returns:
            The mapped BigQuery table name, or None if not found.
        """
        normalized = spark_table.lower().strip()
        return self._mappings.get(normalized)
    
    def get_all_mappings(self) -> dict[str, str]:
        """Get all table mappings.
        
        Returns:
            Dictionary of Spark table names to BigQuery table names.
        """
        return self._mappings.copy()
    
    def replace_table_names(self, sql: str, mappings: Optional[dict[str, str]] = None) -> str:
        """Replace all Spark table names in SQL with BigQuery table names.
        
        Args:
            sql: The SQL statement with Spark table names.
            mappings: Optional dictionary of mappings to use. If None, uses all loaded mappings.
            
        Returns:
            SQL statement with BigQuery table names.
        """
        result = sql
        
        # Sort mappings by length (longest first) to avoid partial replacements
        sorted_mappings = sorted(
            (mappings or self._mappings).items(),
            key=lambda x: len(x[0]),
            reverse=True
        )
        
        for spark_table, bq_table in sorted_mappings:
            # Match backtick-quoted table names OR unquoted table names with word boundaries
            patterns = [
                (rf'`{re.escape(spark_table)}`', f'`{bq_table}`'),
                (rf'(?i)(?<=\bFROM\s)({re.escape(spark_table)})(?=\s|$|,|\))', bq_table),
                (rf'(?i)(?<=\bJOIN\s)({re.escape(spark_table)})(?=\s|$|,|\))', bq_table),
                (rf'(?i)(?<=\bINTO\s)({re.escape(spark_table)})(?=\s|$|,|\))', bq_table),
                (rf'(?i)(?<=\bUPDATE\s)({re.escape(spark_table)})(?=\s|$|,|\))', bq_table),
                (rf'(?i)(?<=\bTABLE\s)({re.escape(spark_table)})(?=\s|$|,|\))', bq_table),
            ]
            
            for pattern, replacement in patterns:
                result = re.sub(pattern, replacement, result, flags=re.IGNORECASE)
        
        return result
    
    def get_mapping_info_for_prompt(self, mappings: Optional[dict[str, str]] = None) -> str:
        """Generate a formatted string of table mappings for use in prompts.
        
        Args:
            mappings: Optional dictionary of mappings to use. If None, uses all loaded mappings.
            
        Returns:
            Formatted string listing all table mappings.
        """
        target_mappings = mappings if mappings is not None else self._mappings
        
        if not target_mappings:
            return "No table mappings available."
        
        lines = ["## Table Name Mappings (Spark → BigQuery):"]
        for spark_table, bq_table in sorted(target_mappings.items()):
            lines.append(f"- {spark_table} → `{bq_table}`")
        
        return "\n".join(lines)


def get_table_mapping_service() -> TableMappingService:
    """Get the singleton table mapping service instance.
    
    Returns:
        TableMappingService instance.
    """
    return TableMappingService()
