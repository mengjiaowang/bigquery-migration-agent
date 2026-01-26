
"""
Service for logging LLM usage to BigQuery.
"""

import logging
import os
import uuid
from datetime import datetime
from typing import Any, Optional

from google.cloud import bigquery
from google.cloud.bigquery import SchemaField, Table, TimePartitioning
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField, Table, TimePartitioning
from google.api_core.exceptions import NotFound
import json

logger = logging.getLogger(__name__)


class UsageLogger:
    """Logs LLM usage details to BigQuery."""
    
    _instance = None
    _bq_client = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(UsageLogger, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        # Avoid re-initialization if already initialized
        if hasattr(self, "table_id"):
            return
            
            
        self.table_id = os.getenv("MODEL_USAGE_LOG_TABLE")
        self.trace_table_id = os.getenv("AGENT_TRACE_LOG_TABLE")
        
        if not self.table_id:
            logger.warning("[UsageLogger] MODEL_USAGE_LOG_TABLE not set. usage logging disabled.")
            
        if not self.trace_table_id:
            logger.warning("[UsageLogger] AGENT_TRACE_LOG_TABLE not set. trace logging disabled.")

        # Ensure tables exist (only runs once per process due to singleton)
        if self.table_id:
            try:
                self._ensure_table_exists()
            except Exception as e:
                logger.error(f"[UsageLogger] Failed to ensure table exists: {e}")

        if self.trace_table_id:
            try:
                self._ensure_trace_table_exists()
            except Exception as e:
                logger.error(f"[UsageLogger] Failed to ensure trace table exists: {e}")

    def _ensure_table_exists(self):
        """Check if table exists, create if not."""
        client = self.client
        if not client:
            return

        try:
            client.get_table(self.table_id)
            logger.info(f"[UsageLogger] Table {self.table_id} exists.")
        except Exception:
            # Assume table doesn't exist (or access denied, but we try to create)
            logger.info(f"[UsageLogger] Table {self.table_id} not found. Creating...")
            
            schema = [
                SchemaField("event_timestamp", "TIMESTAMP", mode="REQUIRED"),
                SchemaField("project_id", "STRING", mode="REQUIRED"),
                SchemaField("location", "STRING", mode="NULLABLE"),
                SchemaField("agent_session_id", "STRING", mode="NULLABLE"),
                SchemaField("node_name", "STRING", mode="REQUIRED"),
                SchemaField("model_name", "STRING", mode="REQUIRED"),
                SchemaField("input_tokens", "INTEGER", mode="NULLABLE"),
                SchemaField("output_tokens", "INTEGER", mode="NULLABLE"),
                SchemaField("cached_content_tokens", "INTEGER", mode="NULLABLE"),
                SchemaField("total_tokens", "INTEGER", mode="NULLABLE"),
                SchemaField("status", "STRING", mode="REQUIRED"),
                SchemaField("error_message", "STRING", mode="NULLABLE"),
                SchemaField("latency_ms", "INTEGER", mode="NULLABLE"),
            ]
            
            table = Table(self.table_id, schema=schema)
            table.time_partitioning = TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="event_timestamp"
            )
            table.clustering_fields = ["agent_session_id", "node_name"]
            
            client.create_table(table, exists_ok=True)
            logger.info(f"[UsageLogger] Table {self.table_id} created/verified.")

    def _ensure_trace_table_exists(self):
        """Check if trace table exists, create if not."""
        client = self.client
        if not client:
            return

        try:
            client.get_table(self.trace_table_id)
            logger.info(f"[UsageLogger] Table {self.trace_table_id} exists.")
        except Exception:
            logger.info(f"[UsageLogger] Table {self.trace_table_id} not found. Creating...")
            
            schema = [
                SchemaField("event_timestamp", "TIMESTAMP", mode="REQUIRED"),
                SchemaField("project_id", "STRING", mode="REQUIRED"),
                SchemaField("agent_session_id", "STRING", mode="REQUIRED"),
                SchemaField("node_name", "STRING", mode="REQUIRED"),
                SchemaField("execution_status", "STRING", mode="REQUIRED"),
                SchemaField("start_time", "TIMESTAMP", mode="REQUIRED"),
                SchemaField("end_time", "TIMESTAMP", mode="REQUIRED"),
                SchemaField("duration_ms", "INTEGER", mode="REQUIRED"),
                SchemaField("input_state", "JSON", mode="NULLABLE"),
                SchemaField("output_state", "JSON", mode="NULLABLE"),
                SchemaField("error_message", "STRING", mode="NULLABLE"),
            ]
            
            table = Table(self.trace_table_id, schema=schema)
            table.time_partitioning = TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="event_timestamp"
            )
            table.clustering_fields = ["agent_session_id", "node_name"]
            
            client.create_table(table, exists_ok=True)
            logger.info(f"[UsageLogger] Table {self.trace_table_id} created/verified.")
    
    @property
    def client(self) -> Optional[bigquery.Client]:
        """Lazy load BigQuery client."""
        if self._bq_client is None:
            try:
                self._bq_client = bigquery.Client()
            except Exception as e:
                logger.error(f"[UsageLogger] Failed to initialize BigQuery client: {e}")
                return None
        return self._bq_client

    def log_usage(
        self,
        agent_session_id: str,
        node_name: str,
        model_name: str,
        usage: dict[str, Any],
        status: str = "SUCCESS",
        error_message: Optional[str] = None,
        latency_ms: Optional[int] = None,
    ):
        """
        Log a model call event to BigQuery.
        
        Args:
            agent_session_id: Unique session ID.
            node_name: Name of the node executing the call.
            model_name: Name of the model used.
            usage: Dictionary with 'input_tokens', 'output_tokens', etc.
            status: 'SUCCESS' or 'ERROR'.
            error_message: Optional error details.
            latency_ms: Optional duration in milliseconds.
        """
        if not self.table_id:
            return

        bq_client = self.client
        if not bq_client:
            return

        try:
            # Extract standard usage fields (handling different potential provider formats)
            # Default to 0 IF usage is None/Empty, but usually it should be provided.
            usage = usage or {}
            
            input_tokens = usage.get("input_tokens", 0)
            output_tokens = usage.get("output_tokens", 0)
            total_tokens = usage.get("total_tokens", 0)
            
            # Cached tokens logic
            cached_tokens = 0
            # Check different locations for cache info
            if "cached_content_tokens" in usage:
                cached_tokens = usage["cached_content_tokens"]
            elif "input_token_details" in usage:
                details = usage["input_token_details"]
                if isinstance(details, dict):
                    cached_tokens = details.get("cached_content_tokens", 0)
                    cached_tokens += details.get("cache_read", 0)
            
            # Cost Calculation
            # Removed as per requirement to handle in BQ
            estimated_cost = None
            
            project_id = os.getenv("GOOGLE_CLOUD_PROJECT")
            location = os.getenv("GOOGLE_CLOUD_LOCATION")
            
            row = {
                "event_timestamp": datetime.utcnow().isoformat(),
                "project_id": project_id,
                "location": location,
                "agent_session_id": agent_session_id,
                "node_name": node_name,
                "model_name": model_name,
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
                "cached_content_tokens": cached_tokens,
                "total_tokens": total_tokens,
                "status": status,
                "error_message": error_message,
                "latency_ms": latency_ms,
            }
            
            # Insert logic with retry for "Table not found"
            try:
                errors = bq_client.insert_rows_json(self.table_id, [row])
                if errors:
                    logger.error(f"[UsageLogger] Failed to insert rows: {errors}")
                else:
                    logger.debug(f"[UsageLogger] Logged usage for {node_name}")
            except NotFound:
                # Table might not exist or propagation delay.
                logger.warning(f"[UsageLogger] Table {self.table_id} not found during insert. Re-verifying...")
                self._ensure_table_exists()
                # Retry once
                errors = bq_client.insert_rows_json(self.table_id, [row])
                if errors:
                    logger.error(f"[UsageLogger] Failed to insert rows after retry: {errors}")
                else:
                    logger.debug(f"[UsageLogger] Logged usage for {node_name} (after retry)")
                
        except Exception as e:
            logger.error(f"[UsageLogger] Unexpected error logging usage: {e}")

    def log_error(
        self,
        agent_session_id: str,
        node_name: str,
        model_name: str,
        error_message: str,
    ):
        """Helper to log failed calls."""
        self.log_usage(
            agent_session_id=agent_session_id,
            node_name=node_name,
            model_name=model_name,
            usage={},
            status="ERROR",
            error_message=error_message
        )

    def log_trace(
        self,
        agent_session_id: str,
        node_name: str,
        status: str,
        start_time: datetime,
        end_time: datetime,
        input_state: Optional[dict[str, Any]] = None,
        output_state: Optional[dict[str, Any]] = None,
        error_message: Optional[str] = None,
    ):
        """Log agent node execution trace."""
        if not self.trace_table_id:
            return

        bq_client = self.client
        if not bq_client:
            return

        try:
            duration_ms = int((end_time - start_time).total_seconds() * 1000)
            project_id = os.getenv("GOOGLE_CLOUD_PROJECT")

            # Sanitize state (avoid overly large objects if needed, but BQ JSON handles a lot)
            # We might want to remove large text fields if they exceed limits, but for BQ mostly OK.
            
            row = {
                "event_timestamp": end_time.isoformat(),
                "project_id": project_id,
                "agent_session_id": agent_session_id,
                "node_name": node_name,
                "execution_status": status,
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "duration_ms": duration_ms,
                "input_state": json.dumps(input_state, default=str) if input_state else None,
                "output_state": json.dumps(output_state, default=str) if output_state else None,
                "error_message": error_message,
            }

            try:
                errors = bq_client.insert_rows_json(self.trace_table_id, [row])
                if errors:
                    logger.error(f"[UsageLogger] Failed to insert trace rows: {errors}")
            except NotFound:
                logger.warning(f"[UsageLogger] Trace table {self.trace_table_id} not found. Re-verifying...")
                self._ensure_trace_table_exists()
                bq_client.insert_rows_json(self.trace_table_id, [row])
                
        except Exception as e:
            logger.error(f"[UsageLogger] Unexpected error logging trace: {e}")
