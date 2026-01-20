"""BigQuery Dry Run service for SQL validation."""

import os
from dataclasses import dataclass
from typing import Optional

import google.auth
from google.cloud import bigquery
from google.cloud.exceptions import BadRequest


@dataclass
class DryRunResult:
    """Result of a BigQuery dry run validation."""
    
    success: bool
    error_message: Optional[str] = None
    total_bytes_processed: Optional[int] = None


class BigQueryService:
    """Service for BigQuery operations including dry run validation."""
    
    def __init__(self, project_id: Optional[str] = None):
        """Initialize BigQuery service.
        
        Uses Application Default Credentials (ADC) for authentication.
        
        Args:
            project_id: GCP project ID. If not provided, uses GOOGLE_CLOUD_PROJECT 
                       env var or ADC project.
        """
        # Get project ID from: parameter > env var > ADC
        if project_id:
            self.project_id = project_id
        else:
            self.project_id = os.getenv("GOOGLE_CLOUD_PROJECT")
            if not self.project_id:
                # Try to get from ADC
                _, auth_project_id = google.auth.default()
                self.project_id = auth_project_id
        
        if not self.project_id:
            raise ValueError(
                "Project ID is required. Set GOOGLE_CLOUD_PROJECT environment variable, "
                "pass project_id parameter, or configure default project in gcloud."
            )
        self._client: Optional[bigquery.Client] = None
    
    @property
    def client(self) -> bigquery.Client:
        """Lazy initialization of BigQuery client."""
        if self._client is None:
            self._client = bigquery.Client(project=self.project_id)
        return self._client
    
    def dry_run(self, sql: str) -> DryRunResult:
        """Perform a dry run validation of BigQuery SQL.
        
        Args:
            sql: The BigQuery SQL statement to validate.
            
        Returns:
            DryRunResult with success status and error message if any.
        """
        job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
        
        try:
            query_job = self.client.query(sql, job_config=job_config)
            
            # If we get here, the query is valid
            return DryRunResult(
                success=True,
                total_bytes_processed=query_job.total_bytes_processed
            )
            
        except BadRequest as e:
            # Extract the error message from BigQuery
            error_message = str(e)
            
            # Try to get more detailed error info
            if hasattr(e, 'errors') and e.errors:
                error_details = []
                for error in e.errors:
                    if isinstance(error, dict):
                        msg = error.get('message', '')
                        location = error.get('location', '')
                        if location:
                            error_details.append(f"{msg} (at {location})")
                        else:
                            error_details.append(msg)
                if error_details:
                    error_message = "; ".join(error_details)
            
            return DryRunResult(
                success=False,
                error_message=error_message
            )
            
        except Exception as e:
            # Handle other exceptions (network errors, auth errors, etc.)
            return DryRunResult(
                success=False,
                error_message=f"Unexpected error: {str(e)}"
            )
    
    def close(self):
        """Close the BigQuery client connection."""
        if self._client is not None:
            self._client.close()
            self._client = None
