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


@dataclass
class ExecutionResult:
    """Result of a BigQuery SQL execution."""
    
    success: bool
    result: Optional[list[dict] | str] = None
    target_table: Optional[str] = None
    error_message: Optional[str] = None
    job_id: Optional[str] = None


class BigQueryService:
    """Service for BigQuery operations including dry run validation."""
    
    def __init__(self, project_id: Optional[str] = None):
        """Initialize BigQuery service.
        
        Uses Application Default Credentials (ADC) for authentication.
        
        Args:
            project_id: GCP project ID. If not provided, uses GOOGLE_CLOUD_PROJECT 
                       env var or ADC project.
        """
        # Get project ID (param > env > ADC)
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
            # Explicitly set quota_project_id to project_id to ensure they match
            from google.api_core.client_options import ClientOptions
            client_options = ClientOptions(quota_project_id=self.project_id)

            self._client = bigquery.Client(project=self.project_id, client_options=client_options)
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
            

            return DryRunResult(
                success=True,
                total_bytes_processed=query_job.total_bytes_processed
            )
            
        except BadRequest as e:
            # Extract the error message from BigQuery
            error_message = str(e)
            
            # Enhance error message
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

    def execute_query(self, sql: str, limit: int = 100, labels: Optional[dict[str, str]] = None) -> ExecutionResult:
        """Execute BigQuery SQL and return results.
        
        Args:
            sql: The BigQuery SQL statement to execute.
            limit: Maximum number of rows to return.
            labels: Optional dictionary of labels to attach to the job.
            
        Returns:
            ExecutionResult with success status, data/message, and error message.
        """
        try:
            job_config = bigquery.QueryJobConfig(labels=labels) if labels else None
            query_job = self.client.query(sql, job_config=job_config)
            
            query_job.result()
            
            # Get destination table if available
            target_table = None
            if hasattr(query_job, 'destination') and query_job.destination:
                target_table = f"{query_job.destination.project}.{query_job.destination.dataset_id}.{query_job.destination.table_id}"
            
            # Check statement type
            if query_job.statement_type in ("INSERT", "UPDATE", "DELETE", "MERGE", "CREATE_TABLE", "CREATE_TABLE_AS_SELECT"):
                # DML/DDL (no rows)
                num_dml_affected_rows = query_job.num_dml_affected_rows
                message = f"Query executed successfully."
                if num_dml_affected_rows is not None:
                    message += f" Rows affected: {num_dml_affected_rows}"
                
                return ExecutionResult(
                    success=True,
                    result=message,
                    target_table=target_table,
                    job_id=query_job.job_id
                )
            else:
                # SELECT or other query returning rows
                rows = list(query_job.result(max_results=limit))
                result_data = [dict(row) for row in rows]
                
                return ExecutionResult(
                    success=True,
                    result=result_data,
                    target_table=target_table,
                    job_id=query_job.job_id
                )
                
        except Exception as e:
            return ExecutionResult(
                success=False,
                error_message=str(e)
            )

    def get_table_ddl(self, table_name: str) -> Optional[str]:
        """Get the DDL for a BigQuery table.
        
        Args:
            table_name: Full table name (project.dataset.table or dataset.table).
            
        Returns:
            The DDL string if found, None otherwise.
        """
        try:
            # Parse table name
            parts = table_name.split('.')
            if len(parts) == 3:
                project, dataset, table = parts
            elif len(parts) == 2:
                # If project not specified, use default project
                project = self.project_id
                dataset, table = parts
            else:
                return None
                
            project = project.strip('`')
            dataset = dataset.strip('`')
            table = table.strip('`')
            
            sql = f"SELECT ddl FROM `{project}.{dataset}.INFORMATION_SCHEMA.TABLES` WHERE LOWER(table_name) = LOWER('{table}')"
            
            # logger.info(f"Executing DDL fetch query: {sql}")
            result = self.execute_query(sql)
            
            if not result.success:
                # Log the error details
                print(f"Error fetching DDL for {table_name}: {result.error_message}")
                return None
                
            if result.result and isinstance(result.result, list) and len(result.result) > 0:
                ddl = result.result[0].get('ddl')
                if ddl:
                    return ddl
                else:
                    print(f"DDL column is empty for {table_name}")
                    return None
            
            print(f"No DDL found for {table_name} (Query returned 0 rows)")
            return None
            
        except Exception as e:
            # Log error but don't fail, just return None
            print(f"Exception fetching DDL for {table_name}: {e}")
            return None
    
    def close(self):
        """Close the BigQuery client connection."""
        if self._client is not None:
            self._client.close()
            self._client = None
