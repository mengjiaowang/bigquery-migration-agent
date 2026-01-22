"""Spark to BigQuery conversion node."""

import logging
import os
from typing import Any

from src.agent.state import AgentState
from src.prompts.templates import SPARK_TO_BIGQUERY_PROMPT
from src.services.llm import get_llm
from src.services.sql_chunker import SQLChunker, ChunkedConverter
from src.services.table_mapping import get_table_mapping_service
from src.services.utils import get_content_text

# Configure logger
logger = logging.getLogger(__name__)


from src.services.bigquery import BigQueryService

def _convert_single_chunk(spark_sql: str, table_mapping_info: str, table_ddls: str) -> str:
    """Convert a single SQL chunk using LLM.
    
    Args:
        spark_sql: The Spark SQL chunk to convert.
        table_mapping_info: Table mapping information for the prompt.
        table_ddls: DDLs for the target BigQuery tables.
        
    Returns:
        The converted BigQuery SQL.
    """
    llm = get_llm()
    
    prompt = SPARK_TO_BIGQUERY_PROMPT.format(
        spark_sql=spark_sql,
        table_mapping_info=table_mapping_info,
        table_ddls=table_ddls,
    )
    response = llm.invoke(prompt)
    
    # Clean up response - remove markdown code blocks if present
    bigquery_sql = get_content_text(response.content).strip()
    if bigquery_sql.startswith("```"):
        lines = bigquery_sql.split("\n")
        # Remove first line (```sql or ```) and last line (```)
        bigquery_sql = "\n".join(lines[1:-1]).strip()
    
    return bigquery_sql


def convert_node(state: AgentState) -> dict[str, Any]:
    """Convert Spark SQL to BigQuery SQL.
    
    For long SQL statements, this function will:
    1. Analyze the SQL structure (CTE, UNION, INSERT...SELECT, etc.)
    2. Split into manageable chunks
    3. Convert each chunk separately
    4. Merge the results
    
    Args:
        state: Current agent state containing hive_sql.
        
    Returns:
        Updated state with bigquery_sql.
    """
    logger.info("=" * 60)
    logger.info("[Node: convert] Starting Spark to BigQuery conversion", extra={"type": "status", "step": "convert", "status": "loading"})
    
    spark_sql = state['spark_sql']
    sql_length = len(spark_sql)
    sql_lines = spark_sql.count('\n')
    
    logger.info(f"[Node: convert] Input SQL: {sql_length} chars, {sql_lines} lines")
    
    # Get table mapping information
    table_mapping_service = get_table_mapping_service()
    table_mapping_info = table_mapping_service.get_mapping_info_for_prompt()
    all_mappings = table_mapping_service.get_all_mappings()
    
    logger.info(f"[Node: convert] Using {len(all_mappings)} table mappings")
    
    # Fetch DDLs for relevant tables
    bq_service = BigQueryService()
    table_ddls_list = []
    
    # Optimization: Only fetch DDLs for tables that appear in the SQL
    # Case insensitive check
    spark_sql_lower = spark_sql.lower()
    
    relevant_bq_tables = set()
    for hive_table, bq_table in all_mappings.items():
        # Simple check if hive table name exists in SQL (could be false positive but safer than complex parsing)
        if hive_table in spark_sql_lower:
            relevant_bq_tables.add(bq_table)
            
    logger.info(f"[Node: convert] Identified {len(relevant_bq_tables)} potentially relevant BigQuery tables")
    
    for bq_table in relevant_bq_tables:
        ddl = bq_service.get_table_ddl(bq_table)
        if ddl:
            table_ddls_list.append(f"-- DDL for {bq_table}:\n{ddl}")
            logger.info(f"[Node: convert] Fetched DDL for {bq_table}")
        else:
            logger.warning(f"[Node: convert] Could not fetch DDL for {bq_table}")
            
    table_ddls = "\n\n".join(table_ddls_list) if table_ddls_list else "No DDLs available."
    
    # Check if chunking is needed
    chunker = SQLChunker(spark_sql)
    use_chunking = chunker.should_chunk()
    
    # Also check environment variable to force/disable chunking
    chunking_mode = os.getenv("SQL_CHUNKING_MODE", "auto").lower()
    if chunking_mode == "disabled":
        use_chunking = False
        logger.info("[Node: convert] SQL chunking disabled by configuration")
    elif chunking_mode == "always":
        use_chunking = True
        logger.info("[Node: convert] SQL chunking forced by configuration")
    
    if use_chunking:
        logger.info("[Node: convert] Using chunked conversion strategy")
        
        # Analyze and chunk
        chunks = chunker.analyze_and_chunk()
        
        if len(chunks) > 1:
            logger.info(f"[Node: convert] Split into {len(chunks)} chunks")
            
            # Create converter with the single-chunk converter function
            def converter_func(sql: str) -> str:
                return _convert_single_chunk(sql, table_mapping_info, table_ddls)
            
            chunked_converter = ChunkedConverter(converter_func)
            bigquery_sql = chunked_converter.convert_chunks(chunks)
            
            logger.info("[Node: convert] Chunked conversion completed")
        else:
            # Only one chunk, convert normally
            logger.info("[Node: convert] SQL analyzed but no chunking needed")
            bigquery_sql = _convert_single_chunk(spark_sql, table_mapping_info, table_ddls)
    else:
        # Direct conversion without chunking
        logger.info("[Node: convert] Using direct conversion (no chunking)")
        bigquery_sql = _convert_single_chunk(spark_sql, table_mapping_info, table_ddls)
    
    bq_service.close()
    
    # Apply table name replacement as a safety net
    # (in case the LLM didn't apply all mappings correctly)
    bigquery_sql = table_mapping_service.replace_table_names(bigquery_sql)
    
    logger.info(f"[Node: convert] Final BigQuery SQL ({len(bigquery_sql)} chars):\n{bigquery_sql}", extra={"type": "status", "step": "convert", "status": "success"})
    
    return {
        "bigquery_sql": bigquery_sql,
        "retry_count": 0,
        "conversion_history": [],
    }
