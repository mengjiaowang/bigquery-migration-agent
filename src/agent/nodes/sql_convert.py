"""Spark to BigQuery conversion node."""

import logging
import os
import time
from typing import Any

from src.agent.state import AgentState
from src.prompts.templates import SPARK_TO_BIGQUERY_PROMPT
from src.services.llm import get_llm
from src.services.sql_chunker import SQLChunker, ChunkedConverter
from src.services.table_mapping import get_table_mapping_service
from src.services.utils import get_content_text, accumulate_token_usage
from src.services.usage_logger import UsageLogger

logger = logging.getLogger(__name__)


from src.services.bigquery import BigQueryService

def _convert_single_chunk(spark_sql: str, table_mapping_info: str, table_ddls: str) -> tuple[str, dict]:
    """Convert a single SQL chunk using LLM.
    
    Args:
        spark_sql: The Spark SQL chunk to convert.
        table_mapping_info: Table mapping information for the prompt.
        table_ddls: DDLs for the target BigQuery tables.
        
    Returns:
        Tuple of (converted BigQuery SQL, token usage dict, model name).
    """
    llm = get_llm("sql_convert")
    # ChatGoogleGenerativeAI uses 'model', others might use 'model_name'
    model_name = getattr(llm, "model", getattr(llm, "model_name", "unknown"))
    
    prompt = SPARK_TO_BIGQUERY_PROMPT.format(
        spark_sql=spark_sql,
        table_mapping_info=table_mapping_info,
        table_ddls=table_ddls,
    )
    
    start_time = time.time()
    response = llm.invoke(prompt)
    end_time = time.time()
    latency_ms = int((end_time - start_time) * 1000)
    
    # Remove markdown code blocks
    bigquery_sql = get_content_text(response.content).strip()
    if bigquery_sql.startswith("```"):
        lines = bigquery_sql.split("\n")
        # Remove first line (```sql or ```) and last line (```)
        bigquery_sql = "\n".join(lines[1:-1]).strip()
    
    usage = response.response_metadata.get("token_usage") or response.usage_metadata
    
    return bigquery_sql, usage, model_name, latency_ms


def sql_convert(state: AgentState) -> dict[str, Any]:
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
    logger.info("[Node: sql_convert] Starting Spark to BigQuery conversion", extra={"type": "status", "step": "sql_convert", "status": "loading"})
    
    spark_sql = state['spark_sql']
    sql_length = len(spark_sql)
    sql_lines = spark_sql.count('\n')
    
    logger.info(f"[Node: sql_convert] Input SQL: {sql_length} chars, {sql_lines} lines")
    
    table_mapping_service = get_table_mapping_service()
    table_mapping = state.get("table_mapping", {})
    token_usage = state.get("token_usage", {})
    table_mapping_info = table_mapping_service.get_mapping_info_for_prompt(table_mapping)
    
    logger.info(f"[Node: sql_convert] Using {len(table_mapping)} table mappings from state")
    
    bq_service = BigQueryService()
    table_ddls_list = []
    
    relevant_bq_tables = set(table_mapping.values())
            
    logger.info(f"[Node: sql_convert] Identified {len(relevant_bq_tables)} relevant BigQuery tables")
    
    for bq_table in relevant_bq_tables:
        ddl = bq_service.get_table_ddl(bq_table)
        if ddl:
            table_ddls_list.append(f"-- DDL for {bq_table}:\n{ddl}")
            logger.info(f"[Node: sql_convert] Fetched DDL for {bq_table}")
        else:
            logger.warning(f"[Node: sql_convert] Could not fetch DDL for {bq_table}")
            
    table_ddls = "\n\n".join(table_ddls_list) if table_ddls_list else "No DDLs available."
    
    chunker = SQLChunker(spark_sql)
    use_chunking = chunker.should_chunk()
    
    chunking_mode = os.getenv("SQL_CHUNKING_MODE", "auto").lower()
    if chunking_mode == "disabled":
        use_chunking = False
        logger.info("[Node: sql_convert] SQL chunking disabled by configuration")
    elif chunking_mode == "always":
        use_chunking = True
        logger.info("[Node: sql_convert] SQL chunking forced by configuration")
    
    if use_chunking:
        logger.info("[Node: sql_convert] Using chunked conversion strategy")
        
        # Analyze and chunk
        chunks = chunker.analyze_and_chunk()
        
        if len(chunks) > 1:
            logger.info(f"[Node: sql_convert] Split into {len(chunks)} chunks")
            
            # Create converter with the single-chunk converter function
            # Create converter with the single-chunk converter function
            def converter_func(sql: str) -> str:
                sql_res, usage, model_name, latency_ms = _convert_single_chunk(sql, table_mapping_info, table_ddls)
                nonlocal token_usage
                token_usage = accumulate_token_usage(token_usage, usage, node_name="sql_convert", model_name=model_name)
                # TODO: storing latency for chunks? For now we just log it in usage logging if improved.
                return sql_res
            
            chunked_converter = ChunkedConverter(converter_func)
            bigquery_sql = chunked_converter.convert_chunks(chunks)
            
            logger.info("[Node: sql_convert] Chunked conversion completed")
        else:
            # Only one chunk, convert normally
            logger.info("[Node: sql_convert] SQL analyzed but no chunking needed")
            bigquery_sql = _convert_single_chunk(spark_sql, table_mapping_info, table_ddls)
    else:
        # Direct conversion without chunking
        logger.info("[Node: sql_convert] Using direct conversion (no chunking)")
        bigquery_sql, usage, model_name, latency_ms = _convert_single_chunk(spark_sql, table_mapping_info, table_ddls)
        token_usage = accumulate_token_usage(token_usage, usage, node_name="sql_convert", model_name=model_name)
    
    # Log usage to BQ
    usage_logger = UsageLogger()
    session_id = state.get("agent_session_id", "unknown")
    
    # We log the *incremental* usage here effectively?
    # Actually accumulate_token_usage tracks totals...
    # We should log the *current node's* usage.
    # We can fetch it from token_usage["nodes"]["sql_convert"]
    
    if token_usage and "nodes" in token_usage and "sql_convert" in token_usage["nodes"]:
        node_stats = token_usage["nodes"]["sql_convert"]
        # Note: if chunked, this might be aggregated usage of 1 call or multiple?
        # accumulate_token_usage aggregates into 'usage' key.
        usage_payload = node_stats.get("usage", {})
        model_used = node_stats.get("model", "unknown")
        
        usage_logger.log_usage(
            agent_session_id=session_id,
            node_name="sql_convert",
            model_name=model_used,
            usage=usage_payload,
            status="SUCCESS",
            latency_ms=locals().get('latency_ms') # Safe access if chunking used (latency_ms might be undefined or last chunk's)
        )

    bq_service.close()
    
    # Apply table name mapping
    bigquery_sql = table_mapping_service.replace_table_names(bigquery_sql, table_mapping)
    
    logger.info(f"[Node: sql_convert] Conversion completed ({len(bigquery_sql)} chars)", extra={"type": "status", "step": "sql_convert", "status": "success"})
    logger.debug(f"[Node: sql_convert] Final BigQuery SQL:\n{bigquery_sql}")
    
    return {
        "bigquery_sql": bigquery_sql,
        "table_ddls": table_ddls,
        "retry_count": 0,
        "conversion_history": [],
        "token_usage": token_usage,
    }
