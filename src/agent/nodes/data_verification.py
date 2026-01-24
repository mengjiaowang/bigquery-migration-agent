"""Data verification node."""

import csv
import os
import logging
from typing import Any, Dict, Optional

from src.agent.state import AgentState
from src.services.bigquery import BigQueryService

# Configure logger
logger = logging.getLogger(__name__)


def load_verification_mapping() -> Dict[str, str]:
    """Load verification mapping from CSV file."""
    mapping = {}
    # Assuming the script runs from the project root
    csv_path = os.path.join(os.getcwd(), "tests/data/data_verify.csv")
    
    if not os.path.exists(csv_path):
        logger.warning(f"[Node: data_verification] Mapping file not found at {csv_path}")
        return mapping
        
    try:
        with open(csv_path, mode='r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get("new_table") and row.get("ground_truth_table"):
                    mapping[row["new_table"].strip()] = row["ground_truth_table"].strip()
    except Exception as e:
        logger.error(f"[Node: data_verification] Failed to load mapping file: {e}")
        
    return mapping

def data_verification(state: AgentState) -> dict[str, Any]:
    """Verify data in the target table after execution.
    
    Args:
        state: Current agent state.
        
    Returns:
        State update with verification results.
    """
    logger.info("============================================================")
    logger.info("[Node: data_verification] Starting Data Verification", extra={"type": "status", "step": "data_verification", "status": "loading"})
    
    target_table = state.get("execution_target_table")
    if not target_table:
        logger.warning("[Node: data_verification] No target table to verify. Skipping.")
        return {
            "data_verification_success": False,
            "data_verification_error": "No target table found.",
        }
        
    bq_service = BigQueryService()
    mapping = load_verification_mapping()
    ground_truth_table = mapping.get(target_table)
    verification_mode = os.environ.get("DATA_VERIFICATION_MODE", "row_count")
    
    try:
        if ground_truth_table:
            logger.info(f"[Node: data_verification] Found ground truth table: {ground_truth_table}. Mode: {verification_mode}")
            
            if verification_mode == "full_content":
                # Full content verification using EXCEPT DISTINCT
                # Check (T1 - T2) U (T2 - T1) is empty
                check_sql = f"""
                    SELECT count(*) as diff_count FROM (
                        (SELECT * FROM `{target_table}` EXCEPT DISTINCT SELECT * FROM `{ground_truth_table}`)
                        UNION ALL
                        (SELECT * FROM `{ground_truth_table}` EXCEPT DISTINCT SELECT * FROM `{target_table}`)
                    )
                """
                logger.debug(f"[Node: data_verification] Running full content check: {check_sql}")
                result = bq_service.execute_query(check_sql)
                
                if result.success and isinstance(result.result, list) and len(result.result) > 0:
                    diff_count = result.result[0].get("diff_count", 0)
                    if diff_count == 0:
                        logger.info(f"[Node: data_verification] ✓ Full content verification successful. Tables are identical.", extra={"type": "status", "step": "data_verification", "status": "success"})
                        return {
                            "data_verification_success": True,
                            "data_verification_result": {"mode": "full_content", "match": True},
                            "data_verification_error": None,
                        }
                    else:
                        msg = f"Tables differ by {diff_count} rows."
                        logger.error(f"[Node: data_verification] ✗ Full content verification failed: {msg}", extra={"type": "status", "step": "data_verification", "status": "error"})
                        return {
                            "data_verification_success": False,
                            "data_verification_result": {"mode": "full_content", "match": False, "diff_count": diff_count},
                            "data_verification_error": msg,
                        }
                else:
                    error_msg = result.error_message or "Failed to run full content check"
                    logger.error(f"[Node: data_verification] ✗ Verification failed: {error_msg}")
                    return {
                        "data_verification_success": False,
                        "data_verification_error": error_msg,
                    }

            else:
                # Row count verification (Default)
                check_sql = f"""
                    SELECT 
                        (SELECT count(*) FROM `{target_table}`) as target_cnt,
                        (SELECT count(*) FROM `{ground_truth_table}`) as gt_cnt
                """
                logger.debug(f"[Node: data_verification] Running row count comparison: {check_sql}")
                result = bq_service.execute_query(check_sql)
                
                if result.success and isinstance(result.result, list) and len(result.result) > 0:
                    target_cnt = result.result[0].get("target_cnt", 0)
                    gt_cnt = result.result[0].get("gt_cnt", 0)
                    
                    if target_cnt == gt_cnt:
                        logger.info(f"[Node: data_verification] ✓ Row count verification successful. Count: {target_cnt}", extra={"type": "status", "step": "data_verification", "status": "success"})
                        return {
                            "data_verification_success": True,
                            "data_verification_result": {"mode": "row_count", "match": True, "count": target_cnt},
                            "data_verification_error": None,
                        }
                    else:
                        msg = f"Row count mismatch. Target: {target_cnt}, Ground Truth: {gt_cnt}"
                        logger.error(f"[Node: data_verification] ✗ Row count verification failed: {msg}", extra={"type": "status", "step": "data_verification", "status": "error"})
                        return {
                            "data_verification_success": False,
                            "data_verification_result": {"mode": "row_count", "match": False, "target_count": target_cnt, "gt_count": gt_cnt},
                            "data_verification_error": msg,
                        }
                else:
                     error_msg = result.error_message or "Failed to run row count check"
                     logger.error(f"[Node: data_verification] ✗ Verification failed: {error_msg}")
                     return {
                         "data_verification_success": False,
                        "data_verification_error": error_msg,
                     }

        else:
            # Fallback to simple existence check if no mapping found
            logger.info("[Node: data_verification] No ground truth mapping found. Running simple existence check.")
            check_sql = f"SELECT count(*) as cnt FROM `{target_table}`"
            logger.debug(f"[Node: data_verification] Running check: {check_sql}")
            
            result = bq_service.execute_query(check_sql)
            
            if result.success and isinstance(result.result, list) and len(result.result) > 0:
                count = result.result[0].get("cnt", 0)
                logger.info(f"[Node: data_verification] ✓ Verification successful. Row count: {count}", extra={"type": "status", "step": "data_verification", "status": "success"})
                return {
                    "data_verification_success": True,
                    "data_verification_result": {"row_count": count},
                    "data_verification_error": None,
                }
            else:
                error_msg = result.error_message or "Failed to get row count"
                logger.error(f"[Node: data_verification] ✗ Verification failed: {error_msg}", extra={"type": "status", "step": "data_verification", "status": "error"})
                return {
                    "data_verification_success": False,
                    "data_verification_result": None,
                    "data_verification_error": error_msg,
                }
            
    except Exception as e:
        logger.error(f"[Node: data_verification] ✗ Verification error: {str(e)}")
        return {
            "data_verification_success": False,
            "data_verification_result": None,
            "data_verification_error": str(e),
        }
    finally:
        bq_service.close()
