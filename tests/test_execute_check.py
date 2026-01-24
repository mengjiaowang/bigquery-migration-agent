
import pytest
import os
from unittest.mock import MagicMock, patch
from src.agent.nodes.execute import execute_node
from src.agent.state import AgentState

ALLOWED_PREFIX = "trip-htl-bi-dbprj.tool_results"
CUSTOM_PREFIX = "my_custom_project.my_dataset"

@pytest.fixture
def base_state():
    return AgentState(
        spark_sql="SELECT * FROM src",
        spark_valid=True,
        spark_error=None,
        bigquery_sql="",
        validation_success=True,
        validation_error=None,
        validation_mode="dry_run",
        retry_count=0,
        max_retries=3,
        conversion_history=[],
        execution_success=None,
        execution_result=None,
        execution_target_table=None,
        execution_error=None,
        data_verification_success=None,
        data_verification_result=None,
        data_verification_error=None,
    )

@patch("src.agent.nodes.execute.BigQueryService")
def test_execute_node_safety_check_read_only(mock_bq_service, base_state):
    # Mock BigQueryService
    mock_instance = mock_bq_service.return_value
    mock_instance.execute_query.return_value = MagicMock(success=True, result=[], target_table=None, error_message=None)
    
    state = base_state.copy()
    state["bigquery_sql"] = "SELECT * FROM `any_project.dataset.table`"
    
    result = execute_node(state)
    
    # Validation should pass, so execute_query should be called
    mock_instance.execute_query.assert_called_once()
    assert result["execution_success"] is True

@patch("src.agent.nodes.execute.BigQueryService")
def test_execute_node_safety_check_insert_allowed(mock_bq_service, base_state):
    mock_instance = mock_bq_service.return_value
    mock_instance.execute_query.return_value = MagicMock(success=True, result=[], target_table=f"{ALLOWED_PREFIX}.my_table", error_message=None)
    
    state = base_state.copy()
    state["bigquery_sql"] = f"INSERT INTO `{ALLOWED_PREFIX}.my_table` (id) VALUES (1)"
    
    result = execute_node(state)
    
    mock_instance.execute_query.assert_called_once()
    assert result["execution_success"] is True

@patch("src.agent.nodes.execute.BigQueryService")
def test_execute_node_safety_check_insert_forbidden(mock_bq_service, base_state):
    state = base_state.copy()
    state["bigquery_sql"] = "INSERT INTO `other_project.dataset.table` (id) VALUES (1)"
    
    result = execute_node(state)
    
    # Should block before calling execute_query
    mock_bq_service.return_value.execute_query.assert_not_called()
    assert result["execution_success"] is False
    assert "Modification not allowed" in result["execution_error"]

@patch("src.agent.nodes.execute.BigQueryService")
def test_execute_node_safety_check_delete_forbidden(mock_bq_service, base_state):
    state = base_state.copy()
    state["bigquery_sql"] = "DELETE FROM `other_project.dataset.table` WHERE true"
    
    result = execute_node(state)
    
    mock_bq_service.return_value.execute_query.assert_not_called()
    assert result["execution_success"] is False
    assert "Modification not allowed" in result["execution_error"]

@patch("src.agent.nodes.execute.BigQueryService")
def test_execute_node_safety_check_parse_error(mock_bq_service, base_state):
    state = base_state.copy()
    state["bigquery_sql"] = "INVALID SQL STATEMENT"
    
    result = execute_node(state)
    
    mock_bq_service.return_value.execute_query.assert_not_called()
    assert result["execution_success"] is False
    assert "Could not parse SQL" in result["execution_error"]

@patch("src.agent.nodes.execute.BigQueryService")
def test_execute_node_safety_check_custom_config(mock_bq_service, base_state):
    # Test with custom allowed dataset via env var
    with patch.dict(os.environ, {"DATA_VERIFICATION_ALLOWED_DATASET": CUSTOM_PREFIX}):
        mock_instance = mock_bq_service.return_value
        mock_instance.execute_query.return_value = MagicMock(success=True, result=[], target_table=f"{CUSTOM_PREFIX}.table", error_message=None)
        
        state = base_state.copy()
        state["bigquery_sql"] = f"INSERT INTO `{CUSTOM_PREFIX}.table` (id) VALUES (1)"
        
        result = execute_node(state)
        
        mock_instance.execute_query.assert_called_once()
        assert result["execution_success"] is True

@patch("src.agent.nodes.execute.BigQueryService")
def test_execute_node_safety_check_custom_config_forbidden(mock_bq_service, base_state):
    # Test that default prefix is NOT allowed when custom one is set
    with patch.dict(os.environ, {"DATA_VERIFICATION_ALLOWED_DATASET": CUSTOM_PREFIX}):
        state = base_state.copy()
        state["bigquery_sql"] = f"INSERT INTO `{ALLOWED_PREFIX}.table` (id) VALUES (1)"
        
        result = execute_node(state)
        
        mock_bq_service.return_value.execute_query.assert_not_called()
        assert result["execution_success"] is False
        assert "Modification not allowed" in result["execution_error"]
