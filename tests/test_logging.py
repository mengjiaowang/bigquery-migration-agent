
import unittest
from unittest.mock import MagicMock, patch
import uuid
import os
import sys

# Mock BigQuery Client and other google modules before import
# We DO NOT mock 'google' top level to avoid breaking namespace packages like google.genai
mock_bq = MagicMock()
mock_cloud = MagicMock()
mock_exceptions = MagicMock()
mock_api_core = MagicMock()
mock_api_ex = MagicMock()
mock_auth = MagicMock()

# Only mock what we surely need to intercept or what might be missing
sys.modules["google.cloud"] = mock_cloud
sys.modules["google.cloud.bigquery"] = mock_bq
# sys.modules["google.cloud.exceptions"] = mock_exceptions # let's mock this carefully

# If google.cloud is a real package, we might masking it.
# We mainly want to mock BigQueryService usage of BigQuery Client.

# UsageLogger uses:
# from google.cloud import bigquery (which we mocked above)
# from google.api_core.exceptions import NotFound

try:
    import google.api_core.exceptions
except ImportError:
    sys.modules["google.api_core"] = mock_api_core
    sys.modules["google.api_core.exceptions"] = mock_api_ex
    mock_api_ex.NotFound = Exception

try:
    import google.cloud.exceptions
except ImportError:
    sys.modules["google.cloud.exceptions"] = mock_exceptions
    mock_exceptions.NotFound = Exception
    mock_exceptions.BadRequest = Exception

from src.services.usage_logger import UsageLogger
from src.services.tracing import trace_node

class TestLogging(unittest.TestCase):
    def setUp(self):
        os.environ["MODEL_USAGE_LOG_TABLE"] = "project.dataset.usage_log"
        os.environ["AGENT_TRACE_LOG_TABLE"] = "project.dataset.trace_log"
        os.environ["GOOGLE_CLOUD_PROJECT"] = "test-project"

    def test_usage_logging_with_run_id(self):
        logger = UsageLogger()
        mock_client = MagicMock()
        logger._bq_client = mock_client
        
        run_id = str(uuid.uuid4())
        logger.log_usage(
            agent_session_id="test_session",
            node_name="test_node",
            model_name="test_model",
            usage={"input_tokens": 10},
            run_id=run_id
        )
        
        # Check if insert_rows_json was called
        self.assertTrue(mock_client.insert_rows_json.called, "insert_rows_json not called")
        
        call_args = mock_client.insert_rows_json.call_args
        rows = call_args[0][1]
        last_row = rows[0]
        
        self.assertEqual(last_row.get("run_id"), run_id, f"run_id missing or incorrect. Got: {last_row.get('run_id')}, Expected: {run_id}")

    def test_tracing_with_run_id(self):
        with patch("src.services.tracing.UsageLogger") as MockLogger:
            mock_logger_instance = MockLogger.return_value
            
            def sample_node(state):
                # Verify run_id is injected into state
                self.assertIsNotNone(state.get("run_id"), "run_id missing in state")
                return {"output": "ok"}
                
            # Wrap the node
            traced_node = trace_node("test_trace_node", sample_node)
            
            state = {"agent_session_id": "trace_session"}
            traced_node(state)
            
            # Verify log_trace called with run_id
            self.assertTrue(mock_logger_instance.log_trace.called, "log_trace not called")
            
            call_kwargs = mock_logger_instance.log_trace.call_args[1]
            self.assertIsNotNone(call_kwargs.get("run_id"), "run_id missing in trace log call")

if __name__ == "__main__":
    unittest.main()
