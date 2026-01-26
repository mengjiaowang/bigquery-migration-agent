
"""
Tracing service for agent execution.
"""
import functools
import logging
from datetime import datetime
from typing import Callable, Any

from src.agent.state import AgentState
from src.services.usage_logger import UsageLogger

logger = logging.getLogger(__name__)

def trace_node(node_name: str, func: Callable[[AgentState], dict[str, Any]]):
    """
    Decorator/Wrapper to trace execution of a graph node.
    Logs input state, output state, status, and duration to BigQuery.
    """
    @functools.wraps(func)
    def wrapper(state: AgentState) -> dict[str, Any]:
        start_time = datetime.utcnow()
        status = "SUCCESS"
        error_message = None
        output = None
        
        # Capture input state (shallow copy or specific fields if needed?)
        # For now, we pass the state as is. The logger will serialize it.
        # We might want to filter large fields later if needed.
        input_state = dict(state)
        
        try:
            output = func(state)
            return output
        except Exception as e:
            status = "ERROR"
            error_message = str(e)
            raise e
        finally:
            end_time = datetime.utcnow()
            agent_session_id = state.get("agent_session_id", "unknown")
            
            # If output is part of state update, we log it.
            # If error, output is None.
            
            try:
                UsageLogger().log_trace(
                    agent_session_id=agent_session_id,
                    node_name=node_name,
                    status=status,
                    start_time=start_time,
                    end_time=end_time,
                    input_state=input_state,
                    output_state=output,
                    error_message=error_message
                )
            except Exception as log_err:
                logger.error(f"[Tracing] Failed to log trace for {node_name}: {log_err}")
                
    return wrapper
