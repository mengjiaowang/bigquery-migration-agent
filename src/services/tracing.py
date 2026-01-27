
"""
Tracing service for agent execution.
"""
import functools
import logging
from datetime import datetime
from typing import Callable, Any, TYPE_CHECKING

if TYPE_CHECKING:
    from src.agent.state import AgentState

from src.services.usage_logger import UsageLogger

logger = logging.getLogger(__name__)

def trace_node(node_name: str, func: Callable[["AgentState"], dict[str, Any]]):
    """
    Decorator/Wrapper to trace execution of a graph node.
    Logs input state, output state, status, and duration to BigQuery.
    """
    @functools.wraps(func)
    def wrapper(state: "AgentState") -> dict[str, Any]:
        start_time = datetime.utcnow()
        status = "SUCCESS"
        error_message = None
        output = None
        
        # Generate a unique run_id for this execution
        import uuid
        run_id = str(uuid.uuid4())
        
        # Capture input state
        # We inject run_id into state so nodes can use it for their own logging
        # state is a TypeDict, but at runtime it behaves like a dict.
        # We might need to ensure the downstream nodes know about this key if they typed strictly,
        # but usually AgentState is flexible or we modify the COPY of state we pass if needed.
        # However, for LangGraph, we typically return updates. 
        # Here we just want the NODE function to have access to it.
        # We can MUTATE the state passed in because it's usually a fresh dict for the step in some frameworks,
        # or we rely on the fact that we are just reading it.
        # LangGraph passes a state object.
        
        # We'll temporarily add run_id to state for the duration of the func call
        # OR we rely on the fact that existing state is a dict.
        state["run_id"] = run_id
        
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
            
            try:
                UsageLogger().log_trace(
                    agent_session_id=agent_session_id,
                    node_name=node_name,
                    status=status,
                    start_time=start_time,
                    end_time=end_time,
                    input_state=input_state,
                    output_state=output,
                    error_message=error_message,
                    run_id=run_id
                )
            except Exception as log_err:
                logger.error(f"[Tracing] Failed to log trace for {node_name}: {log_err}")
                
    return wrapper
