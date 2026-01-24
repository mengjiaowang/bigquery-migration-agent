"""Log streaming service for real-time log delivery to frontend."""

import asyncio
import logging
from collections import deque
from datetime import datetime
from typing import AsyncGenerator, Optional

_log_buffer: deque = deque(maxlen=100)
_subscribers: list[asyncio.Queue] = []
_loop: Optional[asyncio.AbstractEventLoop] = None


class StreamingLogHandler(logging.Handler):
    """Custom log handler that streams logs to subscribers."""
    
    def emit(self, record: logging.LogRecord) -> None:
        """Emit a log record to all subscribers."""
        try:
            extra = record.__dict__
            
            log_entry = {
                "time": datetime.now().strftime("%H:%M:%S"),
                "level": record.levelname.lower(),
                "message": self.format(record),
                "name": record.name,
                "type": extra.get("type", "log"),  # default to "log"
            }
            

            if log_entry["type"] == "status":
                if "step" in extra:
                    log_entry["step"] = extra["step"]
                if "status" in extra:
                    log_entry["status"] = extra["status"]
                if "attempt" in extra:
                    log_entry["attempt"] = extra["attempt"]
                if "data" in extra:
                    log_entry["data"] = extra["data"]
            
            _log_buffer.append(log_entry)
            
            # Send to all subscribers
            for queue in _subscribers:
                try:
                    # Thread-safe put
                    if _loop and _loop.is_running():
                        _loop.call_soon_threadsafe(queue.put_nowait, log_entry)
                    else:
                        # Fallback for sync context or missing loop
                        try:
                            queue.put_nowait(log_entry)
                        except asyncio.QueueFull:
                            pass
                except Exception as e:
                    import sys
                    print(f"Error in emit to subscriber: {e}", file=sys.stderr)
                    pass
                    
        except Exception as e:
            import sys
            print(f"Error in emit: {e}", file=sys.stderr)
            self.handleError(record)


def init_log_loop() -> None:
    """Initialize the global event loop reference for the log handler."""
    global _loop
    try:
        _loop = asyncio.get_running_loop()
    except RuntimeError:
        pass


def setup_log_streaming() -> None:
    """Set up the streaming log handler."""
    global _loop
    try:
        _loop = asyncio.get_running_loop()
    except RuntimeError:
        pass  # No running loop yet

    # Create handler with formatter
    handler = StreamingLogHandler()
    handler.setFormatter(logging.Formatter("%(message)s"))
    handler.setLevel(logging.INFO)
    
    # Add to root logger
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    



async def subscribe_logs() -> AsyncGenerator[dict, None]:
    """Subscribe to log stream.
    
    Yields:
        Log entries as they are emitted.
    """
    # Ensure loop is captured
    global _loop
    if _loop is None:
        try:
            _loop = asyncio.get_running_loop()
        except RuntimeError:
            pass

    queue: asyncio.Queue = asyncio.Queue(maxsize=50)
    _subscribers.append(queue)
    
    try:
        # First, send recent logs from buffer
        for log_entry in list(_log_buffer):
            yield log_entry
        
        # Then stream new logs
        while True:
            try:
                # Use timeout to allow periodic checks and graceful shutdown
                log_entry = await asyncio.wait_for(queue.get(), timeout=30.0)
                yield log_entry
            except asyncio.TimeoutError:
                # Send heartbeat to keep connection alive
                yield {"type": "heartbeat", "time": datetime.now().strftime("%H:%M:%S")}
            except asyncio.CancelledError:
                # Gracefully handle cancellation
                break
            
    except asyncio.CancelledError:
        # Handle cancellation at the outer level
        pass
    finally:
        if queue in _subscribers:
            _subscribers.remove(queue)


def get_recent_logs(count: int = 50) -> list[dict]:
    """Get recent logs from buffer.
    
    Args:
        count: Maximum number of logs to return.
        
    Returns:
        List of recent log entries.
    """
    return list(_log_buffer)[-count:]
