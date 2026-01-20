"""Log streaming service for real-time log delivery to frontend."""

import asyncio
import logging
from collections import deque
from datetime import datetime
from typing import AsyncGenerator, Optional

# Global log buffer and subscribers
_log_buffer: deque = deque(maxlen=100)  # Keep last 100 logs
_subscribers: list[asyncio.Queue] = []


class StreamingLogHandler(logging.Handler):
    """Custom log handler that streams logs to subscribers."""
    
    def emit(self, record: logging.LogRecord) -> None:
        """Emit a log record to all subscribers."""
        try:
            log_entry = {
                "time": datetime.now().strftime("%H:%M:%S"),
                "level": record.levelname.lower(),
                "message": self.format(record),
                "name": record.name,
            }
            
            # Add to buffer
            _log_buffer.append(log_entry)
            
            # Send to all subscribers
            for queue in _subscribers:
                try:
                    queue.put_nowait(log_entry)
                except asyncio.QueueFull:
                    pass  # Skip if queue is full
                    
        except Exception:
            self.handleError(record)


def setup_log_streaming() -> None:
    """Set up the streaming log handler."""
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
