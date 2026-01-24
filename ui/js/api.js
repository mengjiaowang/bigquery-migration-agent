
const API_URL = 'http://localhost:8000';
let eventSource = null;

export function connectLogStream(onMessage, onOpen, onError) {
    if (eventSource) {
        eventSource.close();
    }
    
    eventSource = new EventSource(`${API_URL}/logs/stream`);
    
    eventSource.onmessage = (event) => {
        try {
            const logEntry = JSON.parse(event.data);
            console.log("SSE Received:", logEntry); // Debug log
            if (onMessage) onMessage(logEntry);
        } catch (e) {
            console.error('Failed to parse log entry:', e);
        }
    };
    
    eventSource.onerror = (error) => {
        if (onError) onError(error);
        else console.error('SSE connection error:', error);
        
        // Reconnect after 3 seconds
        setTimeout(() => {
            if (eventSource.readyState === EventSource.CLOSED) {
                connectLogStream(onMessage, onOpen, onError);
            }
        }, 3000);
    };
    
    eventSource.onopen = () => {
        if (onOpen) onOpen();
        else console.log('SSE connection established');
    };
}

export async function convertSql(sparkSql) {
    const response = await fetch(`${API_URL}/convert`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify({ spark_sql: sparkSql }),
    });
    return await response.json();
}
