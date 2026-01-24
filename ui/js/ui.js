// Initialize Mermaid
mermaid.initialize({ 
    startOnLoad: false,
    theme: 'dark',
    securityLevel: 'loose',
    flowchart: {
        curve: 'basis'
    }
});

// Graph Definition (Source of Truth)
const graphDefinition = `flowchart LR
    Start((Start)) --> Spark[Spark Check]
    Spark -->|Valid| Convert[SQL Convert]
    Spark -->|Invalid| End((End))
    Convert --> DryRun[Dry Run]
    DryRun -->|Success| LLM[LLM Check]
    DryRun -->|Fail| Fix[Auto Fix]
    Fix --> DryRun
    LLM -->|Pass| Execute[Execute]
    LLM -->|Fail| Fix
    Execute -->|Success| Data[Data Verify]
    Execute -->|Fail| Fix
    Data --> End`;

// Render Graph initially
document.addEventListener('DOMContentLoaded', async () => {
    const element = document.getElementById('workflowGraph');
    if (element) {
        element.innerHTML = graphDefinition;
        await mermaid.run({
            nodes: [element]
        });
    }
});

// UI Helper Functions

export function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

export function addLog(level, message, fromServer = false) {
    const infoPanel = document.getElementById('infoPanel');
    const time = new Date().toLocaleTimeString('en-US', { hour12: false });
    const line = document.createElement('div');
    line.className = 'info-line';
    
    // const source = fromServer ? '服务' : '前端'; // source not used in original layout explicitly but kept structure
    line.innerHTML = `
        <span class="info-time">[${time}]</span>
        <span class="info-level ${level}">${level.toUpperCase()}</span>
        <span class="info-message">${escapeHtml(message)}</span>
    `;
    infoPanel.appendChild(line);
    infoPanel.scrollTop = infoPanel.scrollHeight;
}

export function addServerLog(logEntry) {
    const infoPanel = document.getElementById('infoPanel');
    const line = document.createElement('div');
    line.className = 'info-line';
    
    const level = logEntry.level || 'info';
    const levelClass = level === 'warning' ? 'warning' : 
                      level === 'error' ? 'error' : 
                      level === 'success' ? 'success' : 'info';
    
    line.innerHTML = `
        <span class="info-time">[${logEntry.time}]</span>
        <span class="info-level ${levelClass}">${level.toUpperCase()}</span>
        <span class="info-message">${escapeHtml(logEntry.message)}</span>
    `;
    infoPanel.appendChild(line);
    infoPanel.scrollTop = infoPanel.scrollHeight;
}

export function clearInput() {
    document.getElementById('sparkSql').value = '';
    addLog('info', 'Input cleared');
}

export function clearLogs() {
    document.getElementById('infoPanel').innerHTML = '';
    addLog('info', 'Logs cleared');
}

export function updateStatus(status, message) {
    const dot = document.getElementById('statusDot');
    const text = document.getElementById('statusText');
    
    dot.style.background = status === 'success' ? 'var(--accent-green)' :
                           status === 'error' ? 'var(--accent-red)' :
                           status === 'loading' ? 'var(--accent-yellow)' :
                           'var(--accent-green)';
    text.textContent = message;
}

function setNodeStatus(nodeId, status) {
    // Determine class
    const className = status; // running, success, error
    
    // Mermaid renders nodes with IDs like "flowchart-Spark-..." but format varies by version
    // Use substring match to be safer
    const selector = `g.node[id*="${nodeId}"]`; 
    const node = document.querySelector(selector);
    
    if (node) {
        // Remove existing status classes
        node.classList.remove('running', 'success', 'error', 'pending');
        node.classList.add(className);
    }
}

export function handleStatusUpdate(event) {
    const { step, status, attempt } = event;
    
    // Map step to Mermaid Node ID
    const stepMap = {
        'spark_sql_validate': 'Spark',
        'sql_convert': 'Convert',
        'llm_sql_check': 'LLM',
        'bigquery_dry_run': 'DryRun',
        'bigquery_error_fix': 'Fix',
        'bigquery_sql_execute': 'Execute',
        'data_verification': 'Data'
    };
    
    const nodeId = stepMap[step];
    if (nodeId) {
        let nodeStatus = 'pending';
        if (status === 'loading') nodeStatus = 'running';
        if (status === 'success') nodeStatus = 'success';
        if (status === 'error') nodeStatus = 'error';
        
        setNodeStatus(nodeId, nodeStatus);
    }
    
    // Also update global status text
    if (status === 'loading') {
        let msg = `Executing: ${step}...`;
        if (attempt) msg += ` (Attempt ${attempt})`;
        updateStatus('loading', msg);
    } else if (status === 'completed') {
        updateStatus('success', 'Conversion Completed');
        setNodeStatus('End', 'success');
    }
}

export function updateResultCards(result) {
    // Update graph nodes based on final result
    if (result.spark_valid) setNodeStatus('Spark', 'success');
    else setNodeStatus('Spark', 'error');
    
    if (result.bigquery_sql) setNodeStatus('Convert', 'success');
    
    if (result.validation_success) {
        setNodeStatus('DryRun', 'success');
    } else if (result.spark_valid) { 
        // Only mark error if we got here
        if (result.validation_mode === 'dry_run') setNodeStatus('DryRun', 'error');
    }
    
    if (result.llm_check_success) setNodeStatus('LLM', 'success');
    else if (result.llm_check_success === false) setNodeStatus('LLM', 'error');

    if (result.execution_success) setNodeStatus('Execute', 'success');
    else if (result.execution_success === false) setNodeStatus('Execute', 'error');
    
    if (result.data_verification_success) setNodeStatus('Data', 'success');
    else if (result.data_verification_success === false) setNodeStatus('Data', 'error');
    
    if (result.retry_count > 0) {
        setNodeStatus('Fix', 'success'); // Indicate we used fix
    }
}

export function resetStatusCards() {
    // Reset all nodes
    const nodes = ['Spark', 'Convert', 'DryRun', 'LLM', 'Fix', 'Execute', 'Data', 'End'];
    nodes.forEach(id => {
        const selector = `g.node[id^="flowchart-${id}-"]`; 
        const node = document.querySelector(selector);
        if (node) {
            node.classList.remove('running', 'success', 'error');
            node.classList.add('pending');
        }
    });
    
    // Clear SQL output
    const bqOutput = document.getElementById('bqOutput');
    if (bqOutput) {
    if (bqOutput) {
        bqOutput.textContent = 'Conversion results will appear here';
    }
    }
}

export function loadSample(sampleSql) {
    document.getElementById('sparkSql').value = sampleSql;
    addLog('info', 'Sample SQL loaded');
}

export function copyOutput() {
    const output = document.getElementById('bqOutput').textContent;
    if (!output || output.trim() === 'Conversion results will appear here') {
        addLog('warning', 'Nothing to copy');
        return;
    }
    
    navigator.clipboard.writeText(output).then(() => {
        const btn = document.getElementById('copyBtn');
        const originalText = btn.textContent;
        btn.textContent = 'Copied!';
        btn.classList.add('copied');
        
        setTimeout(() => {
            btn.textContent = originalText;
            btn.classList.remove('copied');
        }, 2000);
        addLog('info', 'Copied to clipboard');
    }).catch(err => {
        console.error('Copy failed:', err);
        addLog('error', 'Copy failed');
    });
}
