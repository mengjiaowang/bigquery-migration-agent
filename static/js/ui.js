
// UI Helper Functions

export function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

export function addLog(level, message, fromServer = false) {
    const infoPanel = document.getElementById('infoPanel');
    const time = new Date().toLocaleTimeString('zh-CN', { hour12: false });
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
    addLog('info', '已清空输入');
}

export function clearLogs() {
    document.getElementById('infoPanel').innerHTML = '';
    addLog('info', '日志已清除');
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

export function handleStatusUpdate(event) {
    const { step, status, attempt } = event;
    
    // Map step to DOM ID
    const stepMap = {
        'spark': 'sparkStatus',
        'convert': 'convertStatus',
        'bq_dry_run': 'bqStatus',
        'fix': 'bqStatus',  // Fix is part of BQ validation loop
        'execute': 'executionStatus',
        'data_verification': 'dataVerificationStatus'
    };
    
    const elementId = stepMap[step];
    if (elementId) {
        const element = document.getElementById(elementId);
        if (element) {
            if (status === 'loading') {
                element.className = 'status-card-value loading';
                if (step === 'bq_dry_run' && attempt) {
                    element.textContent = `验证中 (第${attempt}次)...`;
                } else if (step === 'fix') {
                    element.textContent = `修复中 (第${attempt}次)...`;
                } else {
                    element.textContent = '运行中...';
                }
            } else if (status === 'success') {
                element.className = 'status-card-value success';
                element.textContent = '✓ 通过';
                if (step === 'execute') element.textContent = '✓ 成功';
            } else if (status === 'error') {
                element.className = 'status-card-value error';
                element.textContent = '✗ 失败';
            }
        }
    }
    
    // Also update global status text if needed
    if (status === 'loading') {
        updateStatus('loading', `正在执行: ${step}...`);
    } else if (status === 'completed') {
        updateStatus('success', '转换完成');
    }
}

export function updateResultCards(result) {
    const sparkStatus = document.getElementById('sparkStatus');
    const bqStatus = document.getElementById('bqStatus');
    sparkStatus.textContent = result.spark_valid ? '✓ 通过' : '✗ 失败';
    sparkStatus.className = 'status-card-value ' + (result.spark_valid ? 'success' : 'error');

    const convertStatus = document.getElementById('convertStatus');
    if (result.bigquery_sql) {
        convertStatus.textContent = '✓ 完成';
        convertStatus.className = 'status-card-value success';
    } else if (result.spark_valid) {
        // If spark valid but no BQ SQL, it might have failed during conversion
        convertStatus.textContent = '✗ 失败';
        convertStatus.className = 'status-card-value error';
    } else {
        convertStatus.textContent = '-';
        convertStatus.className = 'status-card-value pending';
    }

    bqStatus.textContent = result.validation_success ? '✓ 通过' : '✗ 失败';
    bqStatus.className = 'status-card-value ' + (result.validation_success ? 'success' : 'error');

    const executionStatus = document.getElementById('executionStatus');
    if (result.execution_success !== undefined && result.execution_success !== null) {
        executionStatus.textContent = result.execution_success ? '✓ 成功' : '✗ 失败';
        executionStatus.className = 'status-card-value ' + (result.execution_success ? 'success' : 'error');
        
    } else {
        executionStatus.textContent = '-';
        executionStatus.className = 'status-card-value pending';
    }

    const dataVerificationStatus = document.getElementById('dataVerificationStatus');
    if (result.data_verification_success !== undefined && result.data_verification_success !== null) {
        dataVerificationStatus.textContent = result.data_verification_success ? '✓ 通过' : '✗ 失败';
        dataVerificationStatus.className = 'status-card-value ' + (result.data_verification_success ? 'success' : 'error');
    } else {
        dataVerificationStatus.textContent = '-';
        dataVerificationStatus.className = 'status-card-value pending';
    }
}

export function loadSample(sampleSql) {
    document.getElementById('sparkSql').value = sampleSql;
    addLog('info', '已加载示例 SQL');
}

export function copyOutput() {
    const output = document.getElementById('bqOutput').textContent;
    if (!output || output.trim() === '转换结果将在这里显示') {
        addLog('warning', '没有可复制的内容');
        return;
    }
    
    navigator.clipboard.writeText(output).then(() => {
        const btn = document.getElementById('copyBtn');
        const originalText = btn.textContent;
        btn.textContent = '已复制!';
        btn.classList.add('copied');
        
        setTimeout(() => {
            btn.textContent = originalText;
            btn.classList.remove('copied');
        }, 2000);
        addLog('info', '已复制到剪贴板');
    }).catch(err => {
        console.error('Copy failed:', err);
        addLog('error', '复制失败');
    });
}
