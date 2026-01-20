
import { connectLogStream, convertSql } from './api.js';
import * as UI from './ui.js';

let isConverting = false;
let lastConversionResult = null;

const SAMPLE_SQL = `WITH daily_orders AS (
    SELECT 
        to_date(order_time) AS order_date,
        customer_id,
        SUM(amount) AS daily_amount,
        COUNT(*) AS order_count
    FROM orders
    WHERE order_time >= date_sub(current_date(), 30)
    GROUP BY to_date(order_time), customer_id
)
SELECT 
    customer_id,
    AVG(daily_amount) AS avg_daily_spend,
    SUM(order_count) AS total_orders,
    collect_list(order_date) AS order_dates
FROM daily_orders
GROUP BY customer_id
ORDER BY avg_daily_spend DESC
LIMIT 100`;

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    // Connect SSE
    connectLogStream(
        (logEntry) => {
            if (logEntry.type === 'status') {
                UI.handleStatusUpdate(logEntry);
            }
            if (logEntry.type === 'log' || logEntry.type === 'status') {
                UI.addServerLog(logEntry);
            }
        },
        () => console.log('SSE connected'),
        (err) => console.error('SSE error', err)
    );

    // Bind events
    document.getElementById('convertBtn').addEventListener('click', handleConvert);
    document.querySelector('button[onclick="clearInput()"]').onclick = UI.clearInput; // Rebind or remove inline onclick
    // Actually best to replace all inline onclicks
    
    // Replace inline onclicks with event listeners
    replaceInlineEvents();
});

function replaceInlineEvents() {
    // Clear Input
    const clearInputBtn = document.querySelector('.panel .copy-btn[onclick="clearInput()"]');
    if (clearInputBtn) {
        clearInputBtn.removeAttribute('onclick');
        clearInputBtn.addEventListener('click', UI.clearInput);
    }

    // Load Sample
    const loadSampleBtn = document.querySelector('.btn-secondary[onclick="loadSample()"]');
    if (loadSampleBtn) {
        loadSampleBtn.removeAttribute('onclick');
        loadSampleBtn.addEventListener('click', () => UI.loadSample(SAMPLE_SQL));
    }

    // Clear Logs
    const clearLogsBtn = document.querySelector('.info-panel .copy-btn[onclick="clearLogs()"]');
    if (clearLogsBtn) {
        clearLogsBtn.removeAttribute('onclick');
        clearLogsBtn.addEventListener('click', UI.clearLogs);
    }
    
    // Copy Output
    const copyBtn = document.getElementById('copyBtn');
    if (copyBtn) {
        copyBtn.removeAttribute('onclick');
        copyBtn.addEventListener('click', UI.copyOutput);
    }
    
    // Save Result
    const saveBtn = document.getElementById('saveBtn');
    if (saveBtn) {
        saveBtn.removeAttribute('onclick');
        saveBtn.addEventListener('click', saveResult);
    }
}

async function handleConvert() {
    if (isConverting) return;

    const sparkSql = document.getElementById('sparkSql').value.trim();
    if (!sparkSql) {
        UI.addLog('warning', '请输入 Spark SQL');
        return;
    }

    isConverting = true;
    const btn = document.getElementById('convertBtn');
    const btnText = document.getElementById('convertBtnText');
    
    btn.disabled = true;
    btnText.innerHTML = '<div class="spinner"></div> 转换中...';
    UI.resetStatusCards();
    UI.updateStatus('loading', '正在转换...');

    try {
        const startTime = Date.now();
        const result = await convertSql(sparkSql);
        const duration = ((Date.now() - startTime) / 1000).toFixed(2);

        UI.addLog('info', `转换完成，耗时 ${duration}s`);
        
        lastConversionResult = {
            ...result,
            spark_sql: sparkSql,
            conversion_time: new Date().toISOString(),
            duration: duration
        };
        
        document.getElementById('saveBtn').style.display = 'inline-flex';
        
        UI.updateResultCards(result);

        if (result.bigquery_sql) {
            document.getElementById('bqOutput').textContent = result.bigquery_sql;
        }

        if (result.validation_success) {
            UI.updateStatus('success', '转换成功');
        } else {
            UI.updateStatus('error', '验证失败');
        }

        if (result.success) {
            UI.addLog('success', '🎉 转换完全成功!');
        }
        
        if (result.execution_success) {
            let msg = '🚀 SQL 执行成功';
            if (result.execution_target_table) {
                msg += ` | 目标表: ${result.execution_target_table}`;
            }
            UI.addLog('success', msg);
        } else if (result.execution_error) {
            UI.addLog('error', `SQL 执行失败: ${result.execution_error}`);
        }

    } catch (error) {
        console.error('Conversion failed:', error);
        UI.addLog('error', `请求失败: ${error.message}`);
        UI.updateStatus('error', '请求失败');
    } finally {
        isConverting = false;
        btn.disabled = false;
        btnText.textContent = '开始转换';
    }
}

function saveResult() {
    if (!lastConversionResult) return;
    
    const blob = new Blob([JSON.stringify(lastConversionResult, null, 2)], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `conversion_${new Date().getTime()}.json`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
    UI.addLog('info', '结果已保存');
}
