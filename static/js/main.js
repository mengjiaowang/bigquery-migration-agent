
import { connectLogStream, convertSql } from './api.js';
import * as UI from './ui.js';

let isConverting = false;
let lastConversionResult = null;

const SAMPLE_SQL = `CREATE OR REPLACE TABLE original_target_table
AS
WITH
  ExperimentCounts AS (
    SELECT
      experiment_id,
      experiment_variant_id,
      COUNT(DISTINCT user_pseudo_id) AS user_count,
      SUM(COUNT(DISTINCT user_pseudo_id))
        OVER (PARTITION BY experiment_id) AS total_users_in_experiment
    FROM
      abtest
    GROUP BY
      experiment_id,
      experiment_variant_id
  ),
  ExpectedCounts AS (
    SELECT
      experiment_id,
      experiment_variant_id,
      user_count,
      total_users_in_experiment,
      total_users_in_experiment / SUM(total_users_in_experiment)
        OVER () AS expected_proportion,
      SUM(user_count) OVER () AS total_users_overall
    FROM
      ExperimentCounts
  )
SELECT
  experiment_id,
  experiment_variant_id,
  user_count,
  total_users_in_experiment,
  expected_proportion,
  total_users_overall
FROM
  ExpectedCounts`;

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
        UI.addLog('warning', 'Please enter Spark SQL');
        return;
    }

    isConverting = true;
    const btn = document.getElementById('convertBtn');
    const btnText = document.getElementById('convertBtnText');
    
    btn.disabled = true;
    btnText.innerHTML = '<div class="spinner"></div> Converting...';
    UI.resetStatusCards();
    UI.updateStatus('loading', 'Converting...');

    try {
        const startTime = Date.now();
        const result = await convertSql(sparkSql);
        const duration = ((Date.now() - startTime) / 1000).toFixed(2);

        UI.addLog('info', `Conversion completed, duration ${duration}s`);
        
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
            UI.updateStatus('success', 'Conversion Successful');
        } else {
            UI.updateStatus('error', 'Validation Failed');
        }

        if (result.success) {
            UI.addLog('success', '🎉 Conversion Fully Successful!');
        }
        
        if (result.execution_success) {
            let msg = '🚀 SQL Execution Successful';
            if (result.execution_target_table) {
                msg += ` | Target Table: ${result.execution_target_table}`;
            }
            UI.addLog('success', msg);
        } else if (result.execution_error) {
            UI.addLog('error', `SQL Execution Failed: ${result.execution_error}`);
        }

    } catch (error) {
        console.error('Conversion failed:', error);
        UI.addLog('error', `Request Failed: ${error.message}`);
        UI.updateStatus('error', 'Request Failed');
    } finally {
        isConverting = false;
        btn.disabled = false;
        btnText.textContent = 'Start Conversion';
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
    UI.addLog('info', 'Result saved');
}
