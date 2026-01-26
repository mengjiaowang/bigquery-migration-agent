
import { connectLogStream, convertSql } from './api.js';
import * as UI from './ui.js';

let isConverting = false;
let lastConversionResult = null;

const SAMPLE_SQL = `SELECT
  term,
  refresh_date,
  rank
FROM
  google_trends_top_terms
WHERE
  rank <= 10
  AND refresh_date >= '2024-01-01'
ORDER BY
  refresh_date DESC, 
  rank ASC
LIMIT 100`;

// Sound Effects
const audioContext = new (window.AudioContext || window.webkitAudioContext)();

function playSound(type) {
    if (audioContext.state === 'suspended') {
        audioContext.resume();
    }

    const oscillator = audioContext.createOscillator();
    const gainNode = audioContext.createGain();

    oscillator.connect(gainNode);
    gainNode.connect(audioContext.destination);

    if (type === 'success') {
        // Cheerful "ding" (C5 -> E5)
        oscillator.type = 'sine';
        oscillator.frequency.setValueAtTime(523.25, audioContext.currentTime); // C5
        oscillator.frequency.exponentialRampToValueAtTime(659.25, audioContext.currentTime + 0.1); // E5
        gainNode.gain.setValueAtTime(0.1, audioContext.currentTime);
        gainNode.gain.exponentialRampToValueAtTime(0.01, audioContext.currentTime + 0.5);
        oscillator.start();
        oscillator.stop(audioContext.currentTime + 0.5);
    } else if (type === 'error') {
        // Error "buzz" (low saw)
        oscillator.type = 'sawtooth';
        oscillator.frequency.setValueAtTime(150, audioContext.currentTime);
        gainNode.gain.setValueAtTime(0.1, audioContext.currentTime);
        gainNode.gain.exponentialRampToValueAtTime(0.01, audioContext.currentTime + 0.3);
        oscillator.start();
        oscillator.stop(audioContext.currentTime + 0.3);
    }
}

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    // Connect SSE
    connectLogStream(
        (logEntry) => {
            if (logEntry.type === 'status') {
                UI.handleStatusUpdate(logEntry);
            } else if (logEntry.type === 'sql_output') {
                // Streaming SQL Output
                const bqOutput = document.getElementById('bqOutput');
                if (bqOutput) {
                   bqOutput.textContent = logEntry.sql;
                   // Show save button
                   document.getElementById('saveBtn').style.display = 'inline-flex';
                }
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
            playSound('success'); 
        } else {
            UI.updateStatus('error', 'Validation Failed');
            playSound('error');
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
        playSound('error');
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
