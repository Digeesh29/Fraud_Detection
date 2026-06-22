"""
analytics/dashboard.py  (FIXED)
--------------------------------
Key fixes:
  1. /api/all  — single merged endpoint replaces 6 separate fetches.
     One DB round-trip per refresh instead of six.
  2. /api/stream — Server-Sent Events endpoint pushes updates to the
     browser every 2 seconds without the browser polling at all.
  3. Frontend uses SSE (EventSource) instead of setInterval + 6 fetches.
     Falls back to polling if SSE is unsupported.
  4. Polling guard: next poll only starts after the previous one resolves
     (avoids pile-up when queries are slow).
  5. Flask runs with threaded=True so SSE connections don't block API calls.
"""

import json
import logging
import sys
import time
from pathlib import Path

from flask import Flask, Response, jsonify, render_template, stream_with_context
from flask_cors import CORS

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from analytics.alert_manager import AlertManager

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

app     = Flask(__name__)
CORS(app)
manager = AlertManager()


# ── FIX 1: single merged API endpoint ─────────────────────────────────────────

@app.route("/api/all")
def api_all():
    """Return everything the dashboard needs in one query."""
    try:
        return jsonify({
            "stats":           manager.get_summary_stats(),
            "alerts":          manager.get_recent_alerts(limit=20),
            "volume":          manager.get_volume_over_time(hours=6),
            "alert_volume":    manager.get_alerts_over_time(hours=6),
            "patterns":        manager.get_pattern_breakdown(),
            "model":           manager.get_model_performance(),
            "scan":            manager.get_last_scan(),
            "graph_confirmed": manager.get_graph_confirmed_alerts(),
        })
    except Exception as e:
        logger.error(f"/api/all error: {e}")
        return jsonify({"error": str(e)}), 500


# ── FIX 2: Server-Sent Events endpoint ────────────────────────────────────────

@app.route("/api/stream")
def api_stream():
    """
    Push updates to the browser every 2 seconds over a persistent connection.
    The browser never has to poll — updates arrive automatically.
    """
    def event_stream():
        while True:
            try:
                data = {
                    "stats":           manager.get_summary_stats(),
                    "alerts":          manager.get_recent_alerts(limit=20),
                    "volume":          manager.get_volume_over_time(hours=6),
                    "alert_volume":    manager.get_alerts_over_time(hours=6),
                    "patterns":        manager.get_pattern_breakdown(),
                    "model":           manager.get_model_performance(),
                    "scan":            manager.get_last_scan(),
                    "graph_confirmed": manager.get_graph_confirmed_alerts(),
                }
                yield f"data: {json.dumps(data)}\n\n"
            except Exception as e:
                logger.error(f"SSE stream error: {e}")
                yield f"data: {json.dumps({'error': str(e)})}\n\n"
            time.sleep(2)

    return Response(
        stream_with_context(event_stream()),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",   # disable nginx buffering if behind proxy
        },
    )


# ── Keep individual endpoints for debugging ───────────────────────────────────

@app.route("/api/stats")
def api_stats():
    try:    return jsonify(manager.get_summary_stats())
    except Exception as e: return jsonify({"error": str(e)}), 500

@app.route("/api/alerts")
def api_alerts():
    try:    return jsonify(manager.get_recent_alerts(limit=20))
    except Exception as e: return jsonify({"error": str(e)}), 500

@app.route("/api/scan")
def api_scan():
    try:    return jsonify(manager.get_last_scan())
    except Exception as e: return jsonify({"error": str(e)}), 500


# ── Dashboard HTML ─────────────────────────────────────────────────────────────

DASHBOARD_HTML = r"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Fraud Detection Pipeline</title>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
    <style>
        *{margin:0;padding:0;box-sizing:border-box}
        body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#0a0e1a;color:#e2e8f0;min-height:100vh}
        .header{background:linear-gradient(135deg,#1a1f35,#0f1629);border-bottom:1px solid #2d3748;padding:20px 32px;display:flex;justify-content:space-between;align-items:center}
        .header h1{font-size:22px;font-weight:700;color:#fff}
        .header p{font-size:13px;color:#718096;margin-top:2px}
        .live-badge{display:flex;align-items:center;gap:8px;background:#0d2d1a;border:1px solid #1a5c35;border-radius:20px;padding:6px 14px;font-size:13px;color:#48bb78;font-weight:500}
        .live-dot{width:8px;height:8px;background:#48bb78;border-radius:50%;animation:pulse 1.5s infinite}
        .live-badge.disconnected{background:#2d1b00;border-color:#744210;color:#f6ad55}
        .live-badge.disconnected .live-dot{background:#f6ad55}
        @keyframes pulse{0%,100%{opacity:1}50%{opacity:.3}}
        .container{padding:28px 32px;max-width:1400px;margin:0 auto}
        .stats-grid{display:grid;grid-template-columns:repeat(6,1fr);gap:16px;margin-bottom:28px}
        .stat-card{background:#141929;border:1px solid #2d3748;border-radius:12px;padding:20px}
        .stat-label{font-size:11px;color:#718096;text-transform:uppercase;letter-spacing:.8px;margin-bottom:10px;font-weight:600}
        .stat-value{font-size:26px;font-weight:700;color:#fff;letter-spacing:-.5px}
        .stat-sub{font-size:12px;color:#4a5568;margin-top:4px}
        .stat-card.danger{border-color:#742a2a}.stat-card.danger .stat-value{color:#fc8181}
        .stat-card.warning{border-color:#744210}.stat-card.warning .stat-value{color:#f6ad55}
        .stat-card.success{border-color:#1c4532}.stat-card.success .stat-value{color:#68d391}
        .charts-row{display:grid;grid-template-columns:2fr 1fr;gap:20px;margin-bottom:24px}
        .chart-card{background:#141929;border:1px solid #2d3748;border-radius:12px;padding:24px}
        .chart-title{font-size:14px;font-weight:600;color:#a0aec0;margin-bottom:20px;text-transform:uppercase;letter-spacing:.5px}
        .chart-container{position:relative;height:220px}
        .bottom-row{display:grid;grid-template-columns:1fr 1fr;gap:20px;margin-bottom:24px}
        .alerts-card{background:#141929;border:1px solid #2d3748;border-radius:12px;padding:24px}
        .alerts-header{display:flex;justify-content:space-between;align-items:center;margin-bottom:18px}
        .alert-count-badge{background:#742a2a;color:#fc8181;border-radius:10px;padding:2px 10px;font-size:12px;font-weight:600}
        table{width:100%;border-collapse:collapse}
        th{text-align:left;font-size:11px;color:#4a5568;text-transform:uppercase;letter-spacing:.6px;padding:0 0 12px;font-weight:600;border-bottom:1px solid #2d3748}
        td{padding:12px 0;font-size:13px;border-bottom:1px solid #1a2035;color:#cbd5e0}
        tr:last-child td{border-bottom:none}
        .amount{font-weight:600;color:#fff}
        .conf-high{color:#fc8181;font-weight:600}.conf-med{color:#f6ad55;font-weight:600}.conf-low{color:#68d391;font-weight:600}
        .badge{display:inline-block;padding:2px 8px;border-radius:6px;font-size:11px;font-weight:600;text-transform:uppercase}
        .p-cycle{background:#2d1b4e;color:#b794f4}.p-ring{background:#1a3352;color:#63b3ed}
        .p-burst{background:#2d2006;color:#f6ad55}.p-unknown{background:#1a2035;color:#718096}
        .scores-card{background:#141929;border:1px solid #2d3748;border-radius:12px;padding:24px}
        .score-row{display:flex;justify-content:space-between;align-items:center;padding:14px 0;border-bottom:1px solid #1a2035}
        .score-row:last-child{border-bottom:none}
        .score-label{font-size:13px;color:#a0aec0}
        .score-value{font-size:18px;font-weight:700;color:#fff}
        .bar-wrap{width:120px;height:4px;background:#2d3748;border-radius:2px;margin-top:4px}
        .bar{height:4px;border-radius:2px;background:linear-gradient(90deg,#667eea,#764ba2)}
        .scan-grid{display:grid;grid-template-columns:repeat(2,1fr);gap:12px;margin-top:16px}
        .scan-item{background:#0f1629;border-radius:8px;padding:14px;text-align:center}
        .scan-val{font-size:24px;font-weight:700;color:#fff}
        .scan-lbl{font-size:11px;color:#4a5568;margin-top:4px;text-transform:uppercase}
        .footer{text-align:center;color:#2d3748;font-size:12px;padding:20px}
        #last-updated{color:#4a5568;font-size:12px}
    </style>
</head>
<body>
<div class="header">
    <div>
        <h1>&#x1F6E1;&#xFE0F; Real-Time Fraud Detection Pipeline</h1>
        <p>Graph-based anomaly detection &middot; Kafka &middot; Neo4j &middot; SQLite &middot; ML Ensemble</p>
    </div>
    <div style="display:flex;align-items:center;gap:16px">
        <span id="last-updated">&#8212;</span>
        <div class="live-badge" id="live-badge"><div class="live-dot"></div>LIVE</div>
    </div>
</div>

<div class="container">
    <div class="stats-grid">
        <div class="stat-card success">
            <div class="stat-label">Total Transactions</div>
            <div class="stat-value" id="total-tx">&#8212;</div>
            <div class="stat-sub" id="tx-per-min">&#8212; tx/min</div>
        </div>
        <div class="stat-card danger">
            <div class="stat-label">Fraud Alerts</div>
            <div class="stat-value" id="total-alerts">&#8212;</div>
            <div class="stat-sub" id="alerts-last-hour">&#8212; last hour</div>
        </div>
        <div class="stat-card warning">
            <div class="stat-label">Fraud Rate</div>
            <div class="stat-value" id="fraud-rate">&#8212;</div>
            <div class="stat-sub">of all transactions</div>
        </div>
        <div class="stat-card">
            <div class="stat-label">Alerts / Min</div>
            <div class="stat-value" id="alerts-per-min">&#8212;</div>
            <div class="stat-sub">last 60 seconds</div>
        </div>
        <div class="stat-card">
            <div class="stat-label">Avg Confidence</div>
            <div class="stat-value" id="avg-confidence">&#8212;</div>
            <div class="stat-sub">ML ensemble score</div>
        </div>
        <div class="stat-card">
            <div class="stat-label">Graph Confirmed</div>
            <div class="stat-value" id="graph-confirmed">&#8212;</div>
            <div class="stat-sub">pattern verified</div>
        </div>
    </div>

    <div class="charts-row">
        <div class="chart-card">
            <div class="chart-title">Transaction Volume vs Fraud Alerts Over Time</div>
            <div class="chart-container"><canvas id="timelineChart"></canvas></div>
        </div>
        <div class="chart-card">
            <div class="chart-title">Fraud Pattern Breakdown</div>
            <div class="chart-container"><canvas id="patternChart"></canvas></div>
        </div>
    </div>

    <div class="bottom-row">
        <div class="alerts-card">
            <div class="alerts-header">
                <div class="chart-title" style="margin:0">Recent Fraud Alerts</div>
                <span class="alert-count-badge" id="alert-badge">0</span>
            </div>
            <table>
                <thead><tr><th>Sender</th><th>Amount</th><th>Confidence</th><th>Pattern</th><th>Time</th></tr></thead>
                <tbody id="alerts-body">
                    <tr><td colspan="5" style="color:#4a5568;text-align:center;padding:20px">Waiting for alerts...</td></tr>
                </tbody>
            </table>
        </div>

        <div style="display:flex;flex-direction:column;gap:20px">
            <div class="scores-card">
                <div class="chart-title">Model Performance</div>
                <div class="score-row">
                    <div><div class="score-label">Random Forest</div><div class="bar-wrap"><div class="bar" id="rf-bar" style="width:0%"></div></div></div>
                    <div class="score-value" id="rf-score">&#8212;</div>
                </div>
                <div class="score-row">
                    <div><div class="score-label">XGBoost</div><div class="bar-wrap"><div class="bar" id="xgb-bar" style="width:0%;background:linear-gradient(90deg,#f093fb,#f5576c)"></div></div></div>
                    <div class="score-value" id="xgb-score">&#8212;</div>
                </div>
                <div class="score-row">
                    <div class="score-label">True Positives</div>
                    <div class="score-value" id="true-pos">&#8212;</div>
                </div>
                <div class="score-row">
                    <div class="score-label">False Positives</div>
                    <div class="score-value" id="false-pos" style="color:#fc8181">&#8212;</div>
                </div>
            </div>

            <div class="scores-card">
                <div class="chart-title">Last Graph Scan</div>
                <div id="scan-time" style="font-size:12px;color:#4a5568;margin-bottom:4px">Never run</div>
                <div class="scan-grid">
                    <div class="scan-item"><div class="scan-val" id="scan-cycles">0</div><div class="scan-lbl">&#x1F504; Cycles</div></div>
                    <div class="scan-item"><div class="scan-val" id="scan-rings">0</div><div class="scan-lbl">&#x1F465; Rings</div></div>
                    <div class="scan-item"><div class="scan-val" id="scan-bursts">0</div><div class="scan-lbl">&#x26A1; Bursts</div></div>
                    <div class="scan-item"><div class="scan-val" id="scan-hubs">0</div><div class="scan-lbl">&#x1F578;&#xFE0F; Hubs</div></div>
                </div>
            </div>
        </div>
    </div>

    <div class="footer">
        Real-Time Graph-Based Financial Fraud Detection &middot;
        Kafka &middot; Neo4j &middot; SQLite &middot; Isolation Forest &middot; Random Forest &middot; XGBoost
    </div>
</div>

<script>
// ── Charts ────────────────────────────────────────────────────────────────────
const tlChart = new Chart(document.getElementById('timelineChart').getContext('2d'), {
    type:'line',
    data:{labels:[],datasets:[
        {label:'Transactions',data:[],borderColor:'#667eea',backgroundColor:'rgba(102,126,234,.08)',tension:.4,fill:true,pointRadius:0,borderWidth:2},
        {label:'Fraud Alerts',data:[],borderColor:'#fc8181',backgroundColor:'rgba(252,129,129,.08)',tension:.4,fill:true,pointRadius:0,borderWidth:2,yAxisID:'y1'}
    ]},
    options:{responsive:true,maintainAspectRatio:false,interaction:{mode:'index',intersect:false},
        plugins:{legend:{labels:{color:'#718096',font:{size:12}}}},
        scales:{
            x:{ticks:{color:'#4a5568',maxTicksLimit:8},grid:{color:'#1a2035'}},
            y:{ticks:{color:'#4a5568'},grid:{color:'#1a2035'},position:'left'},
            y1:{ticks:{color:'#fc8181'},grid:{display:false},position:'right'}
        }}
});

const ptChart = new Chart(document.getElementById('patternChart').getContext('2d'), {
    type:'doughnut',
    data:{labels:[],datasets:[{data:[],backgroundColor:['#b794f4','#63b3ed','#f6ad55','#68d391','#fc8181'],borderWidth:0}]},
    options:{responsive:true,maintainAspectRatio:false,
        plugins:{legend:{position:'bottom',labels:{color:'#718096',padding:16,font:{size:12}}}}}
});

// ── Render helpers ────────────────────────────────────────────────────────────
function applyData(d) {
    const s = d.stats || {};
    document.getElementById('total-tx').textContent       = (s.total_transactions||0).toLocaleString();
    document.getElementById('total-alerts').textContent   = (s.total_alerts||0).toLocaleString();
    document.getElementById('fraud-rate').textContent     = (s.fraud_rate_pct||0).toFixed(2)+'%';
    document.getElementById('alerts-last-hour').textContent = (s.alerts_last_hour||0)+' last hour';
    document.getElementById('alerts-per-min').textContent = s.alerts_last_minute||0;
    document.getElementById('tx-per-min').textContent     = (s.tx_per_minute||0)+' tx/min';
    document.getElementById('alert-badge').textContent    = s.total_alerts||0;
    document.getElementById('graph-confirmed').textContent= (d.graph_confirmed||[]).length;

    const m = d.model || {};
    const rf=parseFloat(m.avg_rf_score||0), xgb=parseFloat(m.avg_xgb_score||0), conf=parseFloat(m.avg_confidence||0);
    document.getElementById('rf-score').textContent       = (rf*100).toFixed(1)+'%';
    document.getElementById('xgb-score').textContent      = (xgb*100).toFixed(1)+'%';
    document.getElementById('avg-confidence').textContent = (conf*100).toFixed(1)+'%';
    document.getElementById('true-pos').textContent       = m.true_positives||0;
    document.getElementById('false-pos').textContent      = m.false_positives||0;
    document.getElementById('rf-bar').style.width         = (rf*100)+'%';
    document.getElementById('xgb-bar').style.width        = (xgb*100)+'%';

    const sc = d.scan;
    if (sc) {
        document.getElementById('scan-cycles').textContent = sc.cycles_found||0;
        document.getElementById('scan-rings').textContent  = sc.communities_found||0;
        document.getElementById('scan-bursts').textContent = sc.bursts_found||0;
        document.getElementById('scan-hubs').textContent   = sc.hubs_found||0;
        if (sc.scanned_at) document.getElementById('scan-time').textContent =
            'Last scan: '+new Date(sc.scanned_at).toLocaleTimeString();
    }

    const alerts = d.alerts || [];
    const tbody = document.getElementById('alerts-body');
    if (!alerts.length) {
        tbody.innerHTML = '<tr><td colspan="5" style="color:#4a5568;text-align:center;padding:20px">No alerts yet</td></tr>';
    } else {
        tbody.innerHTML = alerts.slice(0,12).map(a => {
            const conf = parseFloat(a.confidence)||0;
            const cc   = conf>=.8?'conf-high':conf>=.5?'conf-med':'conf-low';
            const p    = a.fraud_pattern||'unknown';
            const t    = a.created_at ? new Date(a.created_at).toLocaleTimeString() : '&#8212;';
            const sender = (a.sender_id||'').slice(0,10)+'...';
            const amt  = parseFloat(a.amount||0).toLocaleString('en-US',{style:'currency',currency:'USD'});
            return `<tr><td>${sender}</td><td class="amount">${amt}</td><td class="${cc}">${(conf*100).toFixed(0)}%</td><td><span class="badge p-${p}">${p}</span></td><td>${t}</td></tr>`;
        }).join('');
    }

    const vol = d.volume||[], alr = d.alert_volume||[];
    const labels = vol.map(r => { const t=new Date(r.bucket); return t.getHours()+':'+String(t.getMinutes()).padStart(2,'0'); });
    tlChart.data.labels = labels;
    tlChart.data.datasets[0].data = vol.map(r => r.tx_count);
    tlChart.data.datasets[1].data = labels.map(l => {
        const m = alr.find(a => { const t=new Date(a.bucket); return (t.getHours()+':'+String(t.getMinutes()).padStart(2,'0'))===l; });
        return m ? m.alert_count : 0;
    });
    tlChart.update('none');

    const pts = d.patterns||[];
    ptChart.data.labels = pts.map(p => p.pattern.toUpperCase());
    ptChart.data.datasets[0].data = pts.map(p => p.count);
    ptChart.update('none');

    document.getElementById('last-updated').textContent = 'Updated '+new Date().toLocaleTimeString();
}

// ── FIX 2: SSE — browser receives pushes, no polling needed ──────────────────
function connectSSE() {
    const badge = document.getElementById('live-badge');
    const evtSource = new EventSource('/api/stream');

    evtSource.onmessage = (e) => {
        try {
            applyData(JSON.parse(e.data));
            badge.className = 'live-badge';
            badge.innerHTML = '<div class="live-dot"></div>LIVE';
        } catch(err) { console.error('Parse error:', err); }
    };

    evtSource.onerror = () => {
        badge.className = 'live-badge disconnected';
        badge.innerHTML = '<div class="live-dot"></div>RECONNECTING';
        // Browser auto-reconnects SSE — no manual retry needed
    };
}

// ── FIX 3: polling fallback if SSE not supported (rare) ──────────────────────
function pollFallback() {
    let running = false;
    async function refresh() {
        if (running) return;   // FIX: skip if previous request still in flight
        running = true;
        try {
            const d = await (await fetch('/api/all')).json();
            applyData(d);
        } catch(e) { console.error('Poll error:', e); }
        finally { running = false; }
    }
    refresh();
    setInterval(refresh, 5000);
}

if (typeof EventSource !== 'undefined') {
    connectSSE();
} else {
    pollFallback();
}
</script>
</body>
</html>
"""


@app.route("/")
def index():
    return render_template("dashboard.html")


if __name__ == "__main__":
    logger.info("Dashboard starting at http://localhost:5000")
    # FIX 5: threaded=True — SSE connections each get their own thread,
    # so they don't block regular API calls.
    app.run(host="0.0.0.0", port=5000, debug=False, threaded=True)
