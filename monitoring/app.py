import json
import os
import threading
import time
from datetime import datetime, timezone

import requests
from flask import Flask, jsonify, render_template_string
from kafka import TopicPartition
from kafka.admin import KafkaAdminClient
from kafka.consumer import KafkaConsumer

app = Flask(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "stock-prices-daily")
HDFS_WEBHDFS_BASE = os.getenv("HDFS_WEBHDFS_BASE", "http://namenode:9870/webhdfs/v1")
HDFS_OUTPUT_PATH = os.getenv("HDFS_OUTPUT_PATH", "/data/stock_prices_daily")
NAMENODE_JMX_URL = os.getenv(
    "NAMENODE_JMX_URL", "http://namenode:9870/jmx?qry=Hadoop:service=NameNode,name=FSNamesystem"
)
METRICS_SAMPLE_SECONDS = float(os.getenv("METRICS_SAMPLE_SECONDS", "5"))

# Keep a small in-memory state to estimate throughput.
_state_lock = threading.Lock()
_last_sample = {
    "timestamp": None,
    "messages_total": None,
    "throughput_mps": 0.0,
}


HTML_TEMPLATE = """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Big Data Pipeline Monitor</title>
  <style>
    :root {
      --bg: #f5f7fb;
      --card: #ffffff;
      --ink: #1f2937;
      --muted: #6b7280;
      --accent: #0f766e;
      --accent-2: #f59e0b;
      --danger: #b91c1c;
      --ok: #166534;
      --border: #e5e7eb;
    }

    * {
      box-sizing: border-box;
      margin: 0;
      padding: 0;
    }

    body {
      font-family: "Segoe UI", Tahoma, Geneva, Verdana, sans-serif;
      background:
        radial-gradient(circle at 20% 15%, #dbeafe 0%, transparent 40%),
        radial-gradient(circle at 85% 10%, #ccfbf1 0%, transparent 35%),
        var(--bg);
      color: var(--ink);
      min-height: 100vh;
      padding: 24px;
    }

    .wrap {
      max-width: 1200px;
      margin: 0 auto;
    }

    .hero {
      background: linear-gradient(120deg, #0f766e, #0e7490);
      color: #fff;
      border-radius: 18px;
      padding: 20px 24px;
      box-shadow: 0 12px 28px rgba(15, 118, 110, 0.25);
      margin-bottom: 18px;
    }

    .hero h1 {
      font-size: 1.55rem;
      margin-bottom: 4px;
      letter-spacing: 0.2px;
    }

    .hero p {
      color: #d1fae5;
      font-size: 0.95rem;
    }

    .grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(210px, 1fr));
      gap: 14px;
      margin-bottom: 16px;
    }

    .card {
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 14px;
      padding: 14px;
      box-shadow: 0 6px 18px rgba(15, 23, 42, 0.06);
    }

    .label {
      font-size: 0.78rem;
      text-transform: uppercase;
      letter-spacing: 0.07em;
      color: var(--muted);
      margin-bottom: 6px;
    }

    .value {
      font-size: 1.45rem;
      font-weight: 700;
      color: var(--ink);
      line-height: 1.2;
      word-break: break-word;
    }

    .sub {
      margin-top: 7px;
      font-size: 0.86rem;
      color: var(--muted);
    }

    .status {
      display: inline-block;
      font-weight: 600;
      padding: 4px 9px;
      border-radius: 999px;
      font-size: 0.78rem;
    }

    .status.ok {
      background: #dcfce7;
      color: var(--ok);
    }

    .status.warn {
      background: #fef3c7;
      color: #92400e;
    }

    .status.bad {
      background: #fee2e2;
      color: var(--danger);
    }

    .section {
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 14px;
      padding: 14px;
      box-shadow: 0 6px 18px rgba(15, 23, 42, 0.05);
    }

    .section h2 {
      font-size: 1.02rem;
      margin-bottom: 8px;
    }

    .table {
      width: 100%;
      border-collapse: collapse;
      font-size: 0.92rem;
    }

    .table th,
    .table td {
      padding: 10px 8px;
      border-bottom: 1px dashed var(--border);
      text-align: left;
      vertical-align: top;
    }

    .table th {
      color: var(--muted);
      font-weight: 600;
      width: 220px;
    }

    .footer {
      margin-top: 12px;
      color: var(--muted);
      font-size: 0.85rem;
    }

    @media (max-width: 640px) {
      body {
        padding: 14px;
      }

      .hero {
        padding: 14px;
      }

      .value {
        font-size: 1.2rem;
      }

      .table th {
        width: 45%;
      }
    }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="hero">
      <h1>Stock Pipeline Live Monitor</h1>
      <p>The page refreshes every 5 seconds. It tracks Kafka flow and HDFS/DataNode persistence.</p>
    </div>

    <div class="grid">
      <div class="card">
        <div class="label">Kafka Throughput</div>
        <div class="value" id="kafka-throughput">-</div>
        <div class="sub">messages / second</div>
      </div>
      <div class="card">
        <div class="label">Kafka Total Messages</div>
        <div class="value" id="kafka-total">-</div>
        <div class="sub" id="kafka-partitions">-</div>
      </div>
      <div class="card">
        <div class="label">HDFS Parquet Files</div>
        <div class="value" id="hdfs-files">-</div>
        <div class="sub" id="hdfs-size">-</div>
      </div>
      <div class="card">
        <div class="label">Persisted To DataNode</div>
        <div class="value" id="persisted-status">-</div>
        <div class="sub">Based on HDFS files + live DataNode</div>
      </div>
    </div>

    <div class="section">
      <h2>Cluster Status</h2>
      <table class="table">
        <tr>
          <th>Live DataNodes</th>
          <td id="dn-live">-</td>
        </tr>
        <tr>
          <th>Dead DataNodes</th>
          <td id="dn-dead">-</td>
        </tr>
        <tr>
          <th>HDFS Directory</th>
          <td id="hdfs-path">-</td>
        </tr>
        <tr>
          <th>Last Parquet Update</th>
          <td id="hdfs-last-mod">-</td>
        </tr>
        <tr>
          <th>Last Poll Time (UTC)</th>
          <td id="poll-time">-</td>
        </tr>
      </table>
      <div class="footer" id="error-box"></div>
    </div>
  </div>

  <script>
    function humanBytes(value) {
      if (value === null || value === undefined || isNaN(value)) return "-";
      const units = ["B", "KB", "MB", "GB", "TB"];
      let size = Number(value);
      let unit = 0;
      while (size >= 1024 && unit < units.length - 1) {
        size /= 1024;
        unit += 1;
      }
      return `${size.toFixed(size < 10 ? 2 : 1)} ${units[unit]}`;
    }

    function formatTs(iso) {
      if (!iso) return "-";
      const d = new Date(iso);
      if (isNaN(d.getTime())) return iso;
      return d.toISOString().replace("T", " ").replace(".000Z", " UTC");
    }

    function statusBadge(ok, warnText, okText) {
      if (ok === true) return `<span class=\"status ok\">${okText}</span>`;
      if (ok === false) return `<span class=\"status bad\">${warnText}</span>`;
      return `<span class=\"status warn\">Unknown</span>`;
    }

    async function loadMetrics() {
      let payload;
      try {
        const res = await fetch('/api/metrics');
        payload = await res.json();
      } catch (err) {
        document.getElementById('error-box').textContent = `Cannot fetch /api/metrics: ${err}`;
        return;
      }

      const kafka = payload.kafka || {};
      const hdfs = payload.hdfs || {};
      const datanode = payload.datanode || {};

      document.getElementById('kafka-throughput').textContent = Number(kafka.throughput_mps || 0).toFixed(2);
      document.getElementById('kafka-total').textContent = String(kafka.messages_total ?? '-');
      document.getElementById('kafka-partitions').textContent = `Partitions: ${kafka.partitions ?? '-'}`;
      document.getElementById('hdfs-files').textContent = String(hdfs.parquet_files ?? '-');
      document.getElementById('hdfs-size').textContent = `Total size: ${humanBytes(hdfs.total_size_bytes)}`;

      const persisted = payload.persisted_to_datanode;
      const persistedHtml = persisted
        ? '<span class="status ok">YES</span>'
        : '<span class="status bad">NO</span>';
      document.getElementById('persisted-status').innerHTML = persistedHtml;

      document.getElementById('dn-live').innerHTML = statusBadge((datanode.live_nodes || 0) > 0, 'No live node', `${datanode.live_nodes ?? 0}`);
      document.getElementById('dn-dead').textContent = String(datanode.dead_nodes ?? '-');
      document.getElementById('hdfs-path').textContent = hdfs.path || '-';
      document.getElementById('hdfs-last-mod').textContent = formatTs(hdfs.last_modified_utc);
      document.getElementById('poll-time').textContent = formatTs(payload.polled_at_utc);

      const errors = [];
      if (payload.errors && payload.errors.length) {
        payload.errors.forEach((e) => errors.push(e));
      }
      document.getElementById('error-box').textContent = errors.join(' | ');
    }

    loadMetrics();
    setInterval(loadMetrics, 5000);
  </script>
</body>
</html>
"""


def _parse_live_nodes(raw_value):
    if not raw_value:
        return {}
    if isinstance(raw_value, dict):
        return raw_value
    if isinstance(raw_value, str):
        try:
            return json.loads(raw_value)
        except json.JSONDecodeError:
            return {}
    return {}


def _hdfs_list_status(path):
    url = f"{HDFS_WEBHDFS_BASE}{path}?op=LISTSTATUS"
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    payload = resp.json()
    return payload.get("FileStatuses", {}).get("FileStatus", [])


def _walk_hdfs(path):
    files = []
    stack = [path]

    while stack:
        current = stack.pop()
        statuses = _hdfs_list_status(current)
        for node in statuses:
            node_type = node.get("type")
            suffix = node.get("pathSuffix", "")
            full_path = current.rstrip("/") + "/" + suffix if suffix else current
            if node_type == "DIRECTORY":
                stack.append(full_path)
            elif node_type == "FILE":
                files.append({
                    "path": full_path,
                    "length": int(node.get("length", 0)),
                    "modificationTime": int(node.get("modificationTime", 0)),
                })
    return files


def _collect_kafka_totals():
    admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, client_id="monitoring-admin")
    try:
        topic_meta = admin.describe_topics([KAFKA_TOPIC])
        partitions = [p["partition"] for p in topic_meta[0].get("partitions", [])]
    finally:
        admin.close()

    if not partitions:
        return 0, 0

    consumer = KafkaConsumer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, group_id=None, enable_auto_commit=False)
    try:
        topic_partitions = [TopicPartition(KAFKA_TOPIC, p) for p in partitions]
        end_offsets = consumer.end_offsets(topic_partitions)
        messages_total = sum(end_offsets.get(tp, 0) for tp in topic_partitions)
        return int(messages_total), len(partitions)
    finally:
        consumer.close()


def _collect_datanode_status():
    resp = requests.get(NAMENODE_JMX_URL, timeout=10)
    resp.raise_for_status()
    beans = resp.json().get("beans", [])
    if not beans:
        return {
            "live_nodes": 0,
            "dead_nodes": 0,
            "decommissioning_nodes": 0,
        }

    fsn = beans[0]
    if "NumLiveDataNodes" in fsn:
        return {
            "live_nodes": int(fsn.get("NumLiveDataNodes", 0) or 0),
            "dead_nodes": int(fsn.get("NumDeadDataNodes", 0) or 0),
            "decommissioning_nodes": int(fsn.get("NumDecommissioningDataNodes", 0) or 0),
        }

    live_nodes_raw = fsn.get("LiveNodes", "{}")
    dead_nodes_raw = fsn.get("DeadNodes", "{}")
    decom_nodes_raw = fsn.get("DecomNodes", "{}")

    live_nodes = _parse_live_nodes(live_nodes_raw)
    dead_nodes = _parse_live_nodes(dead_nodes_raw)
    decom_nodes = _parse_live_nodes(decom_nodes_raw)

    return {
        "live_nodes": len(live_nodes),
        "dead_nodes": len(dead_nodes),
        "decommissioning_nodes": len(decom_nodes),
    }


def collect_metrics():
    errors = []
    now = datetime.now(timezone.utc)

    messages_total = 0
    partitions = 0
    throughput_mps = 0.0

    hdfs_files = []
    hdfs_total_bytes = 0
    hdfs_last_modified = None

    datanode_status = {
        "live_nodes": 0,
        "dead_nodes": 0,
        "decommissioning_nodes": 0,
    }

    try:
        messages_total, partitions = _collect_kafka_totals()
    except Exception as exc:
        errors.append(f"Kafka metrics error: {exc}")

    with _state_lock:
        prev_ts = _last_sample["timestamp"]
        prev_total = _last_sample["messages_total"]
        if prev_ts is not None and prev_total is not None:
            dt = max((now - prev_ts).total_seconds(), 1e-6)
            throughput_mps = max((messages_total - prev_total) / dt, 0.0)
        _last_sample["timestamp"] = now
        _last_sample["messages_total"] = messages_total
        _last_sample["throughput_mps"] = throughput_mps

    try:
        hdfs_files = _walk_hdfs(HDFS_OUTPUT_PATH)
        parquet_files = [f for f in hdfs_files if f["path"].endswith(".parquet")]
        hdfs_total_bytes = sum(f["length"] for f in parquet_files)
        if parquet_files:
            latest_mod_ms = max(f["modificationTime"] for f in parquet_files)
            hdfs_last_modified = datetime.fromtimestamp(latest_mod_ms / 1000, tz=timezone.utc)
        else:
            latest_mod_ms = 0
    except Exception as exc:
        parquet_files = []
        latest_mod_ms = 0
        errors.append(f"HDFS metrics error: {exc}")

    try:
        datanode_status = _collect_datanode_status()
    except Exception as exc:
        errors.append(f"DataNode status error: {exc}")

    persisted = (len(parquet_files) > 0) and (datanode_status.get("live_nodes", 0) > 0)

    payload = {
        "polled_at_utc": now.isoformat(),
        "kafka": {
            "topic": KAFKA_TOPIC,
            "messages_total": messages_total,
            "partitions": partitions,
            "throughput_mps": round(throughput_mps, 3),
        },
        "hdfs": {
            "path": HDFS_OUTPUT_PATH,
            "parquet_files": len(parquet_files),
            "total_size_bytes": int(hdfs_total_bytes),
            "last_modified_utc": hdfs_last_modified.isoformat() if hdfs_last_modified else None,
            "latest_modification_ms": latest_mod_ms,
        },
        "datanode": datanode_status,
        "persisted_to_datanode": persisted,
        "errors": errors,
    }
    return payload


@app.route("/")
def index():
    return render_template_string(HTML_TEMPLATE)


@app.route("/api/metrics")
def api_metrics():
    return jsonify(collect_metrics())


@app.route("/healthz")
def healthz():
    return jsonify({"status": "ok"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8501, debug=False)
