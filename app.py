import os
import sqlite3
import threading
import time
import uuid
import signal
import json
from datetime import datetime, timezone
from contextlib import closing

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, Field
import uvicorn

DB_PATH = os.getenv("NEXUS_DB", "nexus.db")
MAX_JOB_ATTEMPTS = 3
MAX_WORKER_RESTARTS = 3
BACKOFF_BASE = 1

app = FastAPI(title="NEXUS Reliability Platform")

db_lock = threading.Lock()
stop_event = threading.Event()
worker_threads = {}
worker_modes = {}
worker_restart_counts = {}
worker_last_error = {}
worker_lock = threading.Lock()


def now():
    return datetime.now(timezone.utc).isoformat()


def get_db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with db_lock:
        conn = get_db()
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS jobs (
            id TEXT PRIMARY KEY,
            job_type TEXT NOT NULL,
            payload TEXT NOT NULL,
            status TEXT NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0,
            worker_id TEXT,
            error TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            completed_at TEXT
        );

        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            event_type TEXT NOT NULL,
            job_id TEXT,
            worker_id TEXT,
            message TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS processed_jobs (
            job_id TEXT PRIMARY KEY,
            processed_at TEXT NOT NULL
        );
        """)
        conn.commit()
        conn.close()


def event(event_type, message, job_id=None, worker_id=None):
    with db_lock:
        conn = get_db()
        conn.execute(
            "INSERT INTO events(timestamp,event_type,job_id,worker_id,message) VALUES(?,?,?,?,?)",
            (now(), event_type, job_id, worker_id, message),
        )
        conn.commit()
        conn.close()


def update_job(job_id, **fields):
    if not fields:
        return
    fields["updated_at"] = now()
    set_clause = ", ".join(f"{k}=?" for k in fields)
    values = list(fields.values()) + [job_id]
    with db_lock:
        conn = get_db()
        conn.execute(f"UPDATE jobs SET {set_clause} WHERE id=?", values)
        conn.commit()
        conn.close()


def claim_next_job(worker_id):
    with db_lock:
        conn = get_db()
        row = conn.execute(
            "SELECT * FROM jobs WHERE status='PENDING' AND attempts < ? "
            "AND (updated_at IS NULL OR updated_at <= ?) "
            "ORDER BY created_at LIMIT 1",
            (MAX_JOB_ATTEMPTS, now()),
        ).fetchone()
        if not row:
            conn.close()
            return None

        new_attempts = row["attempts"] + 1
        conn.execute(
            "UPDATE jobs SET status='PROCESSING', attempts=?, worker_id=?, updated_at=? WHERE id=? AND status='PENDING'",
            (new_attempts, worker_id, now(), row["id"]),
        )
        conn.commit()
        conn.close()
        return dict(row, attempts=new_attempts, status="PROCESSING", worker_id=worker_id)


def mark_completed(job_id, worker_id):
    with db_lock:
        conn = get_db()
        # Idempotency record: only first completion is accepted.
        existing = conn.execute(
            "SELECT job_id FROM processed_jobs WHERE job_id=?", (job_id,)
        ).fetchone()
        if existing:
            conn.close()
            event("DUPLICATE_IGNORED", "Duplicate completion ignored", job_id, worker_id)
            update_job(job_id, status="COMPLETED", worker_id=worker_id)
            return False

        conn.execute(
            "INSERT INTO processed_jobs(job_id, processed_at) VALUES(?,?)",
            (job_id, now()),
        )
        conn.execute(
            "UPDATE jobs SET status='COMPLETED', completed_at=?, updated_at=?, error=NULL WHERE id=?",
            (now(), now(), job_id),
        )
        conn.commit()
        conn.close()
    event("JOB_COMPLETED", "Job completed successfully", job_id, worker_id)
    return True


def mark_failed_or_retry(job_id, worker_id, error):
    with db_lock:
        conn = get_db()
        row = conn.execute("SELECT attempts FROM jobs WHERE id=?", (job_id,)).fetchone()
        if not row:
            conn.close()
            return

        attempts = row["attempts"]
        if attempts >= MAX_JOB_ATTEMPTS:
            conn.execute(
                "UPDATE jobs SET status='DEAD_LETTER', error=?, updated_at=? WHERE id=?",
                (error, now(), job_id),
            )
            conn.commit()
            conn.close()
            event("JOB_DEAD_LETTER", f"Retry limit reached: {error}", job_id, worker_id)
            return

        delay = BACKOFF_BASE * (2 ** (attempts - 1))
        # updated_at is used as the next eligible time.
        from datetime import datetime, timedelta
        retry_at = (datetime.now(timezone.utc) + timedelta(seconds=delay)).isoformat()
        conn.execute(
            "UPDATE jobs SET status='PENDING', error=?, updated_at=?, worker_id=NULL WHERE id=?",
            (error, retry_at, job_id),
        )
        conn.commit()
        conn.close()
    event(
        "RETRY_SCHEDULED",
        f"Retry {attempts + 1}/{MAX_JOB_ATTEMPTS} scheduled after {delay}s",
        job_id,
        worker_id,
    )


def process_job(job, worker_id):
    mode = worker_modes.get(worker_id, "normal")

    if mode == "slow":
        time.sleep(5)

    if mode in ("crash", "always_crash"):
        raise RuntimeError("Simulated worker crash")

    # Simulated business work.
    time.sleep(0.25)


def worker_loop(worker_id):
    event("WORKER_STARTED", "Worker started", worker_id=worker_id)
    while not stop_event.is_set():
        mode = worker_modes.get(worker_id, "normal")

        if mode == "crash":
            # Crash once, then return to normal so recovery can be demonstrated.
            worker_modes[worker_id] = "normal"
            event("WORKER_CRASHED", "Simulated crash", worker_id=worker_id)
            raise RuntimeError("Simulated worker crash")

        if mode == "always_crash":
            event("WORKER_CRASHED", "Simulated persistent crash", worker_id=worker_id)
            raise RuntimeError("Simulated persistent worker crash")

        job = claim_next_job(worker_id)
        if not job:
            time.sleep(0.2)
            continue

        event("JOB_ASSIGNED", "Job assigned to worker", job["id"], worker_id)

        try:
            process_job(job, worker_id)
            mark_completed(job["id"], worker_id)
        except Exception as exc:
            error = str(exc)
            event("JOB_FAILED", error, job["id"], worker_id)
            mark_failed_or_retry(job["id"], worker_id, error)


def worker_supervisor(worker_id):
    while not stop_event.is_set():
        try:
            worker_loop(worker_id)
            return
        except Exception as exc:
            with worker_lock:
                worker_restart_counts[worker_id] = worker_restart_counts.get(worker_id, 0) + 1
                count = worker_restart_counts[worker_id]
                worker_last_error[worker_id] = str(exc)

            event("WORKER_CRASHED", str(exc), worker_id=worker_id)

            if count >= MAX_WORKER_RESTARTS:
                worker_modes[worker_id] = "out_of_service"
                event(
                    "WORKER_OUT_OF_SERVICE",
                    f"Restart limit reached ({MAX_WORKER_RESTARTS})",
                    worker_id=worker_id,
                )
                return

            delay = 2 ** (count - 1)
            event(
                "WORKER_RESTART_SCHEDULED",
                f"Restart {count}/{MAX_WORKER_RESTARTS} scheduled after {delay}s",
                worker_id=worker_id,
            )
            time.sleep(delay)
            event("WORKER_RESTARTED", f"Worker restarted (attempt {count})", worker_id=worker_id)


def start_worker(worker_id):
    with worker_lock:
        if worker_id in worker_threads and worker_threads[worker_id].is_alive():
            return
        if worker_modes.get(worker_id) == "out_of_service":
            return
        worker_threads[worker_id] = threading.Thread(
            target=worker_supervisor, args=(worker_id,), daemon=True
        )
        worker_threads[worker_id].start()


def seed_workers():
    for wid in ("worker-1", "worker-2"):
        worker_modes[wid] = "normal"
        worker_restart_counts[wid] = 0
        start_worker(wid)


class JobRequest(BaseModel):
    id: str | None = Field(default=None)
    job_type: str = "demo"
    payload: dict = Field(default_factory=dict)


class ModeRequest(BaseModel):
    mode: str


@app.on_event("startup")
def startup():
    init_db()
    # Recover jobs that were processing when NEXUS stopped.
    with db_lock:
        conn = get_db()
        rows = conn.execute(
            "SELECT id FROM jobs WHERE status='PROCESSING'"
        ).fetchall()
        for row in rows:
            conn.execute(
                "UPDATE jobs SET status='PENDING', worker_id=NULL, updated_at=? WHERE id=?",
                (now(), row["id"]),
            )
        conn.commit()
        conn.close()
    seed_workers()
    event("NEXUS_STARTED", "NEXUS started and recovered persisted work")


@app.on_event("shutdown")
def shutdown():
    stop_event.set()
    event("NEXUS_STOPPING", "NEXUS shutting down")


@app.get("/", response_class=HTMLResponse)
def dashboard():
    return HTMLResponse(DASHBOARD_HTML)


@app.post("/jobs")
def create_job(req: JobRequest):
    job_id = req.id or str(uuid.uuid4())

    with db_lock:
        conn = get_db()
        existing = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        if existing:
            conn.close()
            event("DUPLICATE_SUBMISSION", "Duplicate job submission detected", job_id)
            return JSONResponse(
                status_code=200,
                content={"job_id": job_id, "status": existing["status"], "duplicate": True},
            )

        timestamp = now()
        conn.execute(
            "INSERT INTO jobs(id,job_type,payload,status,attempts,created_at,updated_at) VALUES(?,?,?,?,?,?,?)",
            (job_id, req.job_type, json.dumps(req.payload), "PENDING", 0, timestamp, timestamp),
        )
        conn.commit()
        conn.close()

    event("JOB_ACCEPTED", "Job persisted and accepted", job_id)
    return {"job_id": job_id, "status": "PENDING", "duplicate": False}


@app.post("/jobs/{job_id}/duplicate")
def duplicate_job(job_id: str):
    with db_lock:
        conn = get_db()
        row = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        conn.close()
    if not row:
        raise HTTPException(404, "Job not found")
    event("DUPLICATE_SUBMISSION", "Simulated duplicate delivery", job_id)
    return {"job_id": job_id, "status": row["status"], "duplicate": True}


@app.post("/workers/{worker_id}/mode")
def set_worker_mode(worker_id: str, req: ModeRequest):
    allowed = {"normal", "slow", "crash", "always_crash"}
    if req.mode not in allowed:
        raise HTTPException(400, f"Mode must be one of {sorted(allowed)}")
    if worker_id not in worker_modes:
        raise HTTPException(404, "Unknown worker")

    worker_modes[worker_id] = req.mode
    event("WORKER_MODE_CHANGED", f"Worker mode set to {req.mode}", worker_id=worker_id)

    if req.mode != "always_crash":
        start_worker(worker_id)

    return {"worker_id": worker_id, "mode": req.mode}


@app.post("/workers/{worker_id}/restart")
def restart_worker(worker_id: str):
    if worker_id not in worker_modes:
        raise HTTPException(404, "Unknown worker")
    worker_modes[worker_id] = "normal"
    worker_restart_counts[worker_id] = 0
    event("WORKER_MANUAL_RESTART", "Operator requested worker restart", worker_id=worker_id)
    start_worker(worker_id)
    return {"worker_id": worker_id, "status": "restart_requested"}


def calculate_health(counts, workers, events):
    active_incidents = []
    out_workers = [w for w in workers if w["status"] == "OUT_OF_SERVICE"]
    dead_jobs = counts.get("DEAD_LETTER", 0)
    pending = counts.get("PENDING", 0)

    for w in out_workers:
        active_incidents.append({
            "severity": "critical",
            "title": f"{w['id']} is OUT_OF_SERVICE",
            "detail": f"Restart budget exhausted ({w['restart_count']}/{MAX_WORKER_RESTARTS}). Last error: {w.get('last_error') or 'unknown'}"
        })

    if dead_jobs:
        active_incidents.append({
            "severity": "warning",
            "title": f"{dead_jobs} job(s) in DEAD_LETTER",
            "detail": "Jobs exhausted their retry budget and require investigation."
        })

    if pending >= 10:
        active_incidents.append({
            "severity": "warning",
            "title": f"Backlog is elevated ({pending} pending)",
            "detail": "There are many jobs waiting for available worker capacity."
        })

    if out_workers:
        overall = "CRITICAL"
    elif active_incidents:
        overall = "DEGRADED"
    else:
        overall = "HEALTHY"

    return {"overall": overall, "active_incidents": active_incidents}


@app.get("/api/state")
def state():
    with db_lock:
        conn = get_db()
        counts = {}
        for row in conn.execute("SELECT status, COUNT(*) c FROM jobs GROUP BY status"):
            counts[row["status"]] = row["c"]

        jobs = [
            dict(row)
            for row in conn.execute(
                "SELECT id,status,attempts,worker_id,error,created_at,updated_at,completed_at "
                "FROM jobs ORDER BY created_at DESC LIMIT 30"
            )
        ]
        events = [
            dict(row)
            for row in conn.execute(
                "SELECT * FROM events ORDER BY id DESC LIMIT 30"
            )
        ]
        conn.close()

    workers = []
    with worker_lock:
        for wid in sorted(worker_modes):
            thread = worker_threads.get(wid)
            mode = worker_modes.get(wid)
            alive = bool(thread and thread.is_alive())
            if mode == "out_of_service":
                status = "OUT_OF_SERVICE"
            elif alive:
                status = "RUNNING"
            else:
                status = "STOPPED"
            workers.append({
                "id": wid,
                "status": status,
                "mode": mode,
                "restart_count": worker_restart_counts.get(wid, 0),
                "last_error": worker_last_error.get(wid),
            })

    health = calculate_health(counts, workers, events)
    return {"counts": counts, "workers": workers, "jobs": jobs, "events": events, "health": health}


DASHBOARD_HTML = r"""
<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>NEXUS Operator Dashboard</title>
<style>
body{font-family:Arial,sans-serif;margin:0;background:#f4f6f8;color:#1f2937}
header{background:#111827;color:white;padding:18px 28px}
main{max-width:1200px;margin:20px auto;padding:0 16px}
.grid{display:grid;grid-template-columns:repeat(4,1fr);gap:12px}
.card{background:white;border-radius:10px;padding:16px;box-shadow:0 1px 4px #0001}
.big{font-size:28px;font-weight:bold}
table{width:100%;border-collapse:collapse;background:white}
th,td{padding:9px;border-bottom:1px solid #eee;text-align:left;font-size:13px}
button,select{padding:8px 10px;border:1px solid #ccc;border-radius:6px;background:white;cursor:pointer}
section{margin:18px 0}
.ok{color:#16803c}.bad{color:#c62828}.warn{color:#a16207}
.controls{display:flex;gap:8px;flex-wrap:wrap}
small{color:#6b7280}
.incident{border-left:5px solid #d1d5db;padding:10px 12px;margin:8px 0;background:#fafafa;border-radius:6px}
.incident.critical{border-left-color:#dc2626}.incident.warning{border-left-color:#d97706}
.health{font-size:22px;font-weight:bold}.health.healthy{color:#16803c}.health.degraded{color:#a16207}.health.critical{color:#c62828}

</style>
</head>
<body>
<header><h1>NEXUS Operator Dashboard</h1><div>Local reliability platform — job processing, recovery and auditability</div></header>
<main>
<section class="grid" id="summary"></section>

<section class="card">
<h2>System Health & Incidents</h2>
<div id="health" class="health">Loading...</div>
<div id="incidents"></div>
</section>

<section class="card">
<h2>Failure Simulation</h2>

<div class="controls">
    <!-- Worker 1 -->
    <button onclick="setMode('worker-1','crash')">Crash Worker 1</button>
    <button onclick="setMode('worker-1','slow')">Slow Worker 1</button>
    <button onclick="setMode('worker-1','always_crash')">Always Crash Worker 1</button>
    <button onclick="setMode('worker-1','normal')">Recover Worker 1</button>

    <!-- Worker 2 -->
    <button onclick="setMode('worker-2','crash')">Crash Worker 2</button>
    <button onclick="setMode('worker-2','slow')">Slow Worker 2</button>
    <button onclick="setMode('worker-2','always_crash')">Always Crash Worker 2</button>
    <button onclick="setMode('worker-2','normal')">Recover Worker 2</button>

    <!-- Job controls -->
    <button onclick="createJob()">Create Demo Job</button>
    <button onclick="duplicateLatest()">Duplicate Latest Job</button>
</div>

<p>
    <small>
        Use these controls to deliberately break the system and observe recovery.
    </small>
</p>
</section>

<section class="card">
<h2>Workers</h2>
<table><thead><tr><th>Worker</th><th>Status</th><th>Mode</th><th>Restarts</th><th>Last error</th></tr></thead>
<tbody id="workers"></tbody></table>
</section>

<section class="card">
<h2>Recent Jobs</h2>
<table><thead><tr><th>ID</th><th>Status</th><th>Attempts</th><th>Worker</th><th>Error</th></tr></thead>
<tbody id="jobs"></tbody></table>
</section>

<section class="card">
<h2>Recent Events</h2>
<table><thead><tr><th>Time</th><th>Type</th><th>Job</th><th>Worker</th><th>Message</th></tr></thead>
<tbody id="events"></tbody></table>
</section>
</main>

<script>
let latestJob = null;
async function api(url, options={}) {
  const r = await fetch(url, {headers:{'Content-Type':'application/json'}, ...options});
  return r.json();
}
async function createJob() {
  const id = "demo-" + Date.now();
  const data = await api("/jobs",{method:"POST",body:JSON.stringify({
    id, job_type:"demo", payload:{message:"demo job"}
  })});
  latestJob = data.job_id;
  refresh();
}
async function duplicateLatest() {
  if (!latestJob) return alert("Create a job first.");
  await api("/jobs/" + encodeURIComponent(latestJob) + "/duplicate",{method:"POST"});
  refresh();
}
async function setMode(worker, mode) {
  await api("/workers/"+worker+"/mode",{method:"POST",body:JSON.stringify({mode})});
  refresh();
}
function esc(s){return String(s??"").replaceAll("&","&amp;").replaceAll("<","&lt;").replaceAll(">","&gt;")}
async function refresh() {
  const d = await api("/api/state");
  const c = d.counts || {};
  document.getElementById("summary").innerHTML = `
    <div class="card"><small>Pending</small><div class="big">${c.PENDING||0}</div></div>
    <div class="card"><small>Processing</small><div class="big">${c.PROCESSING||0}</div></div>
    <div class="card"><small>Completed</small><div class="big ok">${c.COMPLETED||0}</div></div>
    <div class="card"><small>Dead Letter</small><div class="big bad">${c.DEAD_LETTER||0}</div></div>`;
  const health = d.health || {overall:"HEALTHY", active_incidents:[]};
  document.getElementById("health").className = "health " + health.overall.toLowerCase();
  document.getElementById("health").textContent = health.overall === "HEALTHY" ? "✓ SYSTEM HEALTHY" : (health.overall === "CRITICAL" ? "✕ SYSTEM CRITICAL" : "⚠ SYSTEM DEGRADED");
  document.getElementById("incidents").innerHTML = health.active_incidents.length ? health.active_incidents.map(i=>`
    <div class="incident ${esc(i.severity)}"><strong>${esc(i.title)}</strong><br><small>${esc(i.detail)}</small></div>`).join("") : `<div class="incident"><strong>No active incidents</strong><br><small>Workers are operating within configured limits and no jobs are in dead-letter.</small></div>`;
  document.getElementById("workers").innerHTML = d.workers.map(w=>`
    <tr><td>${esc(w.id)}</td><td class="${w.status==='RUNNING'?'ok':'bad'}">${esc(w.status)}</td>
    <td>${esc(w.mode)}</td><td>${w.restart_count}</td><td>${esc(w.last_error)}</td></tr>`).join("");
  document.getElementById("jobs").innerHTML = d.jobs.map(j=>`
    <tr><td>${esc(j.id)}</td><td>${esc(j.status)}</td><td>${j.attempts}</td>
    <td>${esc(j.worker_id)}</td><td>${esc(j.error)}</td></tr>`).join("");
  document.getElementById("events").innerHTML = d.events.map(e=>`
    <tr><td>${esc(e.timestamp)}</td><td>${esc(e.event_type)}</td>
    <td>${esc(e.job_id)}</td><td>${esc(e.worker_id)}</td><td>${esc(e.message)}</td></tr>`).join("");
}
setInterval(refresh,1000);
refresh();
</script>
</body>
</html>
"""

if __name__ == "__main__":
    uvicorn.run("app:app", host="127.0.0.1", port=8000, reload=False)
