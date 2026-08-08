import os
import sqlite3
import threading
import time
import uuid
import signal
import json
from datetime import datetime, timezone, timedelta
from contextlib import closing

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, Field
import uvicorn

DB_PATH = os.getenv("NEXUS_DB", "nexus.db")
MAX_JOB_ATTEMPTS = 3
MAX_WORKER_RESTARTS = int(os.getenv("NEXUS_MAX_WORKER_RESTARTS", "3"))
WORKER_RESTART_WINDOW_SECONDS = int(os.getenv("NEXUS_WORKER_RESTART_WINDOW", "60"))
WORKER_RESTART_BACKOFF_BASE = int(os.getenv("NEXUS_WORKER_RESTART_BACKOFF_BASE", "2"))
WORKER_SETTLING_SECONDS = int(os.getenv("NEXUS_WORKER_SETTLING_SECONDS", "10"))
WORKER_RECOVERY_JOB_GAP_SECONDS = float(os.getenv("NEXUS_RECOVERY_JOB_GAP", "1.0"))
BACKOFF_BASE = 1
BACKLOG_SAMPLE_SECONDS = int(os.getenv("NEXUS_BACKLOG_SAMPLE_SECONDS", "5"))
BACKLOG_GROWTH_WINDOW_SECONDS = int(os.getenv("NEXUS_BACKLOG_GROWTH_WINDOW", "60"))
EVENT_RETENTION_DAYS = int(os.getenv("NEXUS_EVENT_RETENTION_DAYS", "7"))
IDEMPOTENCY_RETENTION_HOURS = int(os.getenv("NEXUS_IDEMPOTENCY_RETENTION_HOURS", "24"))
CACHE_DEFAULT_MAX_AGE_SECONDS = int(os.getenv("NEXUS_CACHE_DEFAULT_MAX_AGE", "30"))
RECONCILIATION_DEFAULT_INTERVAL_SECONDS = int(os.getenv("NEXUS_RECONCILIATION_INTERVAL", "10"))
RECONCILIATION_ESCALATION_AFTER = int(os.getenv("NEXUS_RECONCILIATION_ESCALATION_AFTER", "3"))

app = FastAPI(title="NEXUS Reliability Platform")

db_lock = threading.Lock()
stop_event = threading.Event()
worker_threads = {}
worker_modes = {}
worker_restart_counts = {}
worker_last_error = {}
worker_failure_times = {}
worker_healthy_since = {}
worker_recovery_state = {}
worker_last_job_at = {}
worker_lock = threading.Lock()
background_threads = []


def now():
    return datetime.now(timezone.utc).isoformat()


def get_db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("PRAGMA foreign_keys=ON")
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
            message TEXT NOT NULL,
            subject_type TEXT,
            subject_id TEXT
        );

        CREATE TABLE IF NOT EXISTS processed_jobs (
            job_id TEXT PRIMARY KEY,
            processed_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS backlog_samples (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sampled_at TEXT NOT NULL,
            pending_count INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS worker_attempts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            worker_id TEXT NOT NULL,
            attempted_at TEXT NOT NULL,
            attempt_number INTEGER NOT NULL,
            delay_seconds REAL NOT NULL,
            outcome TEXT NOT NULL,
            error TEXT
        );

        CREATE TABLE IF NOT EXISTS audit_actions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            subject_type TEXT NOT NULL,
            subject_id TEXT NOT NULL,
            action TEXT NOT NULL,
            before_state TEXT,
            after_state TEXT,
            belief TEXT,
            reason TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS facts (
            subject_id TEXT NOT NULL,
            fact_key TEXT NOT NULL,
            owner TEXT NOT NULL,
            authoritative_value TEXT NOT NULL,
            authoritative_updated_at TEXT NOT NULL,
            copy_value TEXT NOT NULL,
            copy_updated_at TEXT NOT NULL,
            compare_interval_seconds INTEGER NOT NULL,
            disagreement_count INTEGER NOT NULL DEFAULT 0,
            first_disagreement_at TEXT,
            last_compared_at TEXT,
            status TEXT NOT NULL DEFAULT 'MATCH',
            PRIMARY KEY(subject_id, fact_key)
        );

        CREATE TABLE IF NOT EXISTS cache_entries (
            cache_key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            source_updated_at TEXT NOT NULL,
            cached_at TEXT NOT NULL,
            max_age_seconds INTEGER NOT NULL,
            stale_served INTEGER NOT NULL DEFAULT 0,
            refresh_in_progress INTEGER NOT NULL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS source_facts (
            source_key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            reachable INTEGER NOT NULL DEFAULT 1
        );
        """)
        # Lightweight migrations for existing nexus.db files.
        existing_events=[r["name"] for r in conn.execute("PRAGMA table_info(events)").fetchall()]
        for col,typ in (("subject_type","TEXT"),("subject_id","TEXT")):
            if col not in existing_events: conn.execute(f"ALTER TABLE events ADD COLUMN {col} {typ}")
        existing_releases=[r["name"] for r in conn.execute("PRAGMA table_info(releases)").fetchall()] if conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='releases'").fetchone() else []
        for col,typ in (("baseline_pending","INTEGER DEFAULT 0"),("baseline_dead_letter","INTEGER DEFAULT 0"),("baseline_completed","INTEGER DEFAULT 0"),("baseline_failed_events","INTEGER DEFAULT 0")):
            if col not in existing_releases and existing_releases: conn.execute(f"ALTER TABLE releases ADD COLUMN {col} {typ}")
        conn.commit()
        conn.close()


def event(event_type, message, job_id=None, worker_id=None, subject_type=None, subject_id=None):
    with db_lock:
        conn = get_db()
        conn.execute(
            "INSERT INTO events(timestamp,event_type,job_id,worker_id,message,subject_type,subject_id) VALUES(?,?,?,?,?,?,?)",
            (now(), event_type, job_id, worker_id, message, subject_type, subject_id),
        )
        conn.commit()
        conn.close()


def audit_action(subject_type, subject_id, action, before_state=None, after_state=None, belief=None, reason=""):
    with db_lock:
        conn = get_db()
        conn.execute(
            "INSERT INTO audit_actions(timestamp,subject_type,subject_id,action,before_state,after_state,belief,reason) VALUES(?,?,?,?,?,?,?,?)",
            (now(), subject_type, subject_id, action,
             json.dumps(before_state) if isinstance(before_state, (dict, list)) else before_state,
             json.dumps(after_state) if isinstance(after_state, (dict, list)) else after_state,
             json.dumps(belief) if isinstance(belief, (dict, list)) else belief,
             reason),
        )
        conn.commit()
        conn.close()
    event("AUDIT_ACTION", f"{action}: {reason}")


def cleanup_retention():
    cutoff = (datetime.now(timezone.utc).timestamp() - EVENT_RETENTION_DAYS * 86400)
    idem_cutoff = (datetime.now(timezone.utc).timestamp() - IDEMPOTENCY_RETENTION_HOURS * 3600)
    cutoff_iso = datetime.fromtimestamp(cutoff, timezone.utc).isoformat()
    idem_iso = datetime.fromtimestamp(idem_cutoff, timezone.utc).isoformat()
    with db_lock:
        conn = get_db()
        conn.execute("DELETE FROM events WHERE timestamp < ?", (cutoff_iso,))
        conn.execute("DELETE FROM audit_actions WHERE timestamp < ?", (cutoff_iso,))
        conn.execute("DELETE FROM backlog_samples WHERE sampled_at < ?", (cutoff_iso,))
        conn.execute("DELETE FROM processed_jobs WHERE processed_at < ?", (idem_iso,))
        conn.commit()
        conn.close()


def record_backlog_sample(force=False):
    with db_lock:
        conn = get_db()
        last = conn.execute("SELECT sampled_at FROM backlog_samples ORDER BY id DESC LIMIT 1").fetchone()
        current = datetime.now(timezone.utc)
        if last and not force:
            try:
                age = (current - datetime.fromisoformat(last["sampled_at"])).total_seconds()
                if age < BACKLOG_SAMPLE_SECONDS:
                    conn.close(); return
            except Exception:
                pass
        pending = conn.execute("SELECT COUNT(*) c FROM jobs WHERE status='PENDING'").fetchone()["c"]
        conn.execute("INSERT INTO backlog_samples(sampled_at,pending_count) VALUES(?,?)", (current.isoformat(), pending))
        conn.commit(); conn.close()


def backlog_metrics():
    with db_lock:
        conn=get_db()
        row=conn.execute("SELECT COUNT(*) c, MIN(created_at) oldest, AVG((julianday('now')-julianday(created_at))*86400.0) avg_age FROM jobs WHERE status='PENDING'").fetchone()
        current=row["c"] or 0
        oldest_age=0
        if row["oldest"]:
            try: oldest_age=max(0,(datetime.now(timezone.utc)-datetime.fromisoformat(row["oldest"])).total_seconds())
            except Exception: pass
        start=(datetime.now(timezone.utc)-timedelta(seconds=BACKLOG_GROWTH_WINDOW_SECONDS)).isoformat()
        old=conn.execute("SELECT pending_count FROM backlog_samples WHERE sampled_at<=? ORDER BY sampled_at DESC LIMIT 1",(start,)).fetchone()
        recent=conn.execute("SELECT pending_count FROM backlog_samples ORDER BY sampled_at DESC LIMIT 1").fetchone()
        growth=None
        if old and recent:
            growth=(recent["pending_count"]-old["pending_count"])/(BACKLOG_GROWTH_WINDOW_SECONDS/60)
        conn.close()
    return {"count":current,"oldest_age_seconds":round(oldest_age,2),"average_age_seconds":round(float(row["avg_age"] or 0),2),"growth_per_minute":round(growth,2) if growth is not None else None,"growth_window_seconds":BACKLOG_GROWTH_WINDOW_SECONDS}


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


def _prune_worker_failures(worker_id):
    cutoff = time.time() - WORKER_RESTART_WINDOW_SECONDS
    worker_failure_times[worker_id] = [t for t in worker_failure_times.get(worker_id, []) if t >= cutoff]
    return worker_failure_times[worker_id]


def _worker_restart_attempt(worker_id, error):
    cutoff = (datetime.now(timezone.utc) - timedelta(seconds=WORKER_RESTART_WINDOW_SECONDS)).isoformat()
    with db_lock:
        conn=get_db()
        count = conn.execute("SELECT COUNT(*) c FROM worker_attempts WHERE worker_id=? AND attempted_at>=?",(worker_id,cutoff)).fetchone()["c"] + 1
        conn.close()
    with worker_lock:
        failures = _prune_worker_failures(worker_id)
        failures.append(time.time())
        worker_restart_counts[worker_id] = count
        worker_last_error[worker_id] = str(error)
        worker_recovery_state[worker_id] = "FAILED"
    return count


def _record_worker_attempt(worker_id, attempt, delay, outcome, error=None):
    with db_lock:
        conn=get_db()
        conn.execute("INSERT INTO worker_attempts(worker_id,attempted_at,attempt_number,delay_seconds,outcome,error) VALUES(?,?,?,?,?,?)",(worker_id,now(),attempt,delay,outcome,error))
        conn.commit(); conn.close()
    audit_action("worker",worker_id,"RESTART_ATTEMPT",before_state=None,after_state={"attempt":attempt,"delay_seconds":delay,"outcome":outcome},belief={"restart_budget":MAX_WORKER_RESTARTS,"window_seconds":WORKER_RESTART_WINDOW_SECONDS},reason=f"Worker restart attempt {attempt}; outcome={outcome}")


def worker_loop(worker_id):
    event("WORKER_STARTED", "Worker started", worker_id=worker_id)
    with worker_lock:
        worker_recovery_state[worker_id] = "RECOVERING"
        worker_healthy_since[worker_id] = time.time()
    while not stop_event.is_set():
        mode = worker_modes.get(worker_id, "normal")
        if mode == "out_of_service":
            return
        if mode == "crash":
            worker_modes[worker_id] = "normal"
            event("WORKER_CRASHED", "Simulated crash", worker_id=worker_id)
            raise RuntimeError("Simulated worker crash")
        if mode == "always_crash":
            event("WORKER_CRASHED", "Simulated persistent crash", worker_id=worker_id)
            raise RuntimeError("Simulated persistent worker crash")

        with worker_lock:
            if worker_recovery_state.get(worker_id) == "RECOVERING" and time.time()-worker_healthy_since.get(worker_id,time.time()) >= WORKER_SETTLING_SECONDS:
                worker_recovery_state[worker_id] = "RECOVERED"
                worker_restart_counts[worker_id] = 0
                worker_failure_times[worker_id] = []
                event("WORKER_RECOVERED", f"Healthy for settling period ({WORKER_SETTLING_SECONDS}s); restart budget reset", worker_id=worker_id)
                audit_action("worker",worker_id,"BUDGET_RESET",after_state={"restart_count":0},reason="Worker earned recovery by staying healthy through settling period")

        job = claim_next_job(worker_id)
        if not job:
            time.sleep(0.2); continue
        event("JOB_ASSIGNED", "Job assigned to worker", job["id"], worker_id)
        try:
            if worker_recovery_state.get(worker_id) == "RECOVERING":
                time.sleep(WORKER_RECOVERY_JOB_GAP_SECONDS)
            process_job(job, worker_id)
            mark_completed(job["id"], worker_id)
            worker_last_job_at[worker_id] = time.time()
        except Exception as exc:
            error=str(exc); event("JOB_FAILED",error,job["id"],worker_id); mark_failed_or_retry(job["id"],worker_id,error)


def worker_supervisor(worker_id):
    while not stop_event.is_set():
        try:
            worker_loop(worker_id); return
        except Exception as exc:
            count=_worker_restart_attempt(worker_id,exc)
            event("WORKER_CRASHED",str(exc),worker_id=worker_id)
            if count >= MAX_WORKER_RESTARTS:
                _record_worker_attempt(worker_id,count,0,"BUDGET_EXHAUSTED",str(exc))
                with worker_lock: worker_modes[worker_id]="out_of_service"; worker_recovery_state[worker_id]="OUT_OF_SERVICE"
                event("WORKER_OUT_OF_SERVICE",f"Restart budget exhausted: {count}/{MAX_WORKER_RESTARTS} failures in {WORKER_RESTART_WINDOW_SECONDS}s",worker_id=worker_id)
                audit_action("worker",worker_id,"OUT_OF_SERVICE",after_state={"status":"OUT_OF_SERVICE","restart_count":count},belief={"failures_in_window":count},reason="Restart budget exhausted")
                return
            delay=WORKER_RESTART_BACKOFF_BASE ** (count-1)
            _record_worker_attempt(worker_id,count,delay,"SCHEDULED",str(exc))
            event("WORKER_RESTART_SCHEDULED",f"Restart {count}/{MAX_WORKER_RESTARTS} scheduled after {delay}s",worker_id=worker_id)
            time.sleep(delay)
            if stop_event.is_set(): return
            if count < MAX_WORKER_RESTARTS:
                event("WORKER_RESTARTED",f"Worker restart attempt {count}; settling period required",worker_id=worker_id)


def start_worker(worker_id):
    with worker_lock:
        if worker_id in worker_threads and worker_threads[worker_id].is_alive(): return False
        if worker_modes.get(worker_id) == "out_of_service": return False
        worker_threads[worker_id]=threading.Thread(target=worker_supervisor,args=(worker_id,),daemon=True)
        worker_threads[worker_id].start()
        return True


def seed_workers():
    for wid in ("worker-1","worker-2"):
        worker_modes[wid]="normal"; worker_restart_counts[wid]=0; worker_failure_times[wid]=[]; worker_recovery_state[wid]="RECOVERING"; worker_healthy_since[wid]=time.time(); start_worker(wid)


class JobRequest(BaseModel):
    id: str | None = Field(default=None)
    job_type: str = "demo"
    payload: dict = Field(default_factory=dict)


class ModeRequest(BaseModel):
    mode: str


@app.on_event("startup")
def startup():
    init_db()
    init_release_tables()
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
    cleanup_retention()
    record_backlog_sample(force=True)
    seed_workers()
    start_background_threads()
    event("NEXUS_STARTED", f"NEXUS started; event retention={EVENT_RETENTION_DAYS}d; idempotency retention={IDEMPOTENCY_RETENTION_HOURS}h")


@app.on_event("shutdown")
def shutdown():
    stop_event.set()
    event("NEXUS_STOPPING", "NEXUS shutting down")


# ---------------------------
# Release safety subsystem
# ---------------------------

RELEASE_BAKE_SECONDS = int(os.getenv("NEXUS_RELEASE_BAKE_SECONDS", "20"))

def init_release_tables():
    with db_lock:
        conn = get_db()
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS releases (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            version TEXT NOT NULL,
            previous_version TEXT,
            rollback_plan TEXT NOT NULL,
            rollback_ready INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL,
            allow_overlap INTEGER NOT NULL DEFAULT 0,
            overlap_reason TEXT,
            created_at TEXT NOT NULL,
            deployed_at TEXT,
            observation_ends_at TEXT,
            finalized_at TEXT,
            rolled_back_at TEXT,
            health_status TEXT,
            health_reason TEXT,
            baseline_pending INTEGER DEFAULT 0,
            baseline_dead_letter INTEGER DEFAULT 0,
            baseline_completed INTEGER DEFAULT 0,
            baseline_failed_events INTEGER DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS release_changes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            release_id TEXT NOT NULL,
            changed_at TEXT NOT NULL,
            change_type TEXT NOT NULL,
            details TEXT NOT NULL
        );
        """)
        existing=[r["name"] for r in conn.execute("PRAGMA table_info(releases)").fetchall()]
        for col,typ in (("baseline_pending","INTEGER DEFAULT 0"),("baseline_dead_letter","INTEGER DEFAULT 0"),("baseline_completed","INTEGER DEFAULT 0"),("baseline_failed_events","INTEGER DEFAULT 0")):
            if col not in existing: conn.execute(f"ALTER TABLE releases ADD COLUMN {col} {typ}")
        conn.commit()
        conn.close()


def release_row(release_id):
    with db_lock:
        conn = get_db()
        row = conn.execute("SELECT * FROM releases WHERE id=?", (release_id,)).fetchone()
        conn.close()
    return dict(row) if row else None


def release_change(release_id, change_type, details):
    with db_lock:
        conn = get_db()
        conn.execute(
            "INSERT INTO release_changes(release_id,changed_at,change_type,details) VALUES(?,?,?,?)",
            (release_id, now(), change_type, details),
        )
        conn.commit()
        conn.close()


def release_health(release):
    with db_lock:
        conn=get_db()
        counts={row["status"]:row["c"] for row in conn.execute("SELECT status,COUNT(*) c FROM jobs GROUP BY status")}
        failed=conn.execute("SELECT COUNT(*) c FROM events WHERE event_type='JOB_FAILED' AND timestamp>=?",(release.get("deployed_at") or now(),)).fetchone()["c"]
        conn.close()
    dead=counts.get("DEAD_LETTER",0); pending=counts.get("PENDING",0)
    baseline_dead=int(release.get("baseline_dead_letter") or 0); baseline_pending=int(release.get("baseline_pending") or 0)
    reasons=[]
    if dead>baseline_dead: reasons.append(f"dead-letter jobs increased from {baseline_dead} to {dead}")
    if pending>max(10,baseline_pending+5): reasons.append(f"pending backlog increased from {baseline_pending} to {pending}")
    if failed>0: reasons.append(f"{failed} job failure event(s) occurred during observation")
    if reasons: return "UNHEALTHY", "Behavior differs from pre-release baseline: " + "; ".join(reasons)
    return "HEALTHY", f"Behavior remains within pre-release baseline (pending={baseline_pending}, dead-letter={baseline_dead}, no new job failures)"


def active_releases():
    with db_lock:
        conn = get_db()
        rows = conn.execute(
            "SELECT * FROM releases WHERE status IN ('DEPLOYED','OBSERVING') ORDER BY created_at DESC"
        ).fetchall()
        conn.close()
    return [dict(r) for r in rows]


def conflicting_release_exists():
    return bool(active_releases())


def finalize_expired_releases():
    for release in active_releases():
        end = release.get("observation_ends_at")
        if not end:
            continue
        try:
            end_dt = datetime.fromisoformat(end)
        except ValueError:
            continue
        if datetime.now(timezone.utc) < end_dt:
            continue

        status, reason = release_health(release)

        with db_lock:
            conn = get_db()
            conn.execute(
                """UPDATE releases
                   SET status=?, health_status=?, health_reason=?, finalized_at=?
                   WHERE id=? AND status IN ('DEPLOYED','OBSERVING')""",
                (
                    "FINALIZED" if status == "HEALTHY" else "UNCERTAIN",
                    status,
                    reason,
                    now(),
                    release["id"],
                ),
            )
            conn.commit()
            conn.close()

        release_change(release["id"], "OBSERVATION_ENDED", f"{status}: {reason}")
        event(
            "RELEASE_FINALIZED" if status == "HEALTHY" else "RELEASE_UNCERTAIN",
            f"Release observation ended: {status} — {reason}",
        )


class ReleaseRequest(BaseModel):
    name: str
    version: str
    previous_version: str | None = None
    rollback_plan: str = ""
    allow_overlap: bool = False
    overlap_reason: str | None = None


class ReleaseActionRequest(BaseModel):
    reason: str | None = None


@app.on_event("startup")
def init_release_system():
    init_release_tables()


@app.post("/releases")
def create_release(req: ReleaseRequest):
    """
    Safety gate: a release cannot even be created unless the operator has
    supplied an explicit rollback plan.
    """
    finalize_expired_releases()

    rollback_plan = req.rollback_plan.strip()
    if not rollback_plan:
        raise HTTPException(
            400,
            "Release refused: a rollback plan is required before deployment.",
        )

    if req.allow_overlap and not (req.overlap_reason or "").strip():
        raise HTTPException(
            400,
            "Release refused: deliberate overlap requires a recorded reason.",
        )

    if conflicting_release_exists() and not req.allow_overlap:
        raise HTTPException(
            409,
            "Release refused: another release is currently deployed/observing. "
            "Explicit overlap is required.",
        )

    release_id = "rel-" + uuid.uuid4().hex[:12]
    timestamp = now()

    with db_lock:
        conn = get_db()
        conn.execute(
            """INSERT INTO releases
               (id,name,version,previous_version,rollback_plan,rollback_ready,
                status,allow_overlap,overlap_reason,created_at,
               baseline_pending,baseline_dead_letter,baseline_completed,baseline_failed_events)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                release_id, req.name, req.version, req.previous_version, rollback_plan, 1,
                "READY", 1 if req.allow_overlap else 0,
                req.overlap_reason.strip() if req.overlap_reason else None, timestamp,
                0, 0, 0, 0,
            ),
        )
        conn.commit()
        conn.close()

    release_change(release_id, "RELEASE_CREATED", "Rollback plan verified")
    event("RELEASE_CREATED",f"Release {release_id} created and rollback readiness verified",subject_type="release",subject_id=release_id)

    return {
        "release_id": release_id,
        "status": "READY",
        "rollback_ready": True,
        "message": "Release accepted for deployment.",
    }


@app.post("/releases/{release_id}/deploy")
def deploy_release(release_id: str):
    finalize_expired_releases()
    release = release_row(release_id)

    if not release:
        raise HTTPException(404, "Release not found")
    if not release["rollback_ready"] or not release["rollback_plan"].strip():
        raise HTTPException(
            400,
            "Release refused: rollback is not known.",
        )
    if release["status"] != "READY":
        raise HTTPException(409, f"Release cannot be deployed from {release['status']}")

    active = active_releases()
    if active and not release["allow_overlap"]:
        raise HTTPException(
            409,
            "Deployment refused: an overlapping release is already active.",
        )

    if active and release["allow_overlap"]:
        release_change(
            release_id,
            "OVERLAP_ALLOWED",
            release["overlap_reason"] or "No reason supplied",
        )
        event("RELEASE_OVERLAP_ALLOWED", f"Deliberate overlapping release: {release['overlap_reason']}", subject_type="release", subject_id=release_id)

    with db_lock:
        conn=get_db()
        baseline={r["status"]:r["c"] for r in conn.execute("SELECT status,COUNT(*) c FROM jobs GROUP BY status")}
        baseline_failed=conn.execute("SELECT COUNT(*) c FROM events WHERE event_type='JOB_FAILED'").fetchone()["c"]
        conn.execute("UPDATE releases SET baseline_pending=?,baseline_dead_letter=?,baseline_completed=?,baseline_failed_events=? WHERE id=?",(baseline.get("PENDING",0),baseline.get("DEAD_LETTER",0),baseline.get("COMPLETED",0),baseline_failed,release_id))
        conn.commit(); conn.close()
    deployed_at = now()
    end_dt = datetime.now(timezone.utc).timestamp() + RELEASE_BAKE_SECONDS
    observation_end = datetime.fromtimestamp(end_dt, timezone.utc).isoformat()

    with db_lock:
        conn = get_db()
        conn.execute(
            """UPDATE releases
               SET status='OBSERVING', deployed_at=?, observation_ends_at=?,
                   health_status='UNKNOWN', health_reason='Observation in progress'
               WHERE id=?""",
            (deployed_at, observation_end, release_id),
        )
        conn.commit()
        conn.close()

    release_change(
        release_id,
        "DEPLOYED",
        f"Deployment started; observation window is {RELEASE_BAKE_SECONDS}s",
    )
    event("RELEASE_DEPLOYED",f"Release {release_id} deployed; observing behavior until {observation_end}",subject_type="release",subject_id=release_id)

    return {
        "release_id": release_id,
        "status": "OBSERVING",
        "observation_ends_at": observation_end,
        "rollback_available": True,
    }


@app.get("/releases")
def list_releases():
    finalize_expired_releases()
    with db_lock:
        conn = get_db()
        releases = [
            dict(r)
            for r in conn.execute(
                "SELECT * FROM releases ORDER BY created_at DESC LIMIT 30"
            )
        ]
        changes = [
            dict(r)
            for r in conn.execute(
                "SELECT * FROM release_changes ORDER BY id DESC LIMIT 100"
            )
        ]
        conn.close()

    return {"releases": releases, "changes": changes}


@app.get("/releases/{release_id}")
def get_release(release_id: str):
    finalize_expired_releases()
    release = release_row(release_id)
    if not release:
        raise HTTPException(404, "Release not found")

    with db_lock:
        conn = get_db()
        changes = [
            dict(r)
            for r in conn.execute(
                "SELECT * FROM release_changes WHERE release_id=? ORDER BY id ASC",
                (release_id,),
            )
        ]
        conn.close()

    return {"release": release, "changes": changes}


@app.get("/releases/{release_id}/health")
def get_release_health(release_id: str):
    finalize_expired_releases()
    release = release_row(release_id)
    if not release:
        raise HTTPException(404, "Release not found")

    if release["status"] == "OBSERVING":
        status, reason = release_health(release)
        return {
            "release_id": release_id,
            "status": status,
            "reason": reason,
            "observation_ends_at": release["observation_ends_at"],
            "rollback_available": True,
        }

    return {
        "release_id": release_id,
        "status": release["health_status"] or release["status"],
        "reason": release["health_reason"],
        "observation_ends_at": release["observation_ends_at"],
        "rollback_available": release["status"] in ("OBSERVING", "FINALIZED", "UNCERTAIN"),
    }


@app.post("/releases/{release_id}/rollback")
def rollback_release(release_id: str, req: ReleaseActionRequest = ReleaseActionRequest()):
    """
    One-action rollback. The operator only needs to invoke this endpoint/button.
    The known rollback plan is recorded as the operation performed.
    """
    release = release_row(release_id)
    if not release:
        raise HTTPException(404, "Release not found")

    if not release["rollback_ready"] or not release["rollback_plan"].strip():
        raise HTTPException(400, "Rollback refused: no known rollback plan.")

    if release["status"] in ("ROLLED_BACK",):
        return {
            "release_id": release_id,
            "status": "ROLLED_BACK",
            "already_rolled_back": True,
        }

    if release["status"] not in ("OBSERVING", "FINALIZED", "UNCERTAIN"):
        raise HTTPException(
            409,
            f"Rollback is not available from release state {release['status']}",
        )

    reason = (req.reason or "Operator initiated one-action rollback").strip()

    with db_lock:
        conn = get_db()
        conn.execute(
            """UPDATE releases
               SET status='ROLLED_BACK', rolled_back_at=?,
                   health_status='ROLLED_BACK', health_reason=?
               WHERE id=?""",
            (now(), reason, release_id),
        )
        conn.commit()
        conn.close()

    release_change(
        release_id,
        "ROLLED_BACK",
        f"Rollback executed using known plan: {release['rollback_plan']}. Reason: {reason}",
    )
    event("RELEASE_ROLLED_BACK",f"Release {release_id} rolled back in one operator action. Reason: {reason}",subject_type="release",subject_id=release_id)

    return {
        "release_id": release_id,
        "status": "ROLLED_BACK",
        "rollback_result": "known",
        "message": "Rollback completed.",
    }


@app.post("/releases/{release_id}/finalize")
def finalize_release(release_id: str):
    release = release_row(release_id)
    if not release:
        raise HTTPException(404, "Release not found")

    if release["status"] != "OBSERVING":
        raise HTTPException(409, f"Release is {release['status']}")

    end = datetime.fromisoformat(release["observation_ends_at"])
    if datetime.now(timezone.utc) < end:
        remaining = max(0, int((end - datetime.now(timezone.utc)).total_seconds()))
        raise HTTPException(
            409,
            f"Observation period is still active ({remaining}s remaining).",
        )

    status, reason = release_health(release)

    with db_lock:
        conn = get_db()
        conn.execute(
            """UPDATE releases
               SET status=?, health_status=?, health_reason=?, finalized_at=?
               WHERE id=?""",
            (
                "FINALIZED" if status == "HEALTHY" else "UNCERTAIN",
                status,
                reason,
                now(),
                release_id,
            ),
        )
        conn.commit()
        conn.close()

    event("RELEASE_FINALIZED" if status == "HEALTHY" else "RELEASE_UNCERTAIN",f"Release {release_id}: {status} — {reason}",subject_type="release",subject_id=release_id)
    release_change(release_id, "OBSERVATION_ENDED", f"{status}: {reason}")

    return {
        "release_id": release_id,
        "status": "FINALIZED" if status == "HEALTHY" else "UNCERTAIN",
        "health": status,
        "reason": reason,
    }


# ---------------------------
# Reconciliation + cache subsystem
# ---------------------------

class FactRequest(BaseModel):
    subject_id: str
    fact_key: str
    owner: str = "human"
    authoritative_value: dict | str | int | float | bool | None = None
    copy_value: dict | str | int | float | bool | None = None
    compare_interval_seconds: int = RECONCILIATION_DEFAULT_INTERVAL_SECONDS

class CachePutRequest(BaseModel):
    value: dict | str | int | float | bool | None
    max_age_seconds: int = CACHE_DEFAULT_MAX_AGE_SECONDS

class SourceFactRequest(BaseModel):
    value: dict | str | int | float | bool | None
    reachable: bool = True


def _j(v): return json.dumps(v, sort_keys=True, default=str)


def reconciliation_scan():
    while not stop_event.is_set():
        pending_events=[]; pending_audits=[]
        try:
            with db_lock:
                conn=get_db(); rows=conn.execute("SELECT * FROM facts").fetchall()
                for r in rows:
                    due=not r["last_compared_at"]
                    if not due:
                        due=(datetime.now(timezone.utc)-datetime.fromisoformat(r["last_compared_at"])).total_seconds() >= r["compare_interval_seconds"]
                    if not due: continue
                    a=r["authoritative_value"]; b=r["copy_value"]
                    try: same=json.loads(a)==json.loads(b)
                    except Exception: same=a==b
                    age_a=max(0,(datetime.now(timezone.utc)-datetime.fromisoformat(r["authoritative_updated_at"])).total_seconds())
                    age_b=max(0,(datetime.now(timezone.utc)-datetime.fromisoformat(r["copy_updated_at"])).total_seconds())
                    if same:
                        conn.execute("UPDATE facts SET status='MATCH',last_compared_at=?,disagreement_count=0,first_disagreement_at=NULL WHERE subject_id=? AND fact_key=?",(now(),r["subject_id"],r["fact_key"]))
                        continue
                    count=r["disagreement_count"]+1
                    status="DISAGREED_ESCALATED" if count>=RECONCILIATION_ESCALATION_AFTER else "DISAGREED"
                    subject=f'{r["subject_id"]}:{r["fact_key"]}'
                    if r["owner"]=="platform":
                        conn.execute("UPDATE facts SET copy_value=?,copy_updated_at=?,status='AUTO_FIXED',disagreement_count=?,first_disagreement_at=COALESCE(first_disagreement_at,?),last_compared_at=? WHERE subject_id=? AND fact_key=?",(a,now(),count,r["first_disagreement_at"] or now(),now(),r["subject_id"],r["fact_key"]))
                        pending_audits.append((subject,"AUTO_FIX",{"copy":json.loads(b),"copy_age_seconds":age_b},{"copy":json.loads(a),"source_age_seconds":age_a},{"authoritative_owner":"platform","source":json.loads(a)},"Authoritative copy disagreed; platform-owned fact was repaired"))
                    else:
                        conn.execute("UPDATE facts SET status=?,disagreement_count=?,first_disagreement_at=COALESCE(first_disagreement_at,?),last_compared_at=? WHERE subject_id=? AND fact_key=?",(status,count,r["first_disagreement_at"] or now(),now(),r["subject_id"],r["fact_key"]))
                        pending_events.append(("FACT_DISAGREEMENT_ESCALATED" if status.endswith("ESCALATED") else "FACT_DISAGREEMENT",f"Human-owned fact disagreement: {subject}; copy age={age_b:.1f}s, source age={age_a:.1f}s"))
                conn.commit(); conn.close()
            for args in pending_audits: audit_action("fact",*args)
            for et,msg in pending_events: event(et,msg)
        except Exception as exc: event("RECONCILIATION_ERROR",str(exc))
        time.sleep(2)


def start_background_threads():
    t=threading.Thread(target=reconciliation_scan,daemon=True); t.start(); background_threads.append(t)


@app.post("/facts")
def upsert_fact(req: FactRequest):
    if req.owner not in ("platform","human"): raise HTTPException(400,"owner must be platform or human")
    a=_j(req.authoritative_value); b=_j(req.copy_value)
    with db_lock:
        conn=get_db(); conn.execute("INSERT INTO facts(subject_id,fact_key,owner,authoritative_value,authoritative_updated_at,copy_value,copy_updated_at,compare_interval_seconds,status) VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(subject_id,fact_key) DO UPDATE SET owner=excluded.owner,authoritative_value=excluded.authoritative_value,authoritative_updated_at=excluded.authoritative_updated_at,copy_value=excluded.copy_value,copy_updated_at=excluded.copy_updated_at,compare_interval_seconds=excluded.compare_interval_seconds,status='MATCH',disagreement_count=0,last_compared_at=NULL",(req.subject_id,req.fact_key,req.owner,a,now(),b,now(),req.compare_interval_seconds,"MATCH")); conn.commit(); conn.close()
    event("FACT_REGISTERED",f"Fact registered for comparison every {req.compare_interval_seconds}s")
    return {"subject_id":req.subject_id,"fact_key":req.fact_key,"owner":req.owner}

@app.get("/facts")
def facts():
    with db_lock:
        conn=get_db(); rows=[dict(r) for r in conn.execute("SELECT * FROM facts ORDER BY subject_id,fact_key")]; conn.close()
    for r in rows:
        r["authoritative_value"]=json.loads(r["authoritative_value"]); r["copy_value"]=json.loads(r["copy_value"])
        r["authoritative_age_seconds"]=max(0,(datetime.now(timezone.utc)-datetime.fromisoformat(r["authoritative_updated_at"])).total_seconds())
        r["copy_age_seconds"]=max(0,(datetime.now(timezone.utc)-datetime.fromisoformat(r["copy_updated_at"])).total_seconds())
    return {"facts":rows,"comparison_policy":{"default_interval_seconds":RECONCILIATION_DEFAULT_INTERVAL_SECONDS,"escalate_after":RECONCILIATION_ESCALATION_AFTER}}

@app.post("/cache/source/{cache_key}")
def set_source(cache_key:str, req:SourceFactRequest):
    with db_lock:
        conn=get_db(); old=conn.execute("SELECT * FROM source_facts WHERE source_key=?",(cache_key,)).fetchone(); conn.execute("INSERT INTO source_facts(source_key,value,updated_at,reachable) VALUES(?,?,?,?) ON CONFLICT(source_key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at,reachable=excluded.reachable",(cache_key,_j(req.value),now(),1 if req.reachable else 0)); conn.execute("DELETE FROM cache_entries WHERE cache_key=?",(cache_key,)); conn.commit(); conn.close()
    audit_action("cache",cache_key,"CACHE_INVALIDATED",before_state=dict(old) if old else None,after_state={"value":req.value,"reachable":req.reachable},reason="Authoritative source changed; cached copy cleared")
    return {"cache_key":cache_key,"invalidated":True}

@app.post("/cache/{cache_key}")
def put_cache(cache_key:str, req:CachePutRequest):
    with db_lock:
        conn=get_db(); conn.execute("INSERT INTO cache_entries(cache_key,value,source_updated_at,cached_at,max_age_seconds,stale_served) VALUES(?,?,?,?,?,0) ON CONFLICT(cache_key) DO UPDATE SET value=excluded.value,source_updated_at=excluded.source_updated_at,cached_at=excluded.cached_at,max_age_seconds=excluded.max_age_seconds,stale_served=0",(cache_key,_j(req.value),now(),now(),req.max_age_seconds)); conn.commit(); conn.close()
    return {"cache_key":cache_key,"value":req.value,"age_seconds":0,"max_age_seconds":req.max_age_seconds,"stale":False}

@app.get("/cache/{cache_key}")
def get_cache(cache_key:str, allow_stale:bool=False):
    with db_lock:
        conn=get_db(); c=conn.execute("SELECT * FROM cache_entries WHERE cache_key=?",(cache_key,)).fetchone(); src=conn.execute("SELECT * FROM source_facts WHERE source_key=?",(cache_key,)).fetchone(); conn.close()
    if not c: raise HTTPException(404,"Cache entry not found")
    age=max(0,(datetime.now(timezone.utc)-datetime.fromisoformat(c["cached_at"])).total_seconds()); stale=age>c["max_age_seconds"]
    if stale and src and src["reachable"]:
        # Single-flight refresh: after waiting for the DB lock, re-check the entry.
        with db_lock:
            conn=get_db(); current=conn.execute("SELECT * FROM cache_entries WHERE cache_key=?",(cache_key,)).fetchone(); source=conn.execute("SELECT * FROM source_facts WHERE source_key=?",(cache_key,)).fetchone()
            current_age=max(0,(datetime.now(timezone.utc)-datetime.fromisoformat(current["cached_at"])).total_seconds()) if current else 999999
            if current and source and (current_age > current["max_age_seconds"] or current["source_updated_at"] != source["updated_at"]):
                conn.execute("UPDATE cache_entries SET value=?,source_updated_at=?,cached_at=?,stale_served=0 WHERE cache_key=?",(source["value"],source["updated_at"],now(),cache_key)); conn.commit(); current=conn.execute("SELECT * FROM cache_entries WHERE cache_key=?",(cache_key,)).fetchone()
            c=current; conn.close()
        age=max(0,(datetime.now(timezone.utc)-datetime.fromisoformat(c["cached_at"])).total_seconds()); stale=age>c["max_age_seconds"]
    if stale and not allow_stale: raise HTTPException(503,"Cache is too old and source is unavailable; stale serving was not selected")
    value=json.loads(c["value"])
    return {"cache_key":cache_key,"value":value,"age_seconds":round(age,2),"max_age_seconds":c["max_age_seconds"],"stale":stale,"stale_served":stale}

@app.get("/cache/{cache_key}/check")
def check_cache(cache_key:str):
    with db_lock:
        conn=get_db(); c=conn.execute("SELECT * FROM cache_entries WHERE cache_key=?",(cache_key,)).fetchone(); srow=conn.execute("SELECT * FROM source_facts WHERE source_key=?",(cache_key,)).fetchone(); conn.close()
    if not c or not srow: raise HTTPException(404,"Cache or source not found")
    same=c["value"]==srow["value"]; age=max(0,(datetime.now(timezone.utc)-datetime.fromisoformat(c["cached_at"])).total_seconds())
    return {"cache_key":cache_key,"matches_source":same,"cache_age_seconds":round(age,2),"max_age_seconds":c["max_age_seconds"],"source_reachable":bool(srow["reachable"]),"checked_at":now()}

@app.get("/history/{subject_type}/{subject_id}")
def subject_history(subject_type:str, subject_id:str):
    with db_lock:
        conn=get_db(); actions=[dict(r) for r in conn.execute("SELECT * FROM audit_actions WHERE subject_type=? AND subject_id=? ORDER BY id DESC",(subject_type,subject_id))]; events=[dict(r) for r in conn.execute("SELECT * FROM events WHERE job_id=? OR worker_id=? ORDER BY id DESC",(subject_id,subject_id))]; conn.close()
    return {"subject_type":subject_type,"subject_id":subject_id,"actions":actions,"events":events,"retention_days":EVENT_RETENTION_DAYS}



@app.get("/timeline/{subject_type}/{subject_id}")
def subject_timeline(subject_type:str, subject_id:str):
    with db_lock:
        conn=get_db()
        rows=[]
        rows += [dict(r,record_kind="event") for r in conn.execute("SELECT timestamp,event_type AS type,message,job_id,worker_id FROM events WHERE subject_type=? AND subject_id=?",(subject_type,subject_id))]
        rows += [dict(r,record_kind="audit") for r in conn.execute("SELECT timestamp,action AS type,reason AS message,NULL AS job_id,NULL AS worker_id FROM audit_actions WHERE subject_type=? AND subject_id=?",(subject_type,subject_id))]
        if subject_type=="release": rows += [dict(r,record_kind="release_change",timestamp=r["changed_at"],type=r["change_type"],message=r["details"],job_id=None,worker_id=None) for r in conn.execute("SELECT changed_at,change_type,details FROM release_changes WHERE release_id=?",(subject_id,))]
        conn.close()
    rows.sort(key=lambda x:x["timestamp"],reverse=True)
    return {"subject_type":subject_type,"subject_id":subject_id,"timeline":rows,"retention_days":EVENT_RETENTION_DAYS}


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


@app.post("/jobs/{job_id}/retry")
def retry_dead_letter(job_id: str):
    with db_lock:
        conn=get_db(); row=conn.execute("SELECT * FROM jobs WHERE id=?",(job_id,)).fetchone()
        if not row: conn.close(); raise HTTPException(404,"Job not found")
        if row["status"] != "DEAD_LETTER": conn.close(); raise HTTPException(409,"Only DEAD_LETTER jobs can be manually retried")
        before=dict(row)
        conn.execute("UPDATE jobs SET status='PENDING', attempts=0, worker_id=NULL, error=NULL, updated_at=? WHERE id=?",(now(),job_id))
        conn.commit(); conn.close()
    audit_action("job",job_id,"MANUAL_RETRY",before,{**before,"status":"PENDING","attempts":0},reason="Operator chose to retry dead-end work")
    return {"job_id":job_id,"status":"PENDING"}


@app.post("/jobs/{job_id}/discard")
def discard_dead_letter(job_id: str):
    with db_lock:
        conn=get_db(); row=conn.execute("SELECT * FROM jobs WHERE id=?",(job_id,)).fetchone()
        if not row: conn.close(); raise HTTPException(404,"Job not found")
        if row["status"] != "DEAD_LETTER": conn.close(); raise HTTPException(409,"Only DEAD_LETTER jobs can be discarded")
        before=dict(row)
        conn.execute("UPDATE jobs SET status='DISCARDED', updated_at=? WHERE id=?",(now(),job_id))
        conn.commit(); conn.close()
    audit_action("job",job_id,"DISCARD",before,{**before,"status":"DISCARDED"},reason="Operator discarded dead-end work")
    return {"job_id":job_id,"status":"DISCARDED"}


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
    if worker_id not in worker_modes: raise HTTPException(404,"Unknown worker")
    return recover_worker(worker_id)


@app.post("/workers/{worker_id}/recover")
def recover_worker(worker_id: str):
    if worker_id not in worker_modes: raise HTTPException(404,"Unknown worker")
    with worker_lock:
        before={"mode":worker_modes.get(worker_id),"restart_count":worker_restart_counts.get(worker_id,0),"state":worker_recovery_state.get(worker_id)}
        worker_modes[worker_id]="normal"
        worker_recovery_state[worker_id]="RECOVERING"
        worker_healthy_since[worker_id]=time.time()
    started=start_worker(worker_id)
    after={"mode":"normal","state":"RECOVERING","started":started}
    audit_action("worker",worker_id,"MANUAL_RECOVER",before,after,reason="Operator returned out-of-service worker without restarting NEXUS")
    event("WORKER_MANUAL_RECOVER",f"Worker returned to service; settling for {WORKER_SETTLING_SECONDS}s",worker_id=worker_id)
    return {"worker_id":worker_id,"status":"RECOVERING","started":started,"settling_seconds":WORKER_SETTLING_SECONDS}


@app.get("/workers/{worker_id}/history")
def worker_history(worker_id: str):
    if worker_id not in worker_modes: raise HTTPException(404,"Unknown worker")
    with db_lock:
        conn=get_db()
        attempts=[dict(r) for r in conn.execute("SELECT * FROM worker_attempts WHERE worker_id=? ORDER BY id DESC LIMIT 50",(worker_id,))]
        events=[dict(r) for r in conn.execute("SELECT * FROM events WHERE worker_id=? ORDER BY id DESC LIMIT 100",(worker_id,))]
        conn.close()
    return {"worker_id":worker_id,"budget":{"max_attempts":MAX_WORKER_RESTARTS,"window_seconds":WORKER_RESTART_WINDOW_SECONDS,"settling_seconds":WORKER_SETTLING_SECONDS},"attempts":attempts,"events":events,"history_retention_days":EVENT_RETENTION_DAYS}


def calculate_health(counts, workers, events):
    active_incidents=[]
    out_workers=[w for w in workers if w["status"]=="OUT_OF_SERVICE"]
    dead_jobs=counts.get("DEAD_LETTER",0)
    pending=counts.get("PENDING",0)
    bm=backlog_metrics()
    for w in out_workers:
        active_incidents.append({"severity":"critical","title":f"{w['id']} is OUT_OF_SERVICE","detail":f"Restart budget exhausted ({w['restart_count']}/{MAX_WORKER_RESTARTS}) in {WORKER_RESTART_WINDOW_SECONDS}s. Last error: {w.get('last_error') or 'unknown'}"})
    if dead_jobs:
        active_incidents.append({"severity":"warning","title":f"{dead_jobs} job(s) in DEAD_LETTER","detail":"Work stopped without finishing. Operator must choose retry or discard."})
    if pending>=10 or (bm["growth_per_minute"] is not None and bm["growth_per_minute"]>0):
        growth=(f" Backlog is growing at {bm['growth_per_minute']:.2f}/min." if bm["growth_per_minute"] is not None else "")
        active_incidents.append({"severity":"warning","title":f"Backlog abnormal: {pending} pending","detail":f"Oldest waiting work is {bm['oldest_age_seconds']:.1f}s old; average age {bm['average_age_seconds']:.1f}s.{growth}"})
    if out_workers: overall="CRITICAL"
    elif active_incidents: overall="DEGRADED"
    else: overall="HEALTHY"
    return {"overall":overall,"statement":"System behavior is normal" if overall=="HEALTHY" else "System has abnormal conditions requiring attention","active_incidents":active_incidents,"backlog":bm}


@app.get("/api/state")
def state():
    record_backlog_sample()
    cleanup_retention()
    with db_lock:
        conn=get_db()
        counts={row["status"]:row["c"] for row in conn.execute("SELECT status,COUNT(*) c FROM jobs GROUP BY status")}
        jobs=[dict(row) for row in conn.execute("SELECT id,status,attempts,worker_id,error,created_at,updated_at,completed_at FROM jobs ORDER BY created_at DESC LIMIT 50")]
        events=[dict(row) for row in conn.execute("SELECT * FROM events ORDER BY id DESC LIMIT 100")]
        actions=[dict(row) for row in conn.execute("SELECT * FROM audit_actions ORDER BY id DESC LIMIT 50")]
        worker_attempts=[dict(row) for row in conn.execute("SELECT * FROM worker_attempts ORDER BY id DESC LIMIT 50")]
        releases=[dict(row) for row in conn.execute("SELECT * FROM releases ORDER BY created_at DESC LIMIT 20")]
        release_changes=[dict(row) for row in conn.execute("SELECT * FROM release_changes ORDER BY id DESC LIMIT 100")]
        facts=[dict(row) for row in conn.execute("SELECT * FROM facts ORDER BY subject_id,fact_key")]
        conn.close()
    workers=[]
    with worker_lock:
        for wid in sorted(worker_modes):
            thread=worker_threads.get(wid); mode=worker_modes.get(wid); alive=bool(thread and thread.is_alive())
            status="OUT_OF_SERVICE" if mode=="out_of_service" else ("RUNNING" if alive else "STOPPED")
            workers.append({"id":wid,"status":status,"mode":mode,"restart_count":worker_restart_counts.get(wid,0),"restart_budget":MAX_WORKER_RESTARTS,"restart_window_seconds":WORKER_RESTART_WINDOW_SECONDS,"recovery_state":worker_recovery_state.get(wid),"settling_seconds":WORKER_SETTLING_SECONDS,"last_error":worker_last_error.get(wid)})
    health=calculate_health(counts,workers,events)
    finalize_expired_releases()
    return {"counts":counts,"workers":workers,"jobs":jobs,"events":events,"actions":actions,"worker_attempts":worker_attempts,"health":health,"backlog":backlog_metrics(),"releases":releases,"release_changes":release_changes,"facts":facts,"policies":{"event_retention_days":EVENT_RETENTION_DAYS,"idempotency_retention_hours":IDEMPOTENCY_RETENTION_HOURS,"cache_default_max_age_seconds":CACHE_DEFAULT_MAX_AGE_SECONDS,"worker_restart_budget":MAX_WORKER_RESTARTS,"worker_restart_window_seconds":WORKER_RESTART_WINDOW_SECONDS,"worker_settling_seconds":WORKER_SETTLING_SECONDS}}


DASHBOARD_HTML = r"""
<!doctype html><html><head><meta charset="utf-8"><title>NEXUS Reliability Dashboard</title>
<style>body{font-family:Arial;margin:0;background:#f4f6f8;color:#1f2937}header{background:#111827;color:#fff;padding:18px 28px}main{max-width:1450px;margin:20px auto;padding:0 16px}.grid{display:grid;grid-template-columns:repeat(5,1fr);gap:10px}.card{background:#fff;border-radius:10px;padding:15px;box-shadow:0 1px 4px #0001;margin-bottom:16px}.big{font-size:25px;font-weight:bold}table{width:100%;border-collapse:collapse}th,td{padding:7px;border-bottom:1px solid #eee;text-align:left;font-size:12px;vertical-align:top}button,input,textarea{padding:7px;border:1px solid #ccc;border-radius:6px}button{cursor:pointer}.ok{color:#16803c}.bad{color:#c62828}.warn{color:#a16207}.incident{padding:9px;margin:6px 0;background:#fafafa;border-left:5px solid #d1d5db}.critical{border-left-color:#dc2626}.warning{border-left-color:#d97706}.controls{display:flex;gap:6px;flex-wrap:wrap}.formgrid{display:grid;grid-template-columns:1fr 1fr;gap:8px}.formgrid textarea{width:100%;box-sizing:border-box}.badge{padding:3px 6px;border-radius:4px;background:#eee}.green{background:#dcfce7}.yellow{background:#fef3c7}.blue{background:#dbeafe}.red{background:#fee2e2}small{color:#6b7280}</style></head>
<body><header><h1>NEXUS Reliability Platform</h1><div>Durable work • bounded recovery • release safety • reconciliation • cache safety • audit history</div></header><main>
<section class="grid" id="summary"></section>
<section class="card"><h2>System judgement</h2><div id="health"></div><div id="incidents"></div></section>
<section class="card"><h2>Backlog</h2><div id="backlog"></div></section>
<section class="card"><h2>Workers & restart budgets</h2><table><thead><tr><th>Worker</th><th>Status</th><th>Recovery</th><th>Budget</th><th>Last error</th><th>Action</th></tr></thead><tbody id="workers"></tbody></table></section>
<section class="card"><h2>Release safety</h2><div class="formgrid"><input id="rn" value="demo-service"><input id="rv" value="v2.0"><input id="rp" value="v1.0"><textarea id="rr">Restore previous version v1.0 and route traffic back to it.</textarea></div><p><label><input type="checkbox" id="ro"> deliberate overlap</label> <input id="ror" placeholder="required overlap reason"></p><button onclick="createRelease()">Create + Deploy</button><span id="rm"></span><table><thead><tr><th>Release</th><th>Status</th><th>Health</th><th>Observation</th><th>Actions</th></tr></thead><tbody id="releases"></tbody></table></section>
<section class="card"><h2>Dead-end work</h2><table><thead><tr><th>Job</th><th>Attempts</th><th>Error</th><th>Action</th></tr></thead><tbody id="dead"></tbody></table></section>
<section class="card"><h2>Reconciliation / disagreements</h2><table><thead><tr><th>Subject</th><th>Fact</th><th>Owner</th><th>Status</th><th>Ages</th><th>Checks</th></tr></thead><tbody id="facts"></tbody></table></section>
<section class="card"><h2>Audit trail</h2><table><thead><tr><th>Time</th><th>Subject</th><th>Action</th><th>Belief</th><th>Before → After</th><th>Reason</th></tr></thead><tbody id="audit"></tbody></table></section>
<section class="card"><h2>Recent platform events</h2><table><thead><tr><th>Time</th><th>Type</th><th>Job</th><th>Worker</th><th>Message</th></tr></thead><tbody id="events"></tbody></table></section>
<section class="card"><h2>Policies</h2><div id="policies"></div></section>
<section class="card"><h2>Failure simulation</h2><div class="controls"><button onclick="mode('worker-1','crash')">Crash W1</button><button onclick="mode('worker-1','always_crash')">Always crash W1</button><button onclick="mode('worker-1','normal')">Normal W1</button><button onclick="mode('worker-2','crash')">Crash W2</button><button onclick="mode('worker-2','always_crash')">Always crash W2</button><button onclick="mode('worker-2','normal')">Normal W2</button><button onclick="job()">Create job</button></div></section>
</main><script>
const esc=s=>String(s??'').replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;');
async function api(u,o={}){let r=await fetch(u,{headers:{'Content-Type':'application/json'},...o});let d=await r.json();if(!r.ok)throw Error(d.detail||JSON.stringify(d));return d}
async function mode(w,m){try{await api('/workers/'+w+'/mode',{method:'POST',body:JSON.stringify({mode:m})});refresh()}catch(e){alert(e.message)}}
async function recover(w){try{await api('/workers/'+w+'/recover',{method:'POST'});refresh()}catch(e){alert(e.message)}}
async function job(){await api('/jobs',{method:'POST',body:JSON.stringify({id:'demo-'+Date.now(),job_type:'demo',payload:{demo:true}})});refresh()}
async function retry(id){await api('/jobs/'+encodeURIComponent(id)+'/retry',{method:'POST'});refresh()}
async function discard(id){if(confirm('Discard this dead-end job?')){await api('/jobs/'+encodeURIComponent(id)+'/discard',{method:'POST'});refresh()}}
async function createRelease(){try{let d=await api('/releases',{method:'POST',body:JSON.stringify({name:rn.value,version:rv.value,previous_version:rp.value,rollback_plan:rr.value,allow_overlap:ro.checked,overlap_reason:ror.value||null})});let x=await api('/releases/'+d.release_id+'/deploy',{method:'POST'});rm.innerHTML=' <span class="ok">deployed and observing</span>';refresh()}catch(e){rm.innerHTML=' <span class="bad">'+esc(e.message)+'</span>'}}
async function rollback(id){if(confirm('One-action rollback?')){await api('/releases/'+id+'/rollback',{method:'POST',body:JSON.stringify({reason:'Operator rollback'})});refresh()}}
async function finalize(id){try{await api('/releases/'+id+'/finalize',{method:'POST'});refresh()}catch(e){alert(e.message)}}
async function refresh(){try{let d=await api('/api/state'),c=d.counts||{},b=d.backlog||{};summary.innerHTML=`<div class="card"><small>Pending</small><div class="big">${c.PENDING||0}</div></div><div class="card"><small>Oldest wait</small><div class="big">${b.oldest_age_seconds||0}s</div></div><div class="card"><small>Growth/min</small><div class="big">${b.growth_per_minute??'—'}</div></div><div class="card"><small>Dead letter</small><div class="big bad">${c.DEAD_LETTER||0}</div></div><div class="card"><small>Processing</small><div class="big">${c.PROCESSING||0}</div></div>`;
health.innerHTML=`<strong class="${d.health.overall==='HEALTHY'?'ok':d.health.overall==='CRITICAL'?'bad':'warn'}">${esc(d.health.statement)}</strong>`;incidents.innerHTML=(d.health.active_incidents||[]).map(i=>`<div class="incident ${i.severity}"><b>${esc(i.title)}</b><br>${esc(i.detail)}</div>`).join('')||'<div class="incident">No abnormal condition detected.</div>';
backlog.innerHTML=`Waiting: <b>${b.count}</b> · oldest: <b>${b.oldest_age_seconds}s</b> · average: <b>${b.average_age_seconds}s</b> · growth: <b>${b.growth_per_minute??'not enough history'}/min</b> · window: ${b.growth_window_seconds}s`;
workers.innerHTML=d.workers.map(w=>`<tr><td>${esc(w.id)}</td><td>${esc(w.status)}</td><td>${esc(w.recovery_state||'')}</td><td>${w.restart_count}/${w.restart_budget} in ${w.restart_window_seconds}s</td><td>${esc(w.last_error)}</td><td>${w.status==='OUT_OF_SERVICE'?`<button onclick="recover('${w.id}')">Recover</button>`:''}</td></tr>`).join('');
dead.innerHTML=(d.jobs||[]).filter(j=>j.status==='DEAD_LETTER').map(j=>`<tr><td>${esc(j.id)}</td><td>${j.attempts}</td><td>${esc(j.error)}</td><td><button onclick="retry('${j.id}')">Retry</button> <button onclick="discard('${j.id}')">Discard</button></td></tr>`).join('')||'<tr><td colspan=4>No dead-end work.</td></tr>';
facts.innerHTML=(d.facts||[]).map(f=>`<tr><td>${esc(f.subject_id)}</td><td>${esc(f.fact_key)}</td><td>${esc(f.owner)}</td><td>${esc(f.status)} (${f.disagreement_count})</td><td>${Math.round((Date.now()-Date.parse(f.authoritative_updated_at))/1000)}s / ${Math.round((Date.now()-Date.parse(f.copy_updated_at))/1000)}s</td><td>${esc(f.last_compared_at||'not yet')}</td></tr>`).join('')||'<tr><td colspan=6>No facts registered.</td></tr>';
audit.innerHTML=(d.actions||[]).slice(0,30).map(a=>`<tr><td>${esc(a.timestamp)}</td><td>${esc(a.subject_type+':'+a.subject_id)}</td><td>${esc(a.action)}</td><td>${esc(a.belief)}</td><td>${esc(a.before_state)} → ${esc(a.after_state)}</td><td>${esc(a.reason)}</td></tr>`).join('');
events.innerHTML=(d.events||[]).slice(0,50).map(e=>`<tr><td>${esc(e.timestamp)}</td><td>${esc(e.event_type)}</td><td>${esc(e.job_id)}</td><td>${esc(e.worker_id)}</td><td>${esc(e.message)}</td></tr>`).join('');
releases.innerHTML=(d.releases||[]).map(r=>`<tr><td><b>${esc(r.name)}</b><br>${esc(r.version)}</td><td><span class="badge">${esc(r.status)}</span></td><td>${esc(r.health_status||'UNKNOWN')}<br><small>${esc(r.health_reason||'')}</small></td><td>${esc(r.observation_ends_at||'-')}</td><td>${['OBSERVING','UNCERTAIN','FINALIZED'].includes(r.status)?`<button onclick="rollback('${r.id}')">↩ Rollback</button>`:''} ${r.status==='OBSERVING'?`<button onclick="finalize('${r.id}')">Finalize</button>`:''}</td></tr>`).join('');
policies.innerHTML=Object.entries(d.policies).map(([k,v])=>`<div><b>${esc(k)}</b>: ${esc(v)}</div>`).join('');}catch(e){console.error(e)}}
setInterval(refresh,1000);refresh();</script></body></html>"""


if __name__ == "__main__":
    uvicorn.run("app:app", host="127.0.0.1", port=8000, reload=False)
