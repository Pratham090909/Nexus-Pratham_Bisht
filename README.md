# NEXUS — Local Reliability Platform

A focused local implementation of the NEXUS assignment. It demonstrates durable job acceptance, worker processing, bounded retries with backoff, duplicate detection, worker crash/recovery, event history, and an operator dashboard.

## 1. Start

```bash
python -m venv .venv
# Windows:
.venv\Scripts\activate
# macOS/Linux:
source .venv/bin/activate

pip install -r requirements.txt
python app.py
```

Open:

http://127.0.0.1:8000

## 2. What is implemented

- Persistent jobs in SQLite
- Job lifecycle: PENDING → PROCESSING → COMPLETED / DEAD_LETTER
- Maximum 3 job attempts
- Exponential retry backoff
- Duplicate job ID detection
- Two local workers
- Worker crash simulation
- Persistent worker failure simulation
- Bounded worker restarts
- OUT_OF_SERVICE state
- Recovery of PROCESSING jobs after NEXUS restart
- Event/audit log
- Operator dashboard
- Failure simulation controls

## 3. Demo sequence

### Normal processing
Click `Create Demo Job` and watch it move to COMPLETED.

### Duplicate
Create a job, then click `Duplicate Latest Job`. The event log records the duplicate delivery.

### Worker crash
Click `Crash Worker 1`. The worker exits and the supervisor restarts it. The event log records the crash and restart.

### Persistent worker failure
Click `Always Crash Worker 1`. The worker repeatedly fails until the restart limit is reached, then becomes OUT_OF_SERVICE.

### Recovery
Click `Recover Worker 1` to return the worker to normal.

### Persistence
Create several jobs and stop/restart the application. Jobs stored in SQLite remain available. Jobs that were PROCESSING during shutdown are recovered to PENDING at startup.

## 4. API

- `POST /jobs` — create a job
- `POST /jobs/{job_id}/duplicate` — simulate duplicate delivery
- `POST /workers/{worker_id}/mode` — set worker mode
- `POST /workers/{worker_id}/restart` — manually restart worker
- `GET /api/state` — dashboard state

Example:

```bash
curl -X POST http://127.0.0.1:8000/jobs ^
  -H "Content-Type: application/json" ^
  -d "{\"id\":\"job-1\",\"job_type\":\"demo\",\"payload\":{\"value\":123}}"
```

## 5. Important scope

This is intentionally a single-machine prototype for the challenge. It is not a production distributed queue. It does not implement every extended requirement from the assignment.
