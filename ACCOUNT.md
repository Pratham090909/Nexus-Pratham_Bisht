# NEXUS Account

## Scope

I implemented the core reliability slice of NEXUS:

1. Safe/persistent acceptance of jobs
2. Clear terminal job outcomes
3. Duplicate-job handling
4. Bounded retries with exponential backoff
5. Worker crash/recovery behavior
6. Event history for incident reconstruction
7. Operator dashboard
8. Deliberate failure simulation

## Design decisions

### Python + FastAPI

FastAPI provides a lightweight local API and dashboard host with minimal setup.

### SQLite

The assignment is scoped to a single machine and a few thousand jobs. SQLite provides persistence without adding an external service.

### Local worker threads

The workers are intentionally simple stand-ins for background services. Their behavior can be controlled to demonstrate failure handling.

### Bounded retries

Jobs have a maximum of three processing attempts. Retry delays increase exponentially.

### Bounded worker restarts

A worker is restarted only a limited number of times. If it continues to fail, it becomes OUT_OF_SERVICE instead of entering an infinite restart loop.

### Event log

State alone is not enough for incident investigation. Events record the sequence of important actions.

## Failure scenarios demonstrated

- Worker crash
- Persistent worker crash
- Slow worker
- Duplicate job delivery
- NEXUS restart/recovery
- Retry exhaustion

## Intentionally not implemented

Because the implementation is time-constrained, the following are outside the focused MVP:

- Distributed multi-machine coordination
- External message brokers
- Cache consistency/reconciliation
- Dependency degradation policies
- Production authentication/authorization
- Full release management and rollback
- Advanced metrics/telemetry
- High availability

## Known limitations

This is a local prototype. SQLite and local threads are appropriate for the assignment scope but are not a replacement for a distributed production architecture.

The duplicate protection is demonstrated at the job/platform level. In a real system, external side effects should also be designed to be idempotent or protected by transactional/outbox patterns.

## Testing performed

The intended manual tests are:

- Create jobs and verify completion
- Submit the same job ID twice
- Crash a worker and observe restart
- Force repeated worker crashes and observe OUT_OF_SERVICE
- Restart NEXUS and verify persisted jobs remain
- Observe event history for each scenario

## Next steps with more time

- Add explicit release/version tracking and one-click rollback
- Add dependency degradation
- Add cache/data consistency checks
- Add richer metrics and structured logging
- Add automated integration tests
- Introduce a dedicated durable queue for a larger deployment
