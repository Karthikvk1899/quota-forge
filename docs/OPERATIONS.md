# Failure model and deployment runbook

## Guarantees and explicit limits

The window is exact relative to the active Redis primary's clock and surviving state. NTP jumps, asynchronous failover, operator key deletion, eviction or data restoration can break real-time/global guarantees. `appendfsync always` reduces the local durability window but is not a consensus protocol. Use noeviction; this service must not share Redis with an eviction-oriented cache.

Lua execution is isolated, not rollback-capable. Key types must remain application-owned; Redis OOM or runtime errors after writes can leave partial state. Outbox capacity bounds records per shard, not total memory. Provision memory headroom and alert before exhaustion. The retry cache grows with unique decision rate times twice the window; the dedup set grows with audited event rate times eight days. High-cardinality attacks need ingress protection.

The producer uses all acknowledgments and idempotence. The Redis-to-Kafka bridge remains at least once. Multiple relay instances may reorder or duplicate events; subject partition keys preserve broker append order, not original decision timestamp order. The dashboard is an arrival-ordered operational view, not an accounting ledger.

Projection deduplication is valid only while its eight-day keys survive. Replaying older external archives into existing counters can double count. For a full rebuild, stop projection consumers, use a new projection namespace and consumer group, replay retained Kafka data, validate totals and switch readers. Seven-day Kafka retention cannot reconstruct older lifetime totals. Keep independent archival storage if durable lifetime analytics is required.

The shared bearer credential grants access to all subjects and dashboard data. This is a trusted-service API, not an end-user tenant boundary. Gateway callers must derive subjects from authenticated identities; never forward arbitrary public subject input. The console credential is intended for local development. Production should place operations UI behind a separately authorized backend/SSO layer, rotate service credentials, enforce TLS at ingress, restrict network access, enable Redis ACL/TLS and Kafka SASL/TLS, and use a secret manager. Subject hashes are pseudonymous, not anonymous.

## Production topology

Use multiple Spring Boot replicas, managed Redis with tested persistence/failover, and at least three Kafka brokers. Set topic replication to three and min.insync.replicas to two through broker/topic provisioning. Configure `spring.kafka.properties.*` for SASL/TLS and `spring.data.redis.*` for the actual managed deployment. Hash tags make scripts cluster-slot compatible, but the provided Compose and tests exercise standalone Redis, not cluster failover. A replica count or shard count change requires migration planning. The projection is deliberately a single Redis slot and can become the analytics bottleneck.

Do not claim benchmark numbers without reproducing them on a declared topology. Measure p50/p95/p99 decision latency, throughput, Redis CPU/memory, audit lag and recovery time under representative subject distributions. The current relay publishes sequentially and prioritizes correctness over maximum throughput.

## Failure drills

1. Start the stack and request a permit through the console.
2. `docker compose stop kafka`; continue traffic. Requests can be admitted while backlog capacity remains.
3. Observe rising outbox depth. At capacity, new requests return 503 without spending permits.
4. `docker compose start kafka`; observe backlog draining and audited totals catching up.
5. `docker compose stop redis`; requests must fail with 503, never a permissive fallback.
6. `docker compose start redis`; retry the same request UUID and inspect recovery.

Avoid `docker compose down -v` unless intentionally erasing local test state. A normal `docker compose down` preserves volumes.

## Alerts and response

* Alert on sustained `quota_relay_failures_total`, rising `quota_outbox_depth`, Redis memory pressure, Kafka consumer lag, or request 503s.
* For a stuck relay: check broker reachability, topic ACLs and ISR. Do not discard outbox records to make the graph green.
* Poison Kafka messages retry indefinitely and block the affected partition. Inspect the offending offset, repair the producer/schema, then perform a documented operator-controlled recovery. There is no automatic silent DLT/drop path.
* A policy conflict means mixed replicas/configuration. Roll back the changed replica. To change policy safely, stop admission, drain audits, wait at least twice the old window, update all replicas and reset only shard policy keys during a planned maintenance window. Do not flush Redis.
* Restore Redis and Kafka with a coordinated recovery plan; restoring one independently can duplicate or omit projection updates.

## Scope

One global policy, one trusted service credential, one quota per request. Weighted permits, live policy changes, per-tenant authorization, fair queuing, active-active regions and durable ledger accounting are intentionally outside this release. These are design boundaries, not empty code paths.
