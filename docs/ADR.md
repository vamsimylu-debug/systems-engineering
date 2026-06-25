# ADR-001: Effectively-Once Charging on At-Least-Once Delivery

## Status
Accepted

## Context
The system is an order to payment pipeline with three realities that make naive consumers unsafe:

1. Duplicate deliveries are expected (upstream retries and intentional duplicate order sends).
2. The worker can restart while there are in-flight messages.
3. The downstream payments API is flaky (500s and occasional long latency/hangs).

The prototype worker used `XREAD` with local offset state (`last_id`) and processed in a single step:
call payments, then increment ledger. That design has critical failure modes:

1. Lost orders on restart because progress tracking lived in-process.
2. Double charging from duplicate deliveries because there was no idempotency key/state.
3. Fragile failure handling because transient downstream errors could stop forward progress.
4. No ownership transfer for in-flight work when a consumer dies.

## Decision

### Delivery and consistency semantics
Chose at-least-once message delivery with effectively-once business effects.

1. Delivery: Redis Streams consumer groups with pending-entry tracking (`XREADGROUP`).
2. Recovery: stalled pending messages reclaimed (`XAUTOCLAIM`) by active workers.
3. Business correctness: each `order_id` contributes to the ledger at most once.

Rationale: true exactly-once across broker + downstream + datastore is not realistic here without distributed transactions and provider-side idempotency guarantees. Effectively-once at the ledger boundary is the practical target that satisfies acceptance correctness.

### Idempotency strategy
Idempotency key is `order_id`. State lives in Redis hash per order (`order_state:{order_id}`) with status transitions:

1. `charged` after a successful provider response.
2. `done` only after ledger increment + processed counter update + status transition are committed atomically.

Races avoided:

1. Concurrent consumers handling duplicate messages: guarded by short-lived per-order lock (`SET NX EX`).
2. Partial effects (ledger incremented twice): ledger apply is conditional and guarded using optimistic transaction (`WATCH/MULTI/EXEC`), so a `done` order cannot be re-applied.

Outcome: duplicate message deliveries become no-ops after first successful completion.

### Failure handling policy
Policy separates transient from permanent failure classes:

1. Payments call failures/timeouts are treated as transient and retried with bounded exponential backoff + jitter and explicit request timeout.
2. If retries are exhausted, message is intentionally left unacked so it remains pending and can be retried later by the same or another consumer.
3. Invalid/poison payloads are treated as permanent; message is logged and acked so one bad message does not block progress.

This preserves correctness under flaky dependencies while preventing stream stalls from malformed data.

## Tradeoffs and alternatives

### Build vs adopt: Redis Streams vs Kafka / SQS / managed broker
Redis Streams is acceptable for this take-home scope and moderate internal throughput because it is easy to operate locally and provides consumer groups with pending replay.

I would switch when one or more become dominant:

1. Durability and replay retention become first-class requirements (audit-grade event retention).
2. Throughput/partition scale exceeds what a single Redis instance can comfortably sustain.
3. Multi-team ownership and schema evolution need stronger contracts/tooling.
4. Operations burden of self-managed broker reliability rises.

Likely choices:

1. Kafka (or managed Kafka) for high-throughput, durable event streaming and long retention.
2. SQS/SNS (or equivalent managed queue/pub-sub) for simpler operations with strong cloud integration when strict ordering/replay requirements are lighter.

### From CI to CD
Path from current CI gate to CD:

1. Build immutable images in CI and push to registry with commit SHA tags.
2. Promote by digest across environments (`dev` -> `stage` -> `prod`) instead of rebuilding.
3. Add deployment controller with progressive rollout:
	- canary for worker and producer (safe under live traffic)
	- automatic rollback on SLO breach (error rate, queue lag, ledger drift alerts)
4. Add environment-specific config/secrets management and policy checks.
5. Prefer GitOps (ArgoCD/Flux) for auditable, declarative environment state and promotion PR workflow.

### Scaling to 100x
First bottleneck is usually external payments latency/failure behavior, then Redis contention hotspots.

Expected sequence and changes:

1. Payments bottleneck: raise worker concurrency, connection pooling, backpressure, and per-customer/order circuit-breakers.
2. Redis hot keys/locks: shard or partition order space, reduce lock TTL windows, batch acknowledgements where safe.
3. Stream and consumer scaling: multiple workers/consumers, tune claim/read batch sizes, lag observability.
4. If Redis durability/throughput limits dominate, migrate to Kafka/managed broker and keep idempotent consumer logic.

## Consequences
What improved:

1. No ledger double-charge from duplicate messages.
2. No silent order loss on worker restart due to consumer-group pending recovery.
3. Better resilience to transient payment failures via bounded retries/timeouts.

Remaining weaknesses / next steps:

1. No dead-letter queue with explicit max-redelivery policy yet.
2. No provider-side idempotency key contract; if provider charges twice after unknown response states, reconciliation strategy is still needed.
3. Limited observability (need metrics/traces/alerts for pending depth, retry rates, and processing latency).
