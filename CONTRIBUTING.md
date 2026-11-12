# Contributing

Use Java 21, Maven 3.9+, Node 22, Docker Engine and Compose v2. Run `mvn verify` and `cd ui && npm ci && npm run build` before submitting a pull request.

For changes to admission logic, include a failing concurrency, boundary or failure test that captures the behavior. For delivery changes, document crash points and duplication/loss semantics. Preserve same-slot Redis keys. Do not change shard count, event schema, retention or policy semantics without a migration plan.

No credentials or generated build outputs should be committed. Keep documentation claims tied to reproducible test evidence.
