# Security

Do not use production secrets in the local console or commit `.env` files. The API requires a high-entropy bearer credential of at least 32 characters. Every holder is a trusted gateway/operator with access to every subject; there is no per-user authorization model. CSRF is disabled because authentication uses an explicit Authorization header, not cookies. CORS is not enabled; the console uses same-origin proxying.

Local Compose binds only the console to loopback. For deployment, enforce HTTPS, private datastore networks, Redis ACL/TLS, Kafka SASL/TLS, ingress request-size/rate limits, and secret rotation. See the operational runbook for memory and durability assumptions.

Report vulnerabilities through the repository's private security reporting feature if enabled; otherwise contact the maintainer privately before opening a public issue containing exploit details. No response-time SLA is promised.
