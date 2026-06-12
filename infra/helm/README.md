# Helm chart

Deploys the service to Kubernetes (00 §7). Stateless app tier — two deployments
off the same image:

- **api** — runs `ans-api`, fronted by a Service + Ingress. HPA on CPU + request
  latency. Readiness gated on `/readyz`.
- **worker** — runs `ans-worker` (the dispatcher). HPA on queue depth
  (`ans_queue_depth`) via a custom/external metric.

Shared config via ConfigMap (the `ANS_*` env vars); secrets (API keys, provider
credentials, DB/Redis URLs) via Secret.

Not yet implemented — scaffold only.
