# Terraform modules

Brings up the full stack from zero (00 §2 internal deliverables): VPC,
multi-AZ Postgres (RDS/Cloud SQL), Redis (ElastiCache/Memorystore, cluster mode
with hash-tag slot pinning), the Kubernetes cluster, and DNS.

Planned modules:

| Module | Purpose |
|---|---|
| `network/` | VPC, subnets across AZs, security groups |
| `postgres/` | Managed Postgres 16, multi-AZ, automated failover |
| `redis/` | Managed Redis 7, cluster mode enabled |
| `cluster/` | Kubernetes control plane + node groups |
| `observability/` | Metrics/traces backend wiring |

Not yet implemented — scaffold only.
