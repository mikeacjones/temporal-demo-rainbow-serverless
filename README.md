# Rainbow deploys

Demo of **Pinned worker versioning** + **elastic order workers** (local or Lambda).
One Workflow type (`CustomerOrder`), five pipeline shapes (`v1`–`v5`). Orders stay
on the version that took them; rollouts only move *new* traffic.

Code to read: [`internal/orders/workflow_v1.go`](internal/orders/workflow_v1.go) …
[`workflow_v5.go`](internal/orders/workflow_v5.go) (one file = one code bundle /
Lambda image), then [`cmd/worker`](cmd/worker) / [`cmd/lambdaworker`](cmd/lambdaworker).
Design notes: [`DESIGN.md`](DESIGN.md).

## Quick start

```bash
make up
open http://localhost:3000
```

| URL | |
| --- | --- |
| Dashboard | http://localhost:3000 |
| Temporal UI | http://localhost:8233 |
| Backend API | http://localhost:8088 |

```bash
make down     # stop + wipe data
make logs
make status
```

## Drive the demo

```bash
make rate N=600
make spike N=5000
make deploy-version V=v3
make break-version V=v4 STEP=Payment
make clear-version V=v4
make rescue V=v4
```

Or use the dashboard: set rate → spike → start deployment → break a version → rescue.

## Local iteration

```bash
make build
make test
make dev    # host processes; logs in .run-logs/
```

Dashboard against host API: http://localhost:3000/?api=http://localhost:8088

After changing control-plane Workflows: `make reset-control`.

## Config (common)

| Variable | Default | Notes |
| --- | --- | --- |
| `TEMPORAL_ADDRESS` | `localhost:7233` | |
| `TEMPORAL_NAMESPACE` | `default` | |
| `TEMPORAL_API_KEY` | — | presence enables TLS (Cloud) |
| `TEMPORAL_DEPLOYMENT_NAME` | `rainbow-orders` | |
| `ORDER_VERSION` | `v1` | worker: which pipeline |
| `ORDER_PROFILE` | `demo` | `fast` (1s) / `demo` (8s) / `heavy` (40s) |

## Temporal Cloud + Lambda

Search attributes (once per namespace):

```bash
temporal operator search-attribute create --name OrderVersion --type Keyword
temporal operator search-attribute create --name OrderStep    --type Keyword
temporal operator search-attribute create --name OrderHealth  --type Keyword
```

Order workers as serverless (one Lambda per version):

```bash
make lambda-build
EXECUTION_ROLE=arn:aws:iam::<account>:role/<role> \
TEMPORAL_ADDRESS=<ns>.<account>.tmprl.cloud:7233 \
TEMPORAL_NAMESPACE=<ns>.<account> \
TEMPORAL_API_KEY=<key> \
  make lambda-deploy
```

Register each function in Cloud UI (**Workers → Serverless**) with Build ID =
version label (`v1`…`v5`). Backend + control plane stay always-on (not Lambda).

Teardown: `make lambda-teardown`. Do **not** set reserved concurrency per function
(it throttles scale-out). Details in [`DESIGN.md`](DESIGN.md).

## Kubernetes

Edit images / Temporal address in [`deploy/k8s/`](deploy/k8s/), then:

```bash
kubectl -n rainbow create secret generic temporal --from-literal=api-key=<key>
make k8s-apply
```

## Layout

| Path | Role |
| --- | --- |
| `internal/orders` | Pinned order Workflow + activities |
| `cmd/worker` | Process worker (`ORDER_VERSION`) |
| `cmd/lambdaworker` | Same pipeline on Lambda |
| `internal/rollout`, `internal/traffic` | Control plane (unversioned) |
| `cmd/controlworker`, `cmd/backend` | Control worker + API |
| `frontend/` | Static dashboard |
