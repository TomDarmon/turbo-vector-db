# turbo-vector Helm chart

This chart deploys the core runtime roles:

- `api`
- `broker`
- `worker`

And optionally deploys in-cluster `rustfs` for local/dev object storage.

## Validate

```bash
helm lint deploy/helm/turbo-vector
helm template tv deploy/helm/turbo-vector --namespace turbo-vector
```

## Install (local RustFS mode)

```bash
helm upgrade --install tv-local deploy/helm/turbo-vector \
  --kube-context orbstack \
  --namespace turbo-vector \
  --create-namespace \
  --wait --wait-for-jobs \
  --set image.pullPolicy=Never
```

## Install (external S3 mode)

```bash
helm upgrade --install tv-s3 deploy/helm/turbo-vector \
  --namespace turbo-vector \
  --create-namespace \
  --set storage.mode=s3 \
  --set rustfs.enabled=false \
  --set storage.bucket=<bucket> \
  --set storage.s3.endpoint=<endpoint> \
  --set storage.s3.region=<region> \
  --set storage.s3.existingSecret=<secret-with-accessKey-secretKey>
```

## Install (GCS mode, workload identity)

```bash
helm upgrade --install tv-gcs deploy/helm/turbo-vector \
  --namespace turbo-vector \
  --create-namespace \
  --set storage.mode=gcs \
  --set rustfs.enabled=false \
  --set storage.bucket=<bucket> \
  --set storage.gcs.useWorkloadIdentity=true
```

## Fail-fast validation

The chart fails render/install for invalid combinations when `validation.strict=true`:

- `rustfs.enabled=true` with external S3 endpoint/secret settings
- `storage.mode=s3` without credentials source
- `storage.mode=gcs` with conflicting auth modes
- `api.cache.mode=pvc` without PVC sizing/claim
- `broker.replicas>1` without `allowUnsafeBrokerHA=true`

## Port forward on local orbstack stack

```bash
kubectl --context orbstack -n turbo-vector port-forward svc/tv-local-turbo-vector-api 8080:8080
```

```bash
make smoke
