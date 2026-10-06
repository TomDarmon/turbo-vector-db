# Release process

This document explains how to release each deployable component in `turbo-vector`.

> The release workflows are disabled by default. Follow `ci-cd-setup.md` to
> point them at your own GCP project and enable them.

## Workflow matrix (4 total)

| Workflow | Purpose | Trigger |
|---|---|---|
| `.github/workflows/publish-rc-image.yml` | Build and push RC images (`api`, `broker`, `worker`) | Push tag `image/vX.Y.Z-rc` or `image/vX.Y.Z-rcN` |
| `.github/workflows/publish-rc-helm.yml` | Package and push RC Helm chart OCI artifact | Push tag `helm/vX.Y.Z-rc` or `helm/vX.Y.Z-rcN` |
| `.github/workflows/publish-image.yml` | Promote RC images to final image tags | Manual (`workflow_dispatch`) |
| `.github/workflows/promote-helm.yml` | Promote RC Helm chart tag to final chart tag | Manual (`workflow_dispatch`) |

## Image release

### RC publish (tag-driven)

1. Create an image RC tag:
   - `git tag image/v1.0.1-rc`
   - `git push origin image/v1.0.1-rc`
2. `publish-rc-image.yml` runs and publishes:
   - `turbo-vector-api:v1.0.1-rc`
   - `turbo-vector-broker:v1.0.1-rc`
   - `turbo-vector-worker:v1.0.1-rc`

### Final promote (manual)

1. Run `publish-image.yml`.
2. Provide:
   - `source_rc_tag` (example: `v1.0.1-rc`)
   - `target_tag` (example: `v1.0.1`)
3. Workflow promotes all three images by retagging in Artifact Registry (no rebuild).

## Helm chart release

### RC publish (tag-driven)

1. Create a helm RC tag:
   - `git tag helm/v1.0.1-rc`
   - `git push origin helm/v1.0.1-rc`
2. `publish-rc-helm.yml` runs and:
   - packages `deploy/helm/turbo-vector`,
   - publishes chart OCI artifact to the `GCP_HELM_REPOSITORY` Artifact Registry repo
     with chart version `1.0.1-rc`.

### Final promote (manual)

1. Run `promote-helm.yml`.
2. Provide:
   - `source_rc_tag` (example: `v1.0.1-rc` or `1.0.1-rc`)
   - `target_tag` (example: `v1.0.1` or `1.0.1`)
3. Workflow promotes the chart tag in Artifact Registry without repackaging.

## Notes

- Image tags include the leading `v` (example: `v1.0.1-rc`).
- Helm chart artifact tags are normalized to version tags without the leading
  `v` when stored in Artifact Registry (example: `1.0.1-rc`).
