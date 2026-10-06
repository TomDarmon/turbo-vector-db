# CI/CD setup (optional)

The release workflows in `.github/workflows/` are **disabled by default**. Every
job is gated on `if: ${{ vars.CICD_ENABLED == 'true' }}`, so pushing tags or
triggering them manually is a no-op until you opt in.

They publish container images and the Helm chart to Google Artifact Registry
using GitHub OIDC (Workload Identity Federation). See `release-process.md` for
the release flow itself.

## 1. Prepare your GCP project

Replace the placeholders with your own values.

```bash
PROJECT_ID=<your-gcp-project-id>
REGION=<your-gcp-region>            # e.g. us-central1
DOCKER_REPO=<your-docker-repository>
HELM_REPO=<your-helm-repository>
GITHUB_REPO=<your-github-owner>/<your-github-repo>

gcloud services enable artifactregistry.googleapis.com iamcredentials.googleapis.com \
  --project "$PROJECT_ID"

gcloud artifacts repositories create "$DOCKER_REPO" \
  --repository-format=docker --location="$REGION" --project "$PROJECT_ID"
gcloud artifacts repositories create "$HELM_REPO" \
  --repository-format=docker --location="$REGION" --project "$PROJECT_ID"
```

## 2. Create a deploy service account and Workload Identity Federation

```bash
gcloud iam service-accounts create github-release --project "$PROJECT_ID"
SA="github-release@${PROJECT_ID}.iam.gserviceaccount.com"

gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member "serviceAccount:${SA}" --role roles/artifactregistry.writer

gcloud iam workload-identity-pools create github --location=global --project "$PROJECT_ID"
gcloud iam workload-identity-pools providers create-oidc github \
  --location=global --workload-identity-pool=github --project "$PROJECT_ID" \
  --issuer-uri="https://token.actions.githubusercontent.com" \
  --attribute-mapping="google.subject=assertion.sub,attribute.repository=assertion.repository" \
  --attribute-condition="assertion.repository=='${GITHUB_REPO}'"

PROJECT_NUMBER="$(gcloud projects describe "$PROJECT_ID" --format='value(projectNumber)')"
gcloud iam service-accounts add-iam-policy-binding "$SA" --project "$PROJECT_ID" \
  --role roles/iam.workloadIdentityUser \
  --member "principalSet://iam.googleapis.com/projects/${PROJECT_NUMBER}/locations/global/workloadIdentityPools/github/attribute.repository/${GITHUB_REPO}"
```

## 3. Configure the GitHub repository

Secrets (`Settings -> Secrets and variables -> Actions -> Secrets`):

| Secret | Value |
|---|---|
| `GCP_WORKLOAD_IDENTITY_PROVIDER` | `projects/<project-number>/locations/global/workloadIdentityPools/github/providers/github` |
| `GCP_SERVICE_ACCOUNT` | `github-release@<your-gcp-project-id>.iam.gserviceaccount.com` |

Variables (`Settings -> Secrets and variables -> Actions -> Variables`):

| Variable | Example |
|---|---|
| `GCP_PROJECT_ID` | `<your-gcp-project-id>` |
| `GCP_REGION` | `<your-gcp-region>` |
| `GCP_DOCKER_REPOSITORY` | `<your-docker-repository>` |
| `GCP_HELM_REPOSITORY` | `<your-helm-repository>` |
| `CICD_ENABLED` | `true` (set last; this switches the workflows on) |

Or with the GitHub CLI:

```bash
gh secret set GCP_WORKLOAD_IDENTITY_PROVIDER --body "projects/${PROJECT_NUMBER}/locations/global/workloadIdentityPools/github/providers/github"
gh secret set GCP_SERVICE_ACCOUNT --body "$SA"
gh variable set GCP_PROJECT_ID --body "$PROJECT_ID"
gh variable set GCP_REGION --body "$REGION"
gh variable set GCP_DOCKER_REPOSITORY --body "$DOCKER_REPO"
gh variable set GCP_HELM_REPOSITORY --body "$HELM_REPO"
gh variable set CICD_ENABLED --body true
```

## 4. Verify

Push an RC tag (`git tag image/v0.1.0-rc && git push origin image/v0.1.0-rc`)
and check the `Publish RC images` run in the Actions tab.

To disable CI/CD again, delete the `CICD_ENABLED` variable or set it to
anything other than `true`.

## Using another registry or CI system

The workflows only depend on GCP for authentication and the registry host. To
target another registry (GHCR, Docker Hub, ECR), replace the
`google-github-actions/*` steps with that registry's login action and point
`GCP_ARTIFACT_REGISTRY_REPOSITORY` / the Helm OCI URL at the new host.
