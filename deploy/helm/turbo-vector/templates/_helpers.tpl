{{- define "turbo-vector.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "turbo-vector.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := include "turbo-vector.name" . -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "turbo-vector.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "turbo-vector.labels" -}}
helm.sh/chart: {{ include "turbo-vector.chart" . }}
app.kubernetes.io/name: {{ include "turbo-vector.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "turbo-vector.selectorLabels" -}}
app.kubernetes.io/name: {{ include "turbo-vector.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "turbo-vector.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "turbo-vector.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "turbo-vector.apiServiceName" -}}
{{- printf "%s-api" (include "turbo-vector.fullname" .) -}}
{{- end -}}

{{- define "turbo-vector.brokerServiceName" -}}
{{- printf "%s-broker" (include "turbo-vector.fullname" .) -}}
{{- end -}}

{{- define "turbo-vector.rustfsServiceName" -}}
{{- printf "%s-rustfs" (include "turbo-vector.fullname" .) -}}
{{- end -}}

{{- define "turbo-vector.storageMode" -}}
{{- .Values.storage.mode | default "rustfs" | lower -}}
{{- end -}}

{{- define "turbo-vector.storageProvider" -}}
{{- if eq (include "turbo-vector.storageMode" .) "rustfs" -}}
s3
{{- else -}}
{{- include "turbo-vector.storageMode" . -}}
{{- end -}}
{{- end -}}

{{- define "turbo-vector.storageEndpoint" -}}
{{- $mode := include "turbo-vector.storageMode" . -}}
{{- if eq $mode "rustfs" -}}
{{- printf "http://%s:%v" (include "turbo-vector.rustfsServiceName" .) .Values.rustfs.service.port -}}
{{- else if eq $mode "s3" -}}
{{- .Values.storage.s3.endpoint -}}
{{- else -}}
{{- .Values.storage.gcs.endpoint -}}
{{- end -}}
{{- end -}}

{{- define "turbo-vector.storageRegion" -}}
{{- $mode := include "turbo-vector.storageMode" . -}}
{{- if eq $mode "rustfs" -}}
us-east-1
{{- else if eq $mode "s3" -}}
{{- .Values.storage.s3.region -}}
{{- else -}}
{{- .Values.storage.gcs.region -}}
{{- end -}}
{{- end -}}

{{- define "turbo-vector.storageS3CredentialsSecretName" -}}
{{- $mode := include "turbo-vector.storageMode" . -}}
{{- if eq $mode "rustfs" -}}
{{- printf "%s-rustfs-credentials" (include "turbo-vector.fullname" .) -}}
{{- else if .Values.storage.s3.existingSecret -}}
{{- .Values.storage.s3.existingSecret -}}
{{- else -}}
{{- printf "%s-s3-credentials" (include "turbo-vector.fullname" .) -}}
{{- end -}}
{{- end -}}

{{- define "turbo-vector.createS3CredentialsSecret" -}}
{{- $mode := include "turbo-vector.storageMode" . -}}
{{- if eq $mode "rustfs" -}}
true
{{- else if and (eq $mode "s3") (not .Values.storage.s3.existingSecret) -}}
true
{{- else -}}
false
{{- end -}}
{{- end -}}

{{- define "turbo-vector.gcsCredentialsEnabled" -}}
{{- if and (eq (include "turbo-vector.storageMode" .) "gcs") (not .Values.storage.gcs.useWorkloadIdentity) -}}
true
{{- else -}}
false
{{- end -}}
{{- end -}}

{{- define "turbo-vector.gcsCredentialsVolumeName" -}}
gcs-credentials
{{- end -}}

{{- define "turbo-vector.apiCachePvcName" -}}
{{- printf "%s-api-cache" (include "turbo-vector.fullname" .) -}}
{{- end -}}

{{- define "turbo-vector.validate" -}}
{{- if .Values.validation.strict }}
{{- $mode := include "turbo-vector.storageMode" . -}}
{{- $rustfsEnabled := .Values.rustfs.enabled -}}
{{- $s3Endpoint := .Values.storage.s3.endpoint | default "" | trim -}}
{{- $s3ExistingSecret := .Values.storage.s3.existingSecret | default "" | trim -}}
{{- $s3AccessKey := .Values.storage.s3.accessKey | default "" | trim -}}
{{- $s3SecretKey := .Values.storage.s3.secretKey | default "" | trim -}}
{{- $s3InlineCreds := and (ne $s3AccessKey "") (ne $s3SecretKey "") -}}
{{- $gcsUseWI := .Values.storage.gcs.useWorkloadIdentity -}}
{{- $gcsSecretName := .Values.storage.gcs.credentialsSecretName | default "" | trim -}}
{{- $apiCacheMode := .Values.api.cache.mode | default "emptyDir" -}}
{{- $apiCacheExistingClaim := .Values.api.cache.pvc.existingClaim | default "" | trim -}}
{{- $apiCacheSize := .Values.api.cache.pvc.size | default "" | trim -}}
{{- $apiCacheRequireStorageClass := .Values.api.cache.pvc.requireStorageClass | default false -}}
{{- $apiCacheStorageClass := .Values.api.cache.pvc.storageClass | default "" | trim -}}

{{- if and $rustfsEnabled (ne $mode "rustfs") -}}
{{- fail "invalid values: rustfs.enabled=true requires storage.mode=rustfs" -}}
{{- end -}}
{{- if and (eq $mode "rustfs") (not $rustfsEnabled) -}}
{{- fail "invalid values: storage.mode=rustfs requires rustfs.enabled=true" -}}
{{- end -}}
{{- if and $rustfsEnabled (or (ne $s3Endpoint "") (ne $s3ExistingSecret "") (ne $s3AccessKey "") (ne $s3SecretKey "")) -}}
{{- fail "invalid values: rustfs.enabled=true cannot be combined with external storage.s3 endpoint/secret/inline credentials" -}}
{{- end -}}

{{- if eq $mode "s3" -}}
{{- if eq ($s3Endpoint | trim) "" -}}
{{- fail "invalid values: storage.mode=s3 requires storage.s3.endpoint" -}}
{{- end -}}
{{- if eq ((.Values.storage.s3.region | default "" | trim)) "" -}}
{{- fail "invalid values: storage.mode=s3 requires storage.s3.region" -}}
{{- end -}}
{{- if and (eq $s3ExistingSecret "") (not $s3InlineCreds) -}}
{{- fail "invalid values: storage.mode=s3 requires either storage.s3.existingSecret or inline storage.s3.accessKey+secretKey" -}}
{{- end -}}
{{- if and (ne $s3ExistingSecret "") $s3InlineCreds -}}
{{- fail "invalid values: choose either storage.s3.existingSecret OR inline storage.s3.accessKey+secretKey, not both" -}}
{{- end -}}
{{- end -}}

{{- if eq $mode "gcs" -}}
{{- if eq ((.Values.storage.gcs.endpoint | default "" | trim)) "" -}}
{{- fail "invalid values: storage.mode=gcs requires storage.gcs.endpoint" -}}
{{- end -}}
{{- if eq ((.Values.storage.gcs.region | default "" | trim)) "" -}}
{{- fail "invalid values: storage.mode=gcs requires storage.gcs.region" -}}
{{- end -}}
{{- if and $gcsUseWI (ne $gcsSecretName "") -}}
{{- fail "invalid values: storage.mode=gcs requires exactly one auth mode; useWorkloadIdentity=true cannot be combined with credentialsSecretName" -}}
{{- end -}}
{{- if and (not $gcsUseWI) (eq $gcsSecretName "") -}}
{{- fail "invalid values: storage.mode=gcs with useWorkloadIdentity=false requires storage.gcs.credentialsSecretName" -}}
{{- end -}}
{{- end -}}

{{- if eq $apiCacheMode "pvc" -}}
{{- if and (eq $apiCacheExistingClaim "") (eq $apiCacheSize "") -}}
{{- fail "invalid values: api.cache.mode=pvc requires api.cache.pvc.existingClaim or api.cache.pvc.size" -}}
{{- end -}}
{{- if and $apiCacheRequireStorageClass (eq $apiCacheStorageClass "") (eq $apiCacheExistingClaim "") -}}
{{- fail "invalid values: api.cache.pvc.requireStorageClass=true requires api.cache.pvc.storageClass when creating a claim" -}}
{{- end -}}
{{- end -}}

{{- if and (gt (int .Values.broker.replicas) 1) (not .Values.allowUnsafeBrokerHA) -}}
{{- fail "invalid values: broker.replicas>1 is blocked unless allowUnsafeBrokerHA=true" -}}
{{- end -}}

{{- if gt (int .Values.runtime.distributedRequiredSuccessfulShards) (int .Values.runtime.distributedShardCount) -}}
{{- fail "invalid values: runtime.distributedRequiredSuccessfulShards must be <= runtime.distributedShardCount" -}}
{{- end -}}
{{- end -}}
{{- end -}}
