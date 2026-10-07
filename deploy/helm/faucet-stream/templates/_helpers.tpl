{{/* Chart name */}}
{{- define "faucet-stream.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* Fully qualified app name */}}
{{- define "faucet-stream.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "faucet-stream.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* Common labels */}}
{{- define "faucet-stream.labels" -}}
helm.sh/chart: {{ include "faucet-stream.chart" . }}
{{ include "faucet-stream.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "faucet-stream.selectorLabels" -}}
app.kubernetes.io/name: {{ include "faucet-stream.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "faucet-stream.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "faucet-stream.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{/* Image ref (repository:tag, tag defaults to appVersion) */}}
{{- define "faucet-stream.image" -}}
{{- $tag := default .Chart.AppVersion .Values.image.tag -}}
{{- printf "%s:%s" .Values.image.repository $tag -}}
{{- end -}}

{{/* Secret names */}}
{{- define "faucet-stream.envSecretName" -}}
{{- default (printf "%s-env" (include "faucet-stream.fullname" .)) .Values.secret.name -}}
{{- end -}}

{{- define "faucet-stream.historySecretName" -}}
{{- printf "%s-serve-history" (include "faucet-stream.fullname" .) -}}
{{- end -}}

{{- define "faucet-stream.authSecretName" -}}
{{- printf "%s-serve-auth" (include "faucet-stream.fullname" .) -}}
{{- end -}}

{{- define "faucet-stream.configMapName" -}}
{{- if .Values.pipelineConfig.existingConfigMap -}}
{{- .Values.pipelineConfig.existingConfigMap -}}
{{- else -}}
{{- printf "%s-config" (include "faucet-stream.fullname" .) -}}
{{- end -}}
{{- end -}}

{{- define "faucet-stream.configFilePath" -}}
{{- printf "%s/%s" (.Values.pipelineConfig.mountPath | trimSuffix "/") .Values.pipelineConfig.fileName -}}
{{- end -}}

{{/*
Stable serve auth token: an explicit value wins; otherwise reuse the token
already stored in the auth Secret (so upgrades don't rotate it); otherwise mint
a fresh random one.
*/}}
{{- define "faucet-stream.serveAuthToken" -}}
{{- if .Values.serve.auth.token -}}
{{- .Values.serve.auth.token -}}
{{- else -}}
{{- $sec := (lookup "v1" "Secret" .Release.Namespace (include "faucet-stream.authSecretName" .)) -}}
{{- if and $sec (hasKey (default dict $sec.data) .Values.serve.auth.existingSecretKey) -}}
{{- index $sec.data .Values.serve.auth.existingSecretKey | b64dec -}}
{{- else -}}
{{- randAlphaNum 40 -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Shared env block for every faucet pod. Renders user env, the chart Secret (if
created), and (for serve token auth) the auth token from its Secret.
Usage: {{- include "faucet-stream.env" . | nindent 12 }}
*/}}
{{- define "faucet-stream.env" -}}
{{- with .Values.env }}
{{ toYaml . }}
{{- end }}
{{- end -}}

{{/*
envFrom sources: user-provided, plus the chart-managed Secret when enabled.
Usage: {{- include "faucet-stream.envFrom" . | nindent 12 }}
*/}}
{{- define "faucet-stream.envFrom" -}}
{{- $srcs := default (list) .Values.envFrom -}}
{{- if .Values.secret.create -}}
{{- $srcs = append $srcs (dict "secretRef" (dict "name" (include "faucet-stream.envSecretName" .))) -}}
{{- end -}}
{{- if $srcs }}
{{ toYaml $srcs }}
{{- end -}}
{{- end -}}

{{/*
Connector-verification initContainer. Runs `faucet schema source|sink <name>`
for every declared connector and fails pod startup if any is not compiled into
the image. No-op (renders nothing) when disabled or no connectors declared.
Usage: {{- include "faucet-stream.verifyInitContainer" . | nindent 8 }}
*/}}
{{- define "faucet-stream.verifyInitContainer" -}}
{{- if and .Values.connectors.verify.enabled (or .Values.connectors.sources .Values.connectors.sinks) }}
- name: verify-connectors
  image: {{ include "faucet-stream.image" . }}
  imagePullPolicy: {{ .Values.image.pullPolicy }}
  command:
    - /bin/sh
    - -c
    - |
      set -eu
      fail=0
      for s in {{ join " " .Values.connectors.sources | default "" }}; do
        if faucet schema source "$s" >/dev/null 2>&1; then
          echo "ok   source-$s"
        else
          echo "MISSING source-$s — not compiled into $(faucet --version)"; fail=1
        fi
      done
      for s in {{ join " " .Values.connectors.sinks | default "" }}; do
        if faucet schema sink "$s" >/dev/null 2>&1; then
          echo "ok   sink-$s"
        else
          echo "MISSING sink-$s — not compiled into $(faucet --version)"; fail=1
        fi
      done
      if [ "$fail" -ne 0 ]; then
        echo "one or more declared connectors are absent from image {{ include "faucet-stream.image" . }}" >&2
        echo "rebuild the image with those features (see Dockerfile / scripts/build-image.sh) or fix connectors: in values" >&2
        exit 1
      fi
      echo "all declared connectors present"
  securityContext:
    {{- toYaml .Values.securityContext | nindent 4 }}
  resources:
    requests: { cpu: 25m, memory: 32Mi }
    limits: { cpu: 250m, memory: 128Mi }
{{- end }}
{{- end -}}
