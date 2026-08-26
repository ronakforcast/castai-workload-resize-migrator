{{/*
Common labels
*/}}
{{- define "castai-workload-resize-migrator.labels" -}}
app.kubernetes.io/name: castai-workload-resize-migrator
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/version: {{ .Chart.AppVersion }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end -}}

{{/*
Selector labels
*/}}
{{- define "castai-workload-resize-migrator.selectorLabels" -}}
app.kubernetes.io/name: castai-workload-resize-migrator
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}
