{{/* vim: set filetype=mustache: */}}
{{/*
Expand the name of the chart.
*/}}
{{- define "strimzi.name" -}}
{{- default "strimzi" .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "strimzi.fullname" -}}
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

{{/*
helper function to complete STRIMZI_NAMESPACE.
*/}}
{{- define "strimzi.watched_namespaces" -}}
{{- if .Values.strimzi.watch_namespaces -}}
{{- $ns := .Values.strimzi.watch_namespaces -}}
{{- $ns := append $ns .Release.Namespace -}}
{{- join "," $ns -}}
{{- else -}}
{{- .Release.Namespace -}}
{{- end -}}
{{- end -}}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "strimzi.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Generate a docker registry prefix or empty string.

NOTE: Not currently being used.  Is this useful?
*/}}
{{- define "dockerRegistryOverride" -}}
{{- if .Values.dockerRegistryOverride -}}
{{- printf "%s/" .Values.image.dockerRegistry -}}
{{- else -}}
{{- "" -}}
{{- end -}}
{{- end -}}

{{- define "imageRepositoryOverride" -}}
{{- .Values.imageRepositoryOverride -}}
{{- end -}}
