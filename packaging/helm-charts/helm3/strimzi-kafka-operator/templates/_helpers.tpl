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
Create chart name and version as used by the chart label.
*/}}
{{- define "strimzi.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Creates the image name from the registry, repository, image, tag, and digest
- Priority is given to digests over tags
- Registry, repository, and image will be joined with '/' if values are not blank
- tagSuffix is added to tagPrefix or default tag.  To ignore the suffix, use tag.
- tagSuffix can be ignored by using tag instead of tagPrefix
To use, add the following key/value pairs to the scope:
- "key" [optional]: the key to lookup under .Values for the image map
- "tagSuffix" [optional]: the suffix to add to tagPrefix or the default tag
- Example: `template "strimzi.image" (merge . (dict "key" "cruiseControl" "tagSuffix" "-kafka-3.1.0"))`
*/}}
{{- define "strimzi.image" -}}
{{- $vals := ternary .Values.image (index .Values .key).image (empty .key) -}}
{{- $ref := join "/" (compact (list (default .Values.defaultImageRegistry $vals.registry) (default .Values.defaultImageRepository $vals.repository) (default .Values.defaultImageName $vals.name))) -}}
{{- $tag := join "" (compact (list (coalesce $vals.tag $vals.tagPrefix .Values.defaultImageTag) (ternary .tagSuffix "" (empty $vals.tag)))) -}}
{{- join "" (compact (list $ref (ternary ":" "@" (empty $vals.digest)) (default $tag $vals.digest))) -}}
{{- $_ := unset . "key" -}}
{{- $_ := unset . "tagSuffix" -}}
{{- end -}}

{{/*
 Create a list of comma-separated values corresponding to a given key in a map array.
*/}}
{{- define "strimzi.listPluck" -}}
{{- $pluckedList := list -}}
{{- range .list -}}
{{- $pluckedList = append $pluckedList (get . $.key) -}}
{{- end -}}
{{- join "," $pluckedList -}}
{{- end -}}
