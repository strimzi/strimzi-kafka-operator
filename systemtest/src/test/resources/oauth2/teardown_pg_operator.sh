#!/usr/bin/env bash

PGO_INSTANCE_NAMESPACE=$1
PGO_VERSION=$2
PGO_VERSION="${PGO_VERSION:=main}"
PGO_BASEDIR=/tmp/pgo/poe

if [[ -d "${PGO_BASEDIR}" ]]
then
  echo "No need to clone repository again, just remove from existing files."
else
  echo "Need to download postgres-operator-example and update its deployment"
  mkdir -p ${PGO_BASEDIR}
  git clone https://github.com/CrunchyData/postgres-operator-examples.git --branch ${PGO_VERSION} ${PGO_BASEDIR}
  for file in $(grep -rin "namespace: postgres-operator" ${PGO_BASEDIR} | cut -d ":" -f 1); do sed -i "s/namespace: .*/namespace: ${PGO_INSTANCE_NAMESPACE}/" "${file}"; done
  sed 's/name: hippo/name: postgres-kc/' "${PGO_BASEDIR}/kustomize/postgres/postgres.yaml" | kubectl delete -f - -n "${PGO_INSTANCE_NAMESPACE}" --wait
fi

sed 's/name: hippo/name: postgres-kc/' "${PGO_BASEDIR}/kustomize/postgres/postgres.yaml" | kubectl delete -f - -n "${PGO_INSTANCE_NAMESPACE}" --wait
kubectl delete -k "${PGO_BASEDIR}/kustomize/install/default" -n "${PGO_INSTANCE_NAMESPACE}" --wait
