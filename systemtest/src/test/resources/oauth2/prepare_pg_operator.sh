#!/usr/bin/env bash

PGO_INSTANCE_NAMESPACE=$1
PGO_VERSION=$2
PGO_VERSION="${PGO_VERSION:=main}"
PGO_BASEDIR=/tmp/pgo/poe

echo $PGO_VERSION

echo "[INFO] $(date -u +"%Y-%m-%d %H:%M:%S") Deploy Postgres Operator"
[[ -d "${PGO_BASEDIR}" ]] && rm -rf "${PGO_BASEDIR}"
mkdir -p "${PGO_BASEDIR}"
git clone https://github.com/CrunchyData/postgres-operator-examples.git --branch ${PGO_VERSION} "${PGO_BASEDIR}"
for file in $(grep -rin "namespace: postgres-operator" "${PGO_BASEDIR}" | cut -d ":" -f 1); do sed -i "s/namespace: .*/namespace: ${PGO_INSTANCE_NAMESPACE}/" "${file}"; done

kubectl create namespace "${PGO_INSTANCE_NAMESPACE}" || true
kubectl apply --server-side -k "${PGO_BASEDIR}/kustomize/install/default"
# Wait for deployment to show up. If we're too quick, kubectl wait command might fail on non-existing resource
sleep 10

echo "[INFO] $(date -u +"%Y-%m-%d %H:%M:%S") Wait for Postgres Operator readiness"
kubectl wait deployment/pgo --for=condition=available --timeout=90s -n ${PGO_INSTANCE_NAMESPACE}
kubectl wait deployment/pgo-upgrade --for=condition=available --timeout=90s -n ${PGO_INSTANCE_NAMESPACE}


echo "[INFO] $(date -u +"%Y-%m-%d %H:%M:%S") Wait for postgres keycloak instance deployment"
sed 's/name: hippo/name: postgres-kc/' "${PGO_BASEDIR}/kustomize/postgres/postgres.yaml" | kubectl apply -f - -n "${PGO_INSTANCE_NAMESPACE}"

# This is needed to avoid race condition when pods are not created yet before waiting for pods condition
PODS=$(kubectl get pods -n ${PGO_INSTANCE_NAMESPACE})
RETRY=12
while [[ ${PODS} != *"postgres-kc-backup"* && ${RETRY} -gt 0 ]]
do
    echo "[INFO] $(date -u +"%Y-%m-%d %H:%M:%S") postgres-kc-backup pod does not exist! Going to check it in 5 seconds (${RETRY})"
    sleep 10
    PODS=$(kubectl get pods -n ${PGO_INSTANCE_NAMESPACE})
    (( RETRY-=1 ))
done

DB_BACKUP_POD=$(kubectl get pods -l postgres-operator.crunchydata.com/pgbackrest-backup="replica-create" -o jsonpath="{.items[0].metadata.name}" -n ${PGO_INSTANCE_NAMESPACE})
kubectl wait pod/${DB_BACKUP_POD} --for=condition=containersready --timeout=300s -n ${PGO_INSTANCE_NAMESPACE}

PG_POD_NAME=$(kubectl get pods -n "${PGO_INSTANCE_NAMESPACE}" | grep "postgres-kc-instance" | cut -d " " -f 1)
kubectl wait pod/${PG_POD_NAME} --for=condition=containersready --timeout=300s -n ${PGO_INSTANCE_NAMESPACE}

# Wait for existence of a secret
count=0
until kubectl get secret postgres-kc-pguser-postgres-kc -n "${PGO_INSTANCE_NAMESPACE}" || (( count++ >= 10 ))
do
  echo "Wait until secret is created: postgres-kc-pguser-postgres-kc"
  sleep 5
done

echo "[INFO] $(date -u +"%Y-%m-%d %H:%M:%S") Postgres Operator and Database successfully deployed"
