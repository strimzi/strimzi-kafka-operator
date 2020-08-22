#!/usr/bin/env bash

NAMESPACE=$1

KEYCLOAK_VERSION=11.0.0

SCRIPT_PATH=$(dirname "${BASH_SOURCE[0]}")

echo "[INFO] Deploy Keycloak Operator"
oc project ${NAMESPACE}
kubectl apply -f https://github.com/keycloak/keycloak-operator/raw/${KEYCLOAK_VERSION}/deploy/service_account.yaml
kubectl apply -f https://github.com/keycloak/keycloak-operator/raw/${KEYCLOAK_VERSION}/deploy/role_binding.yaml
kubectl apply -f https://github.com/keycloak/keycloak-operator/raw/${KEYCLOAK_VERSION}/deploy/role.yaml
curl -s https://raw.githubusercontent.com/keycloak/keycloak-operator/${KEYCLOAK_VERSION}/deploy/cluster_roles/cluster_role_binding.yaml | sed "s#namespace: .*#namespace: ${NAMESPACE}#g" | kubectl apply -f -
kubectl apply -f https://github.com/keycloak/keycloak-operator/raw/${KEYCLOAK_VERSION}/deploy/cluster_roles/cluster_role.yaml
kubectl apply -f https://github.com/keycloak/keycloak-operator/raw/${KEYCLOAK_VERSION}/deploy/crds/keycloak.org_keycloakbackups_crd.yaml
kubectl apply -f https://github.com/keycloak/keycloak-operator/raw/${KEYCLOAK_VERSION}/deploy/crds/keycloak.org_keycloakclients_crd.yaml
kubectl apply -f https://github.com/keycloak/keycloak-operator/raw/${KEYCLOAK_VERSION}/deploy/crds/keycloak.org_keycloakclients_crd.yaml
kubectl apply -f https://github.com/keycloak/keycloak-operator/raw/${KEYCLOAK_VERSION}/deploy/crds/keycloak.org_keycloaks_crd.yaml
kubectl apply -f https://github.com/keycloak/keycloak-operator/raw/${KEYCLOAK_VERSION}/deploy/crds/keycloak.org_keycloakusers_crd.yaml
kubectl apply -f https://github.com/keycloak/keycloak-operator/raw/${KEYCLOAK_VERSION}/deploy/operator.yaml
echo "[INFO] Deploy Keycloak instance"
kubectl apply -f https://github.com/keycloak/keycloak-operator/raw/${KEYCLOAK_VERSION}/deploy/examples/keycloak/keycloak.yaml

# This is needed to avoid race condition when pods are not created yet before waiting for pods condition
PODS=$(oc get pods)
RETRY=5
while [[ ${PODS} != *"keycloak-0"* && ${RETRY} -gt 0 ]]
do
	sleep 3
	PODS=$(oc get po)
	((RETRY-=1))
done

echo "[INFO] Wait for Keycloak Operator readiness"
kubectl wait deployment/keycloak-operator --for=condition=available --timeout=90s -n ${NAMESPACE}

echo "[INFO] Wait for Keycloak readiness"
kubectl wait pod/keycloak-0 --for=condition=containersready --timeout=300s -n ${NAMESPACE}

echo "[INFO] Copy realm scripts"
kubectl cp ${SCRIPT_PATH}/create_realm.sh keycloak-0:/tmp/create_realm.sh
kubectl cp ${SCRIPT_PATH}/create_realm_authorization.sh keycloak-0:/tmp/create_realm_authorization.sh

echo "[INFO] Get Admin password"
PASSWORD=$(kubectl get secret credential-example-keycloak -o=jsonpath='{.data.ADMIN_PASSWORD}' | base64 -d)
USERNAME=$(kubectl get secret credential-example-keycloak -o=jsonpath='{.data.ADMIN_USERNAME}' | base64 -d)

echo "[INFO] Import realms - USER:${USERNAME} - PASS:${PASSWORD}"
AUTHENTICATION_REALM_OUTPUT=$(kubectl exec keycloak-0 -n ${NAMESPACE} -- /tmp/create_realm.sh ${USERNAME} ${PASSWORD} localhost:8443)
if [[ ${AUTHENTICATION_REALM_OUTPUT} == *"Realm wasn't imported!"* ]]; then
  echo "[ERROR] Authentication realm wasn't imported!"
  exit 1
fi

AUTHORIZATION_REALM_OUTPUT=$(kubectl exec keycloak-0 -n ${NAMESPACE} -- /tmp/create_realm_authorization.sh ${USERNAME} ${PASSWORD} localhost:8443)
if [[ ${AUTHORIZATION_REALM_OUTPUT} == *"Realm wasn't imported!"* ]]; then
  echo "[ERROR] Authorization realm wasn't imported!"
  exit 1
fi

echo "[INFO] All realms were successfully imported!"
