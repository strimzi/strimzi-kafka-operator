#!/usr/bin/env bash

NAMESPACE=$1

KEYCLOAK_VERSION=11.0.0

SCRIPT_PATH=$(dirname "${BASH_SOURCE[0]}")

echo "[INFO] $(date -u +"%Y-%m-%d %H:%M:%S") Generate keycloak secret"
openssl req  -nodes -new -x509  -keyout keycloak.key -out keycloak.crt -subj '/CN=keycloak'
kubectl create secret -n ${NAMESPACE} generic sso-x509-https-secret --from-file=tls.crt=keycloak.crt --from-file=tls.key=keycloak.key

echo "[INFO] $(date -u +"%Y-%m-%d %H:%M:%S") Deploy Keycloak Operator"
kubectl apply -n ${NAMESPACE} -f https://github.com/keycloak/keycloak-operator/raw/${KEYCLOAK_VERSION}/deploy/service_account.yaml
kubectl apply -n ${NAMESPACE} -f https://github.com/keycloak/keycloak-operator/raw/${KEYCLOAK_VERSION}/deploy/role_binding.yaml
kubectl apply -n ${NAMESPACE} -f https://github.com/keycloak/keycloak-operator/raw/${KEYCLOAK_VERSION}/deploy/role.yaml
curl -s https://raw.githubusercontent.com/keycloak/keycloak-operator/${KEYCLOAK_VERSION}/deploy/cluster_roles/cluster_role_binding.yaml | sed "s#namespace: .*#namespace: ${NAMESPACE}#g" | kubectl apply  -n ${NAMESPACE} -f -
kubectl apply -n ${NAMESPACE} -f https://github.com/keycloak/keycloak-operator/raw/${KEYCLOAK_VERSION}/deploy/cluster_roles/cluster_role.yaml
kubectl apply -n ${NAMESPACE} -f https://github.com/keycloak/keycloak-operator/raw/${KEYCLOAK_VERSION}/deploy/crds/keycloak.org_keycloakbackups_crd.yaml
kubectl apply -n ${NAMESPACE} -f https://github.com/keycloak/keycloak-operator/raw/${KEYCLOAK_VERSION}/deploy/crds/keycloak.org_keycloakclients_crd.yaml
kubectl apply -n ${NAMESPACE} -f https://github.com/keycloak/keycloak-operator/raw/${KEYCLOAK_VERSION}/deploy/crds/keycloak.org_keycloakrealms_crd.yaml
kubectl apply -n ${NAMESPACE} -f https://github.com/keycloak/keycloak-operator/raw/${KEYCLOAK_VERSION}/deploy/crds/keycloak.org_keycloaks_crd.yaml
kubectl apply -n ${NAMESPACE} -f https://github.com/keycloak/keycloak-operator/raw/${KEYCLOAK_VERSION}/deploy/crds/keycloak.org_keycloakusers_crd.yaml
kubectl apply -n ${NAMESPACE} -f https://github.com/keycloak/keycloak-operator/raw/${KEYCLOAK_VERSION}/deploy/operator.yaml
echo "[INFO] $(date -u +"%Y-%m-%d %H:%M:%S") Deploy Keycloak instance"
kubectl apply -n ${NAMESPACE} -f https://github.com/keycloak/keycloak-operator/raw/${KEYCLOAK_VERSION}/deploy/examples/keycloak/keycloak.yaml

# This is needed to avoid race condition when pods are not created yet before waiting for pods condition
PODS=$(kubectl get pods -n ${NAMESPACE})
RETRY=10
while [[ ${PODS} != *"keycloak-0"* && ${RETRY} -gt 0 ]]
do
	echo "[INFO] $(date -u +"%Y-%m-%d %H:%M:%S") keycloak-0 does not exists! Going to check it in 5 seconds (${RETRY})"
	sleep 5
	PODS=$(kubectl get po -n ${NAMESPACE})
	((RETRY-=1))
done

echo "[INFO] $(date -u +"%Y-%m-%d %H:%M:%S") Wait for Keycloak Operator readiness"
kubectl wait deployment/keycloak-operator --for=condition=available --timeout=90s -n ${NAMESPACE}

echo "[INFO] $(date -u +"%Y-%m-%d %H:%M:%S") Wait for Keycloak readiness"
kubectl wait pod/keycloak-0 --for=condition=containersready --timeout=300s -n ${NAMESPACE}

echo "[INFO] $(date -u +"%Y-%m-%d %H:%M:%S") Copy realm scripts"
kubectl cp  -n ${NAMESPACE} ${SCRIPT_PATH}/create_realm.sh keycloak-0:/tmp/create_realm.sh
kubectl cp  -n ${NAMESPACE} ${SCRIPT_PATH}/create_realm_authorization.sh keycloak-0:/tmp/create_realm_authorization.sh

echo "[INFO] $(date -u +"%Y-%m-%d %H:%M:%S") Get Admin password"
PASSWORD=$(kubectl get secret -n ${NAMESPACE} credential-example-keycloak -o=jsonpath='{.data.ADMIN_PASSWORD}' | base64 -d)
USERNAME=$(kubectl get secret -n ${NAMESPACE} credential-example-keycloak -o=jsonpath='{.data.ADMIN_USERNAME}' | base64 -d)

echo "[INFO] $(date -u +"%Y-%m-%d %H:%M:%S") Import realms - USER:${USERNAME} - PASS:${PASSWORD}"
AUTHENTICATION_REALM_OUTPUT=$(kubectl exec keycloak-0 -n ${NAMESPACE} -- /tmp/create_realm.sh ${USERNAME} ${PASSWORD} localhost:8443)
echo ${AUTHENTICATION_REALM_OUTPUT}
if [[ ${AUTHENTICATION_REALM_OUTPUT} == *"Realm wasn't imported!"* ]]; then
  echo "[ERROR] $(date -u +"%Y-%m-%d %H:%M:%S") Authentication realm wasn't imported!"
  exit 1
fi

AUTHORIZATION_REALM_OUTPUT=$(kubectl exec keycloak-0 -n ${NAMESPACE} -- /tmp/create_realm_authorization.sh ${USERNAME} ${PASSWORD} localhost:8443)
if [[ ${AUTHORIZATION_REALM_OUTPUT} == *"Realm wasn't imported!"* ]]; then
  echo "[ERROR] $(date -u +"%Y-%m-%d %H:%M:%S") Authorization realm wasn't imported!"
  exit 1
fi

echo "[INFO] $(date -u +"%Y-%m-%d %H:%M:%S") All realms were successfully imported!"
