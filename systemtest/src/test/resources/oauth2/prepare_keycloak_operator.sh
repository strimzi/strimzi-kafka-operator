#!/usr/bin/env bash

NAMESPACE=$1

KEYCLOAK_VERSION=11.0.0

SCRIPT_PATH=$(dirname "${BASH_SOURCE[0]}")

echo "[INFO] Deploy Keycloak Operator"
oc apply -f https://github.com/keycloak/keycloak-operator/raw/${KEYCLOAK_VERSION}/deploy/service_account.yaml
oc apply -f https://github.com/keycloak/keycloak-operator/raw/${KEYCLOAK_VERSION}/deploy/role_binding.yaml
oc apply -f https://github.com/keycloak/keycloak-operator/raw/${KEYCLOAK_VERSION}/deploy/role.yaml
curl -s https://raw.githubusercontent.com/keycloak/keycloak-operator/${KEYCLOAK_VERSION}/deploy/cluster_roles/cluster_role_binding.yaml | sed "s#namespace: .*#namespace: ${NAMESPACE}#g" | oc apply -f -
oc apply -f https://github.com/keycloak/keycloak-operator/raw/${KEYCLOAK_VERSION}/deploy/cluster_roles/cluster_role.yaml
oc apply -f https://github.com/keycloak/keycloak-operator/raw/${KEYCLOAK_VERSION}/deploy/crds/keycloak.org_keycloakbackups_crd.yaml
oc apply -f https://github.com/keycloak/keycloak-operator/raw/${KEYCLOAK_VERSION}/deploy/crds/keycloak.org_keycloakclients_crd.yaml
oc apply -f https://github.com/keycloak/keycloak-operator/raw/${KEYCLOAK_VERSION}/deploy/crds/keycloak.org_keycloakclients_crd.yaml
oc apply -f https://github.com/keycloak/keycloak-operator/raw/${KEYCLOAK_VERSION}/deploy/crds/keycloak.org_keycloaks_crd.yaml
oc apply -f https://github.com/keycloak/keycloak-operator/raw/${KEYCLOAK_VERSION}/deploy/crds/keycloak.org_keycloakusers_crd.yaml
oc apply -f https://github.com/keycloak/keycloak-operator/raw/${KEYCLOAK_VERSION}/deploy/operator.yaml

echo "[INFO] Wait for Keycloak Operator readiness"
oc wait deployment/keycloak-operator --for=condition=available --timeout=90s -n ${NAMESPACE}

echo "[INFO] Deploy Keycloak"
oc apply -f https://github.com/keycloak/keycloak-operator/raw/${KEYCLOAK_VERSION}/deploy/examples/keycloak/keycloak.yaml
# To avoid race when pod is not even created when we start to wait, it takes between 1-20s on our envs
sleep 20

echo "[INFO] Wait for Keycloak readiness"
oc wait pod/keycloak-0 --for=condition=containersready --timeout=300s -n ${NAMESPACE}

echo "[INFO] Copy realm scripts"
oc cp ${SCRIPT_PATH}/create_realm.sh keycloak-0:/tmp/create_realm.sh
oc cp ${SCRIPT_PATH}/create_realm_authorization.sh keycloak-0:/tmp/create_realm_authorization.sh

echo "[INFO] Get Admin password"
PASSWORD=$(oc get secret credential-example-keycloak -o=jsonpath='{.data.ADMIN_PASSWORD}' | base64 -d)
USERNAME=$(oc get secret credential-example-keycloak -o=jsonpath='{.data.ADMIN_USERNAME}' | base64 -d)

echo "[INFO] Import realms - USER:${USERNAME} - PASS:${PASSWORD}"
oc exec keycloak-0 -n ${NAMESPACE} -- /tmp/create_realm.sh ${USERNAME} ${PASSWORD} localhost:8443
echo "[INFO] Import of Internal realm finished with: $?"
oc exec keycloak-0 -n ${NAMESPACE} -- /tmp/create_realm_authorization.sh ${USERNAME} ${PASSWORD} localhost:8443
echo "[INFO] Import of Auth realm finished with: $?"
