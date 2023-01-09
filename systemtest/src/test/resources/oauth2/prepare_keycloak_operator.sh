#!/usr/bin/env bash

KEYCLOAK_OPERATOR_NAMESPACE=$1
KEYCLOAK_VERSION=$2
KEYCLOAK_INSTANCE_NAMESPACE=$3

SCRIPT_PATH=$(dirname "${BASH_SOURCE[0]}")

echo "[INFO] $(date -u +"%Y-%m-%d %H:%M:%S") Generate keycloak secret"
mkdir -p /tmp/keycloak
openssl req  -nodes -new -x509  -keyout /tmp/keycloak/keycloak.key -out /tmp/keycloak/keycloak.crt -subj '/CN=keycloak'
kubectl create secret -n ${KEYCLOAK_OPERATOR_NAMESPACE} generic sso-x509-https-secret --from-file=tls.crt=/tmp/keycloak/keycloak.crt --from-file=tls.key=/tmp/keycloak/keycloak.key

echo "[INFO] $(date -u +"%Y-%m-%d %H:%M:%S") Deploy Keycloak Operator"
kubectl apply -n ${KEYCLOAK_OPERATOR_NAMESPACE} -f https://github.com/keycloak/keycloak-operator/raw/${KEYCLOAK_VERSION}/deploy/service_account.yaml
kubectl apply -n ${KEYCLOAK_OPERATOR_NAMESPACE} -f https://github.com/keycloak/keycloak-operator/raw/${KEYCLOAK_VERSION}/deploy/role_binding.yaml
kubectl apply -n ${KEYCLOAK_OPERATOR_NAMESPACE} -f https://github.com/keycloak/keycloak-operator/raw/${KEYCLOAK_VERSION}/deploy/role.yaml
curl -s https://raw.githubusercontent.com/keycloak/keycloak-operator/${KEYCLOAK_VERSION}/deploy/cluster_roles/cluster_role_binding.yaml | sed "s#namespace: .*#namespace: ${KEYCLOAK_OPERATOR_NAMESPACE}#g" | kubectl apply  -n ${KEYCLOAK_OPERATOR_NAMESPACE} -f -
kubectl apply -n ${KEYCLOAK_OPERATOR_NAMESPACE} -f https://github.com/keycloak/keycloak-operator/raw/${KEYCLOAK_VERSION}/deploy/cluster_roles/cluster_role.yaml
kubectl apply -n ${KEYCLOAK_OPERATOR_NAMESPACE} -f https://github.com/keycloak/keycloak-operator/raw/${KEYCLOAK_VERSION}/deploy/crds/keycloak.org_keycloakbackups_crd.yaml
kubectl apply -n ${KEYCLOAK_OPERATOR_NAMESPACE} -f https://github.com/keycloak/keycloak-operator/raw/${KEYCLOAK_VERSION}/deploy/crds/keycloak.org_keycloakclients_crd.yaml
kubectl apply -n ${KEYCLOAK_OPERATOR_NAMESPACE} -f https://github.com/keycloak/keycloak-operator/raw/${KEYCLOAK_VERSION}/deploy/crds/keycloak.org_keycloakrealms_crd.yaml
kubectl apply -n ${KEYCLOAK_OPERATOR_NAMESPACE} -f https://github.com/keycloak/keycloak-operator/raw/${KEYCLOAK_VERSION}/deploy/crds/keycloak.org_keycloaks_crd.yaml
kubectl apply -n ${KEYCLOAK_OPERATOR_NAMESPACE} -f https://github.com/keycloak/keycloak-operator/raw/${KEYCLOAK_VERSION}/deploy/crds/keycloak.org_keycloakusers_crd.yaml
kubectl apply -n ${KEYCLOAK_OPERATOR_NAMESPACE} -f https://github.com/keycloak/keycloak-operator/raw/${KEYCLOAK_VERSION}/deploy/operator.yaml


