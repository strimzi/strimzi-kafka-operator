#!/usr/bin/env bash

KEYCLOAK_OPERATOR_NAMESPACE=$1
KEYCLOAK_VERSION=$2
KEYCLOAK_INSTANCE_NAMESPACE=$3

SCRIPT_PATH=$(dirname "${BASH_SOURCE[0]}")

echo "[INFO] $(date -u +"%Y-%m-%d %H:%M:%S") Generate keycloak secret"
mkdir -p /tmp/keycloak

openssl req -subj '/CN=test.keycloak.org/O=Test Keycloak./C=US' -newkey rsa:2048 -nodes -keyout /tmp/keycloak/key.pem -x509 -days 365 -out /tmp/keycloak/certificate.pem

kubectl create secret tls example-tls-secret --cert /tmp/keycloak/certificate.pem --key /tmp/keycloak/key.pem -n ${KEYCLOAK_OPERATOR_NAMESPACE}

echo "[INFO] $(date -u +"%Y-%m-%d %H:%M:%S") Deploy Keycloak Operator"

kubectl apply -n ${KEYCLOAK_OPERATOR_NAMESPACE} -f https://raw.githubusercontent.com/keycloak/keycloak-k8s-resources/${KEYCLOAK_VERSION}/kubernetes/keycloaks.k8s.keycloak.org-v1.yml
kubectl apply -n ${KEYCLOAK_OPERATOR_NAMESPACE} -f https://raw.githubusercontent.com/keycloak/keycloak-k8s-resources/${KEYCLOAK_VERSION}/kubernetes/keycloakrealmimports.k8s.keycloak.org-v1.yml
curl -s https://raw.githubusercontent.com/keycloak/keycloak-k8s-resources/${KEYCLOAK_VERSION}/kubernetes/kubernetes.yml | sed "s#namespace: .*#namespace: ${KEYCLOAK_OPERATOR_NAMESPACE}#g" | kubectl apply  -n ${KEYCLOAK_OPERATOR_NAMESPACE} -f -



