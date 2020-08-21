#!/usr/bin/env bash

NAMESPACE=$1
USERNAME=$2
PASSWORD=$3
URL=$4

SCRIPT_PATH=$(dirname "${BASH_SOURCE[0]}")
KEYCLOAK_VERSION="11.0.0"

echo "[INFO] Create ConfigMap with setup realms scripts "
oc create configmap ${CONFIG_MAP_NAME} --from-file=${SCRIPT_PATH}/create_realm.sh --from-file=${SCRIPT_PATH}/create_realm_authorization.sh

echo "[INFO] Deploy Keycloak"
curl -s https://raw.githubusercontent.com/keycloak/keycloak-quickstarts/${KEYCLOAK_VERSION}/kubernetes-examples/keycloak.yaml | sed "s#namespace: .*#namespace: ${NAMESPACE}#g" | sed "s#type: .*#type: ClusterIP#g" | oc apply -f -

sleep 5

POD_NAME=$(oc get po | grep keycloak | awk '{print $1}')

echo "[INFO] Wait for Keycloak pod ${POD_NAME} readiness"
oc wait pod/${POD_NAME} --for=condition=containersready --timeout=300s -n ${NAMESPACE}

echo "[INFO] Copy realm scripts"
oc cp ${SCRIPT_PATH}/create_realm.sh ${POD_NAME}:/tmp/create_realm.sh
oc cp ${SCRIPT_PATH}/create_realm_authorization.sh ${POD_NAME}:/tmp/create_realm_authorization.sh

echo "[INFO] Import realms - USER:${USERNAME} - PASS:${PASSWORD}"
oc exec ${POD_NAME} -n ${NAMESPACE} -- /tmp/create_realm.sh ${USERNAME} ${PASSWORD} localhost:8443
echo "[INFO] Import of Internal realm finished with: $?"
oc exec ${POD_NAME} -n ${NAMESPACE} -- /tmp/create_realm_authorization.sh ${USERNAME} ${PASSWORD} localhost:8443
echo "[INFO] Import of Auth realm finished with: $?"
