#!/usr/bin/env bash

eval $(minikube docker-env)

sed -i 's#kubernetes.yml#kubernetes.yml | sed "s\#Always\#IfNotPresent\#g"#g' systemtest/src/test/resources/oauth2/prepare_keycloak_operator.sh
sed -i 's#kubectl delete -n ${KEYCLOAK_OPERATOR_NAMESPACE} -f https://raw.githubusercontent.com/keycloak/keycloak-k8s-resources/${KEYCLOAK_VERSION}/kubernetes/kubernetes.yml#curl -s https://raw.githubusercontent.com/keycloak/keycloak-k8s-resources/${KEYCLOAK_VERSION}/kubernetes/kubernetes.yml | sed "s\#Always\#IfNotPresent\#g" | kubectl delete -n \${KEYCLOAK_OPERATOR_NAMESPACE} -f -#g' systemtest/src/test/resources/oauth2/teardown_keycloak_operator.sh
sed -i '/.*openpolicyagent\/opa.*/ a \          imagePullPolicy: IfNotPresent' systemtest/src/test/resources/opa/opa.yaml

docker load < $HOME/$PPC64LE_IMAGE_TARBALL_DIR/keycloak-21.0.0.tar.gz
docker load < $HOME/$PPC64LE_IMAGE_TARBALL_DIR/keycloak-init-container-master.tar.gz
docker load < $HOME/$PPC64LE_IMAGE_TARBALL_DIR/keycloak-operator-21.0.0.tar.gz
docker load < $HOME/$PPC64LE_IMAGE_TARBALL_DIR/opa-latest.tar.gz
