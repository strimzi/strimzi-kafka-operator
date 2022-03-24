#!/usr/bin/env bash

eval $(minikube docker-env)

sed -i 's#kubectl apply -n \${KEYCLOAK_OPERATOR_NAMESPACE} -f https://github.com/keycloak/keycloak-operator/raw/\${KEYCLOAK_VERSION}/deploy/operator.yaml#curl -s https://raw.githubusercontent.com/keycloak/keycloak-operator/\${KEYCLOAK_VERSION}/deploy/operator.yaml | sed "s\#Always\#IfNotPresent\#g" | kubectl apply -n \${KEYCLOAK_OPERATOR_NAMESPACE} -f -#g' systemtest/src/test/resources/oauth2/prepare_keycloak_operator.sh
sed -i 's#kubectl delete -n \${KEYCLOAK_OPERATOR_NAMESPACE} -f https://github.com/keycloak/keycloak-operator/raw/\${KEYCLOAK_VERSION}/deploy/operator.yaml#curl -s https://raw.githubusercontent.com/keycloak/keycloak-operator/\${KEYCLOAK_VERSION}/deploy/operator.yaml | sed "s\#Always\#IfNotPresent\#g" | kubectl delete -n \${KEYCLOAK_OPERATOR_NAMESPACE} -f -#g' systemtest/src/test/resources/oauth2/teardown_keycloak_operator.sh
sed -i '/.*openpolicyagent\/opa.*/ a \          imagePullPolicy: IfNotPresent' systemtest/src/test/resources/opa/opa.yaml 

docker load < $HOME/$S390X_IMAGE_TARBALL_DIR/keycloak-15.0.2.tar.gz
docker load < $HOME/$S390X_IMAGE_TARBALL_DIR/keycloak-init-container-master.tar.gz
docker load < $HOME/$S390X_IMAGE_TARBALL_DIR/keycloak-operator-15.0.2.tar.gz
docker load < $HOME/$S390X_IMAGE_TARBALL_DIR/opa-latest.tar.gz
        