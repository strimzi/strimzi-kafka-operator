#!/usr/bin/env bash
set -xe

rm -rf ~/.kube

KUBE_VERSION=${KUBE_VERSION:-1.19.0}
MINIKUBE_REGISTRY_IMAGE=${REGISTRY_IMAGE:-"registry"}
COPY_DOCKER_LOGIN=${COPY_DOCKER_LOGIN:-"false"}

DEFAULT_MINIKUBE_MEMORY=$(free -m | grep "Mem" | awk '{print $2}')
DEFAULT_MINIKUBE_CPU=$(awk '$1~/cpu[0-9]/{usage=($2+$4)*100/($2+$4+$5); print $1": "usage"%"}' /proc/stat | wc -l)

MINIKUBE_MEMORY=${MINIKUBE_MEMORY:-$DEFAULT_MINIKUBE_MEMORY}
MINIKUBE_CPU=${MINIKUBE_CPU:-$DEFAULT_MINIKUBE_CPU}

echo "[INFO] MINIKUBE_MEMORY: ${MINIKUBE_MEMORY}"
echo "[INFO] MINIKUBE_CPU: ${MINIKUBE_CPU}"

ARCH=$1
if [ -z "$ARCH" ]; then
    ARCH="amd64"
fi

function install_kubectl {
    if [ "${TEST_KUBECTL_VERSION:-latest}" = "latest" ]; then
        TEST_KUBECTL_VERSION=$(curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt)
    fi
    curl -Lo kubectl https://storage.googleapis.com/kubernetes-release/release/${TEST_KUBECTL_VERSION}/bin/linux/${ARCH}/kubectl && chmod +x kubectl
    sudo cp kubectl /usr/local/bin
}

function label_node {
	# It should work for all clusters
	for nodeName in $(kubectl get nodes -o custom-columns=:.metadata.name --no-headers);
	do
		echo ${nodeName};
		kubectl label node ${nodeName} rack-key=zone;
	done
}

if [ "$TEST_CLUSTER" = "minikube" ]; then
    install_kubectl
    if [ "${TEST_MINIKUBE_VERSION:-latest}" = "latest" ]; then
        TEST_MINIKUBE_URL=https://storage.googleapis.com/minikube/releases/latest/minikube-linux-${ARCH}
    else
        TEST_MINIKUBE_URL=https://github.com/kubernetes/minikube/releases/download/${TEST_MINIKUBE_VERSION}/minikube-linux-${ARCH}
    fi

    if [ "$KUBE_VERSION" != "latest" ] && [ "$KUBE_VERSION" != "stable" ]; then
        KUBE_VERSION="v${KUBE_VERSION}"
    fi

    curl -Lo minikube ${TEST_MINIKUBE_URL} && chmod +x minikube
    sudo cp minikube /usr/local/bin

    export MINIKUBE_WANTUPDATENOTIFICATION=false
    export MINIKUBE_WANTREPORTERRORPROMPT=false
    export MINIKUBE_HOME=$HOME
    export CHANGE_MINIKUBE_NONE_USER=true

    mkdir $HOME/.kube || true
    touch $HOME/.kube/config

	docker run -d -p 5000:5000 ${MINIKUBE_REGISTRY_IMAGE}

    export KUBECONFIG=$HOME/.kube/config
    # We can turn on network polices support by adding the following options --network-plugin=cni --cni=calico
    # We have to allow trafic for ITS when NPs are turned on
    # We can allow NP after Strimzi#4092 which should fix some issues on STs side
    minikube start --vm-driver=docker --kubernetes-version=${KUBE_VERSION} \
      --insecure-registry=localhost:5000 --extra-config=apiserver.authorization-mode=Node,RBAC \
      --cpus=${MINIKUBE_CPU} --memory=${MINIKUBE_MEMORY}

    if [ $? -ne 0 ]
    then
        echo "Minikube failed to start or RBAC could not be properly set up"
        exit 1
    fi

    minikube addons enable default-storageclass

    # Add Docker hub credentials to Minikube
    if [ "$COPY_DOCKER_LOGIN" = "true" ]
    then
      set +ex

      docker exec "minikube" bash -c "echo '$(cat $HOME/.docker/config.json)'| sudo tee -a /var/lib/kubelet/config.json > /dev/null && sudo systemctl restart kubelet"

      set -ex
    fi

    if [ "$ARCH" = "s390x" ]; then
        git clone -b v1.9.11 --depth 1 https://github.com/kubernetes/kubernetes.git
        sed -i 's/:1.11//' kubernetes/cluster/addons/registry/images/Dockerfile
        docker build --pull -t gcr.io/google_containers/kube-registry-proxy:0.4-s390x kubernetes/cluster/addons/registry/images/
        minikube cache add s390x/registry:2.8.0-beta.1 gcr.io/google_containers/kube-registry-proxy:0.4-s390x
        minikube addons enable registry --images="Registry=s390x/registry:2.8.0-beta.1,KubeRegistryProxy=google_containers/kube-registry-proxy:0.4-s390x"
        rm -rf kubernetes
    else
        minikube addons enable registry
    fi

    minikube addons enable registry-aliases

    kubectl create clusterrolebinding add-on-cluster-admin --clusterrole=cluster-admin --serviceaccount=kube-system:default
else
    echo "Unsupported TEST_CLUSTER '$TEST_CLUSTER'"
    exit 1
fi

label_node
