#!/usr/bin/env bash
set -x

rm -rf ~/.kube

KUBE_VERSION=${KUBE_VERSION:-1.16.0}

function install_kubectl {
    if [ "${TEST_KUBECTL_VERSION:-latest}" = "latest" ]; then
        TEST_KUBECTL_VERSION=$(curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt)
    fi
    curl -Lo kubectl https://storage.googleapis.com/kubernetes-release/release/${TEST_KUBECTL_VERSION}/bin/linux/amd64/kubectl && chmod +x kubectl
    sudo cp kubectl /usr/bin
}

function install_nsenter {
    # Pre-req for helm
    curl https://mirrors.edge.kernel.org/pub/linux/utils/util-linux/v${TEST_NSENTER_VERSION}/util-linux-${TEST_NSENTER_VERSION}.tar.gz -k | tar -zxf-
    cd util-linux-${TEST_NSENTER_VERSION}
    ./configure --without-ncurses
    make nsenter
    sudo cp nsenter /usr/bin
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
        TEST_MINIKUBE_URL=https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64
    else
        TEST_MINIKUBE_URL=https://github.com/kubernetes/minikube/releases/download/${TEST_MINIKUBE_VERSION}/minikube-linux-amd64
    fi
    curl -Lo minikube ${TEST_MINIKUBE_URL} && chmod +x minikube
    sudo cp minikube /usr/bin

    export MINIKUBE_WANTUPDATENOTIFICATION=false
    export MINIKUBE_WANTREPORTERRORPROMPT=false
    export MINIKUBE_HOME=$HOME
    export CHANGE_MINIKUBE_NONE_USER=true
    
    mkdir $HOME/.kube || true
    touch $HOME/.kube/config

    docker run -d -p 5000:5000 registry

    export KUBECONFIG=$HOME/.kube/config
    # We can turn on network polices support by adding the following options --network-plugin=cni --cni=calico
    # We have to allow trafic for ITS when NPs are turned on
    # We can allow NP after Strimzi#4092 which should fix some issues on STs side
    sudo -E minikube start --vm-driver=none --kubernetes-version=v${KUBE_VERSION} \
      --insecure-registry=localhost:5000 --extra-config=apiserver.authorization-mode=Node,RBAC

    if [ $? -ne 0 ]
    then
        echo "Minikube failed to start or RBAC could not be properly set up"
        exit 1
    fi

    sudo -E minikube addons enable default-storageclass
	kubectl create clusterrolebinding add-on-cluster-admin --clusterrole=cluster-admin --serviceaccount=kube-system:default

elif [ "$TEST_CLUSTER" = "minishift" ]; then
    #install_kubectl
    MS_VERSION=1.13.1
    curl -Lo minishift.tgz https://github.com/minishift/minishift/releases/download/v$MS_VERSION/minishift-$MS_VERSION-linux-amd64.tgz && tar -xvf minishift.tgz --strip-components=1 minishift-$MS_VERSION-linux-amd64/minishift && rm minishift.tgz && chmod +x minishift
    sudo cp minishift /usr/bin

    #export MINIKUBE_WANTUPDATENOTIFICATION=false
    #export MINIKUBE_WANTREPORTERRORPROMPT=false
    export MINISHIFT_HOME=$HOME
    #export CHANGE_MINIKUBE_NONE_USER=true
    mkdir $HOME/.kube || true
    touch $HOME/.kube/config

    docker run -d -p 5000:5000 registry

    export KUBECONFIG=$HOME/.kube/config
    sudo -E minishift start
    sudo -E minishift addons enable default-storageclass
elif [ "$TEST_CLUSTER" = "oc" ]; then
    mkdir -p /tmp/openshift
    wget https://github.com/openshift/origin/releases/download/v3.7.0/openshift-origin-client-tools-v3.7.0-7ed6862-linux-64bit.tar.gz -O openshift.tar.gz
    tar xzf openshift.tar.gz -C /tmp/openshift --strip-components 1
    sudo cp /tmp/openshift/oc /usr/bin
else
    echo "Unsupported TEST_CLUSTER '$TEST_CLUSTER'"
    exit 1
fi

label_node
