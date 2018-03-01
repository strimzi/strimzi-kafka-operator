#!/bin/bash

function install_kubectl {
    curl -Lo kubectl https://storage.googleapis.com/kubernetes-release/release/$(curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt)/bin/linux/amd64/kubectl && chmod +x kubectl
    sudo cp kubectl /usr/bin
}

if [ "$TEST_CLUSTER" = "minikube" ]; then
    install_kubectl
    curl -Lo minikube https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64 && chmod +x minikube
    sudo cp minikube /usr/bin

    export MINIKUBE_WANTUPDATENOTIFICATION=false
    export MINIKUBE_WANTREPORTERRORPROMPT=false
    export MINIKUBE_HOME=$HOME
    export CHANGE_MINIKUBE_NONE_USER=true
    mkdir $HOME/.kube || true
    touch $HOME/.kube/config

    docker run -d -p 5000:5000 registry

    export KUBECONFIG=$HOME/.kube/config
    sudo -E minikube start --vm-driver=none --insecure-registry localhost:5000
    sudo -E minikube addons enable default-storageclass
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
    sudo -E minishift start --vm-driver=none --insecure-registry localhost:5000
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